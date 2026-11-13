"""写锁/网络恢复/双通道交错回归测试（只通过 HTTP 驱动两服务）。

复现并锁定本次故障：
在岸站历史接口恢复后，另一连接对归档库持有写锁超过 busy_timeout，
采集循环不能退出；锁释放后必须自行补齐历史缺口，无需重启服务或调用
回退接口。同时覆盖写入失败不推进游标/水位、重叠只入库一次、缺口后的
船位不可查询、持锁期间进程退出后重启按已提交进度接续。

用法（与 e2e_test.py 相同，环境需已启动 station:8000 / collector:8001）：
    python3 tests/lock_regression_test.py
"""

from __future__ import annotations

import argparse
import os
import sqlite3
import subprocess
import sys
import threading
import time

import httpx

FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global FAIL
    if not cond:
        FAIL += 1
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))


def wait_until(desc: str, fn, timeout: float = 90.0, interval: float = 0.5):
    """轮询直到 fn 返回真值；只返回真值，错误仅用于超时诊断。"""
    deadline = time.time() + timeout
    last: object = None
    while time.time() < deadline:
        try:
            last = fn()
        except httpx.HTTPError:
            # 服务短暂不可达（例如重启中）不算成功，继续等待
            last = None
            time.sleep(interval)
            continue
        if isinstance(last, bool):
            if last:
                return last
            last = None
        elif last:
            return last
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc} (last={last})")


class WriteLock:
    """在独立连接上持有 SQLite 写锁，模拟“另一连接持写锁”。

    BEGIN IMMEDIATE 立即取得写锁并保持，直到 release()。它不提交任何
    数据，因此只阻塞写者，不阻塞 WAL 读者。sqlite3 连接只能在创建它的
    线程中使用，所以整段持锁逻辑都运行在同一个工作线程里。
    """

    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        self._thread: threading.Thread | None = None
        self._held = threading.Event()
        self._release = threading.Event()

    def acquire(self) -> None:
        def hold() -> None:
            conn = sqlite3.connect(self.db_path, timeout=30, isolation_level=None)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._held.set()
                self._release.wait(timeout=300)
                conn.rollback()
            finally:
                conn.close()

        self._thread = threading.Thread(target=hold, daemon=True)
        self._thread.start()
        assert self._held.wait(timeout=15), "无法取得写锁"

    def release(self) -> None:
        self._release.set()
        if self._thread is not None:
            self._thread.join(timeout=15)

    def __enter__(self) -> "WriteLock":
        self.acquire()
        return self

    def __exit__(self, *args) -> None:
        self.release()


def open_reader(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=10, isolation_level=None)
    conn.row_factory = sqlite3.Row
    return conn


def fetch_status(col: httpx.Client) -> dict:
    return col.get("/status", timeout=5).json()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--station", default="http://127.0.0.1:8000")
    ap.add_argument("--collector", default="http://127.0.0.1:8001")
    ap.add_argument("--db", default=os.getenv("DB_PATH", "/tmp/colldata/collector.db"))
    ap.add_argument(
        "--restart-command",
        default="",
        help="可选：重启采集器的 shell 命令（如 docker compose restart collector）",
    )
    args = ap.parse_args()
    st = httpx.Client(base_url=args.station, timeout=15)
    col = httpx.Client(base_url=args.collector, timeout=15)

    # ---------- 0. 重置并等初始采集稳定 ----------
    st.post("/station/reset")
    col.post("/admin/reset")

    def caught_up():
        s = fetch_status(col)
        return s["complete"] and s["received_total"] >= 60 and s["open_error_count"] == 0

    wait_until("初始采集稳定", caught_up, timeout=60)
    deadline = time.time() + 10
    while st.get("/station/status").json()["history_fail_rate"] != 0:
        if time.time() > deadline:
            break
        time.sleep(0.2)
    time.sleep(1.5)
    before = fetch_status(col)
    check("初始状态无缺口", before["contiguous_seq"] == before["received_total"],
          f"w={before['contiguous_seq']} total={before['received_total']}")

    # ---------- 1. 历史接口恢复后，另一连接持写锁超过超时 ----------
    # 先让历史翻页全部失败一段时间，期间 SSE 继续入库；再恢复历史接口，
    # 恰好处于“历史恢复、补采开始”的时点持锁，这正是故障现象的场景。
    st.post("/station/history-fail-rate", json={"rate": 1.0})
    time.sleep(5)
    st.post("/station/history-fail-rate", json={"rate": 0.0})

    def history_recovered():
        s = fetch_status(col)
        return s if s["open_error_count"] == 0 else None

    wait_until("历史接口恢复并消除错误", history_recovered, timeout=30)
    station_pre = st.get("/station/status").json()["last_seq"]

    busy_timeout = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
    attempts = int(os.getenv("DB_WRITE_RETRY_ATTEMPTS", "2"))
    delay = float(os.getenv("DB_WRITE_RETRY_DELAY", "0.5"))
    # 持锁必须超过 busy_timeout；再留出余量，确保超过整个重试预算
    lock_seconds = max(6.0, (busy_timeout * attempts) / 1000 + delay + 3.0)
    status_samples: list[dict] = []
    errors_samples: list[list] = []
    errors_seen: list[BaseException] = []

    def sample_while_locked() -> None:
        # 持锁期间持续查询：读连接必须可用（WAL 快照），不能返回 5xx/超时
        while not stop_sampling.is_set():
            try:
                status_samples.append(fetch_status(col))
                errors_samples.append(
                    col.get("/errors?include_resolved=true", timeout=5).json()
                )
            except BaseException as exc:  # noqa: BLE001
                errors_seen.append(exc)
            time.sleep(0.5)

    stop_sampling = threading.Event()
    sampler = threading.Thread(target=sample_while_locked, daemon=True)
    lock = WriteLock(args.db)
    try:
        lock.acquire()
        sampler.start()
        time.sleep(lock_seconds)
    finally:
        stop_sampling.set()
        sampler.join(timeout=10)
        lock.release()

    check("持锁期间读接口持续可用（无超时/错误）", not errors_seen,
          f"errors={errors_seen[:2]}")
    check("持锁期间 /status 读到已提交数据",
          len(status_samples) > 0 and all(
              s["received_total"] > 0 for s in status_samples
          ), f"samples={len(status_samples)}")
    if status_samples:
        check("持锁期间已提交总数不回退",
              all(
                  status_samples[i + 1]["received_total"] >= status_samples[i]["received_total"]
                  for i in range(len(status_samples) - 1)
              ))
    # 持锁时长确实超过了单次 busy_timeout（故障注入的前提条件）
    check("持锁时长超过 SQLITE busy_timeout",
          lock_seconds > busy_timeout / 1000,
          f"held={lock_seconds:.1f}s busy_timeout={busy_timeout}ms")

    # ---------- 2. 锁释放后自动补齐，无需重启/回退接口 ----------
    # 锁释放后必须自行补齐：水位追上已见最大序号、无未恢复错误、两路仍连续
    def healed():
        s = fetch_status(col)
        ok = (
            s["complete"]
            and s["open_error_count"] == 0
            and s["contiguous_seq"] >= s["max_seen_seq"]
            and s["received_total"] == s["contiguous_seq"]
        )
        return s if ok else None

    final = wait_until("锁释放后自行补齐缺口", healed, timeout=120)
    check("锁释放后水位追上真实最新", final["contiguous_seq"] >= station_pre,
          f"w={final['contiguous_seq']} station_pre={station_pre}")
    check("锁释放后无需重启仍连续", final["contiguous_seq"] == final["received_total"])
    check("锁期间写锁失败被计数", final["db_write_failures"] >= 1,
          f"db_write_failures={final['db_write_failures']}")
    check("锁期间锁冲突被观测", final["db_write_lock_waits"] >= 1,
          f"db_write_lock_waits={final['db_write_lock_waits']}")
    # 明确验证：整个过程中未调用 /admin/rewind-backfill（本脚本从未调用）
    check("未调用回退接口即完成补齐", True)

    # 直接核对库内无重复 seq、轨迹不越过缺口
    db = open_reader(args.db)
    try:
        dup = db.execute(
            "SELECT seq, COUNT(*) c FROM positions GROUP BY seq HAVING c > 1 LIMIT 3"
        ).fetchall()
        check("两路重叠仍只入库一次", len(dup) == 0, str(dup))
        total, distinct = db.execute(
            "SELECT COUNT(*), COUNT(DISTINCT seq) FROM positions"
        ).fetchone()
        check("库内 seq 唯一", total == distinct and total > 0,
              f"total={total} distinct={distinct}")
        w = int(db.execute(
            "SELECT value FROM meta WHERE key='contiguous_seq'"
        ).fetchone()["value"])
        rows_after = db.execute(
            "SELECT COUNT(*) c FROM positions WHERE seq > ?", (w,)
        ).fetchone()["c"]
        check("已提交水位之前无空洞（水位即连续上界）",
              rows_after == 0 or w == final["contiguous_seq"],
              f"w={w} rows_after_watermark={rows_after}")
    finally:
        db.close()

    # ---------- 3. 两路采集交错：SSE 与历史同时写，仍只入库一次 ----------
    st.post("/station/history-fail-rate", json={"rate": 0.0})
    inter_before = fetch_status(col)
    time.sleep(6)
    inter_after = fetch_status(col)
    check("两路采集交错后仍连续",
          inter_after["contiguous_seq"] == inter_after["received_total"])
    check("两路都在贡献", inter_after["sse_received"] > 0
          and inter_after["history_received"] > 0,
          f"sse={inter_after['sse_received']} history={inter_after['history_received']}")
    check("交错期间水位单调增长",
          inter_after["contiguous_seq"] > inter_before["contiguous_seq"])

    db = open_reader(args.db)
    try:
        dup = db.execute(
            "SELECT seq, COUNT(*) c FROM positions GROUP BY seq HAVING c > 1 LIMIT 3"
        ).fetchall()
        check("交错写入后仍无重复", len(dup) == 0, str(dup))
    finally:
        db.close()

    # ---------- 4. 缺口注入：轨迹截断，锁释放后自动补齐（不需要回退接口） ----------
    cur_status = fetch_status(col)
    w_before = cur_status["contiguous_seq"]
    gap = w_before - 5
    vessels = col.get("/vessels").json()
    mmsi = vessels[0]["mmsi"]
    traj = col.get(f"/trajectories/{mmsi}").json()
    max_visible_before = max(p["seq"] for p in traj["positions"])
    r = col.post("/admin/inject-gap", json={"seqs": [gap]})
    check("缺口注入成功", r.status_code == 200, r.text)
    s_gap = fetch_status(col)
    check("水位回退到缺口前", s_gap["contiguous_seq"] <= gap - 1,
          f"w={s_gap['contiguous_seq']}")
    traj_gap = col.get(f"/trajectories/{mmsi}").json()
    check("缺口后的船位不可查询",
          all(p["seq"] <= gap - 1 for p in traj_gap["positions"]),
          f"max={max(p['seq'] for p in traj_gap['positions'])}")

    def gap_healed():
        s = fetch_status(col)
        return s if s["complete"] and s["contiguous_seq"] >= w_before else None

    s_healed = wait_until("缺口自动补齐（未调用回退接口）", gap_healed, timeout=60)
    traj_after = col.get(f"/trajectories/{mmsi}").json()
    check("补齐后轨迹恢复", max(p["seq"] for p in traj_after["positions"])
          >= max_visible_before)
    check("补齐后无未恢复错误", s_healed["open_error_count"] == 0)
    check("缺口由采集器自愈补齐（gap_repairs 增加）",
          s_healed["gap_repairs"] >= 1, f"gap_repairs={s_healed['gap_repairs']}")

    # ---------- 5. 持锁期间进程退出，重启按已提交进度接续 ----------
    committed = fetch_status(col)
    station_at_exit = st.get("/station/status").json()["last_seq"]
    lock2 = WriteLock(args.db)
    lock2.acquire()
    try:
        if args.restart_command:
            subprocess.run(args.restart_command, shell=True, check=False)
        else:
            # 默认按本地进程名终止；Docker 环境请传入 --restart-command
            subprocess.run(["pkill", "-f", "[c]ollector.app:app"], check=False)
        # 等端口不可达（进程确实退出）
        deadline = time.time() + 20
        while time.time() < deadline:
            try:
                col.get("/health", timeout=1)
            except httpx.HTTPError:
                break
            time.sleep(0.5)
        # 锁仍然持有，重启后必须先从已提交数据恢复
    finally:
        lock2.release()

    if not args.restart_command:
        # 本地模式：测试脚本负责重新拉起采集器（沿用同一 DB 文件）
        proc = subprocess.Popen(
            [
                sys.executable, "-m", "uvicorn", "collector.app:app",
                "--host", "127.0.0.1", "--port", "8001",
            ],
            env={**os.environ, "STATION_URL": args.station, "DB_PATH": args.db},
            stdout=open("/tmp/collector-lock-restart.log", "w"),
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
        )
        check("采集器重新拉起", proc.pid > 0)
    else:
        check("通过外部命令重启采集器", True)

    wait_until(
        "重启后服务恢复",
        lambda: col.get("/health", timeout=2).status_code == 200,
        timeout=60,
    )
    after = wait_until(
        "重启后接续",
        lambda: (
            lambda s: (
                s
                if s["complete"]
                and s["received_total"] >= committed["received_total"]
                and s["contiguous_seq"] >= station_at_exit
                else None
            )
        )(fetch_status(col)),
        timeout=120,
    )
    check("重启后已提交船位保留",
          after["received_total"] >= committed["received_total"],
          f"{committed['received_total']} -> {after['received_total']}")
    check("重启后按已提交水位接续", after["contiguous_seq"] >= committed["contiguous_seq"])
    check("重启后最终补齐停机/持锁期间的数据", after["contiguous_seq"] >= station_at_exit)
    check("重启后仍无重复",
          after["contiguous_seq"] == after["received_total"])

    print("\n========== 汇总 ==========")
    print(f"失败用例: {FAIL}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
