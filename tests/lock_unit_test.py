"""Deterministic DB-level regression: write lock, failed writes, cursor/watermark.

Runs without any HTTP services. It imports collector.db and collector.ingest
directly, points them at a temporary SQLite file, and uses a real second
connection holding BEGIN IMMEDIATE to simulate the "other connection holding
the write lock longer than busy_timeout".

    PYTHONPATH=<libs> python3 tests/lock_unit_test.py
"""

from __future__ import annotations

import argparse
import asyncio
import os
import sqlite3
import tempfile
import threading
import time

from collector.db import Database, DatabaseBusy
from collector.ingest import META_HISTORY_CURSOR, IngestEngine


class HoldLock:
    """在独立线程的连接上执行 BEGIN IMMEDIATE，保持写锁直到 stop()。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self.ready = threading.Event()
        self.done = threading.Event()
        self.thread: threading.Thread | None = None

    def start(self) -> None:
        # sqlite3 连接只能在创建它的线程中使用，因此整段持锁逻辑都放在
        # 同一个工作线程里；ready 表示 BEGIN IMMEDIATE 已经成功。
        def hold() -> None:
            conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self.ready.set()
                self.done.wait(timeout=60)
                conn.rollback()
            finally:
                conn.close()

        self.thread = threading.Thread(target=hold, daemon=True)
        self.thread.start()
        assert self.ready.wait(timeout=10), "无法取得写锁"

    def stop(self) -> None:
        self.done.set()
        if self.thread is not None:
            self.thread.join(timeout=10)


def _pos(seq: int) -> dict:
    return {
        "seq": seq,
        "mmsi": "413000001",
        "utc_ts": time.time(),
        "latitude": 31.2,
        "longitude": 121.4,
    }


class _FakeResponse:
    def __init__(self, records: list[dict], has_more: bool = False) -> None:
        self._records = records
        self._has_more = has_more

    def raise_for_status(self) -> None:
        return None

    def json(self) -> dict:
        return {"records": self._records, "has_more": self._has_more}


class _FakeClient:
    """最小化 httpx.AsyncClient 替身，供 _history_step 使用。"""

    def __init__(self, records: list[dict], has_more: bool = False) -> None:
        self.records = records
        self.has_more = has_more

    async def get(self, url: str, params: dict | None = None) -> _FakeResponse:
        cursor = int((params or {}).get("cursor", 0))
        filtered = [r for r in self.records if r["seq"] > cursor]
        return _FakeResponse(filtered, self.has_more and bool(filtered))


async def main() -> int:
    failures = 0

    def check(name: str, cond: bool, detail: str = "") -> None:
        nonlocal failures
        if not cond:
            failures += 1
        print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f" -- {detail}" if detail else ""))

    tmpdir = tempfile.mkdtemp(prefix="lock-unit-")
    path = os.path.join(tmpdir, "collector.db")
    db_module = __import__("collector.db", fromlist=["x"])
    old = (
        db_module.SQLITE_BUSY_TIMEOUT_MS,
        db_module.DB_WRITE_RETRY_ATTEMPTS,
        db_module.DB_WRITE_RETRY_DELAY,
    )
    # 短窗口让测试快速且确定（与生产默认值语义一致）
    db_module.SQLITE_BUSY_TIMEOUT_MS = 300
    db_module.DB_WRITE_RETRY_ATTEMPTS = 2
    db_module.DB_WRITE_RETRY_DELAY = 0.1
    try:
        db = Database(path)
        await db.connect()
        await db.reset_all()
        engine = IngestEngine(db)

        await db.upsert_position(_pos(1), "sse")
        await db.advance_watermark()
        check("初始水位推进", await db.get_watermark() == 1)

        # ---------- 持锁写入：有界失败且无副作用 ----------
        lock = HoldLock(path)
        lock.start()
        try:
            t0 = time.time()
            busy: BaseException | None = None
            try:
                await db.upsert_position(_pos(2), "sse")
            except DatabaseBusy as exc:
                busy = exc
            elapsed = time.time() - t0
            check("持锁写入抛出 DatabaseBusy 而非卡死", busy is not None, str(busy))
            check("写重试窗口有界（未无限等待）", elapsed < 15, f"{elapsed:.1f}s")
            check("失败写入不推进水位", await db.get_watermark() == 1,
                  f"w={await db.get_watermark()}")

            # 读连接在持锁期间仍能读取已提交数据
            check("持锁期间读连接可读已提交数据",
                  await db.count_positions() == 1,
                  f"total={await db.count_positions()}")

            # 错误登记不能阻塞：短超时失败后转存内存
            engine.max_seen_seq = 10
            err_id = await engine._record_error("db_write", "unit lock error")
            check("持锁期间错误登记不阻塞且转存内存",
                  err_id is None and len(engine._pending_errors) == 1,
                  f"id={err_id} pending={len(engine._pending_errors)}")

            # 历史页在持锁期间：游标不前进
            try:
                await engine._history_step(_FakeClient([_pos(2), _pos(3)]))
            except DatabaseBusy:
                pass
            check("锁期间历史游标不前进",
                  int(await db.get_meta(META_HISTORY_CURSOR, "0")) == 0,
                  f"cursor={await db.get_meta(META_HISTORY_CURSOR, '0')}")
        finally:
            lock.stop()

        # ---------- 锁释放后：重试成功、只入库一次 ----------
        await engine._history_step(_FakeClient([_pos(2), _pos(3)]))
        check("锁释放后写入成功", await db.seq_exists(2) and await db.seq_exists(3))
        check("锁释放后水位推进到 3", await db.get_watermark() == 3,
              f"w={await db.get_watermark()}")
        check("锁释放后游标推进",
              int(await db.get_meta(META_HISTORY_CURSOR, "0")) == 3)
        check("失败页重放不产生重复", await db.count_positions() == 3,
              f"total={await db.count_positions()}")
        check("暂存错误已补记并标记恢复",
              len(engine._pending_errors) == 0
              and any(e["kind"] == "db_write"
                      for e in await db.list_errors(10, True)))
        # 再重放同一页：主键去重，总数/水位不变（两路重叠只入库一次）
        await engine._history_step(_FakeClient([_pos(2), _pos(3)]))
        check("两路重叠只入库一次",
              await db.count_positions() == 3 and await db.get_watermark() == 3,
              f"total={await db.count_positions()} w={await db.get_watermark()}")

        # ---------- 缺口自愈：自动回退游标，无需手工调用 ----------
        await db.upsert_position(_pos(5), "sse")
        await db.set_meta(META_HISTORY_CURSOR, "5")
        engine.max_seen_seq = 5
        repaired = await engine._repair_cursor_if_gap(force=True)
        check("检测到水位后缺口并自动回退游标", repaired
              and int(await db.get_meta(META_HISTORY_CURSOR, "0"))
              == await db.get_watermark(),
              f"cursor={await db.get_meta(META_HISTORY_CURSOR, '0')} "
              f"w={await db.get_watermark()}")
        check("缺口后的船位暂不可见（水位未越过 4）", await db.get_watermark() == 3)
        await engine._history_step(_FakeClient([_pos(4)]))
        check("重放补齐后水位推进到 5", await db.get_watermark() == 5,
              f"w={await db.get_watermark()}")

        await db.close()
    finally:
        (
            db_module.SQLITE_BUSY_TIMEOUT_MS,
            db_module.DB_WRITE_RETRY_ATTEMPTS,
            db_module.DB_WRITE_RETRY_DELAY,
        ) = old

    print(f"\n失败用例: {failures}")
    return 1 if failures else 0


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.parse_args()
    raise SystemExit(asyncio.run(main()))
