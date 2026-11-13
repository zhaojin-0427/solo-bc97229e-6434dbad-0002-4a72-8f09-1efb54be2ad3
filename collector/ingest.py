"""采集引擎：并发运行 SSE 实时接收与历史分页补采两个循环。

容错原则（对应需求）：
1. 启动即同时开 SSE 与历史补采；两者重叠记录靠 seq 主键去重，只入库一次。
2. SSE 不补发旧记录，断线重连期间错过的序号由历史补采循环补齐。
3. 历史翻页失败只记错误并重试，绝不影响实时接收。
4. 每次写入后尝试推进连续水位；只有 1..w 全部入库，水位才等于 w。
5. 所有进度（船位、水位、历史翻页游标、错误）都在 SQLite 中，重启自动接续。
6. 写锁冲突（另一连接持锁超过 busy_timeout）绝不让任一循环退出：
   - 位置写入失败：该序号不会推进连续水位，本页历史游标整体不前进，
     退避后从同一游标重放（已提交记录由主键去重，只入库一次）；
   - 错误登记失败：先暂存内存，写锁释放后补记并标记已恢复；
   - SSE 写失败：不断流、不退出循环，缺口交给历史补采自愈；
   - 两个循环都对未预期异常兜底，只有显式 stop 才会结束。
7. 自动补齐：只要连续水位落后于已见最大序号且历史游标已越过水位，
   历史循环就自动把游标回退到水位重新分页。无需重启服务，也无需手工
   调用回退接口；回退后的重放由主键去重，不会重复入库。
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import sqlite3
import time
from typing import Any

import httpx

from .db import (
    ERROR_KIND_DB,
    ERROR_KIND_HISTORY,
    ERROR_KIND_PARSE,
    ERROR_KIND_SSE,
    Database,
    DatabaseBusy,
)

STATION_URL = os.getenv("STATION_URL", "http://station:8000")
HISTORY_PAGE_SIZE = int(os.getenv("HISTORY_PAGE_SIZE", "50"))
HISTORY_RETRY_BASE = float(os.getenv("HISTORY_RETRY_BASE", "1.0"))
HISTORY_RETRY_MAX = float(os.getenv("HISTORY_RETRY_MAX", "30"))
SSE_RECONNECT_BASE = float(os.getenv("SSE_RECONNECT_BASE", "0.5"))
SSE_RECONNECT_MAX = float(os.getenv("SSE_RECONNECT_MAX", "30"))
SSE_READ_TIMEOUT = float(os.getenv("SSE_READ_TIMEOUT", "60"))
STATION_CATCHUP_IDLE = float(os.getenv("STATION_CATCHUP_IDLE", "0.5"))
# 数据库层锁重试耗尽后的采集侧退避
LOCK_RETRY_BASE = float(os.getenv("LOCK_RETRY_BASE", "0.5"))
LOCK_RETRY_MAX = float(os.getenv("LOCK_RETRY_MAX", "10"))
# 自动回退补采的最小/最大间隔：缺口反复出现（例如岸站本身缺报）时
# 指数放慢重放节奏，水位一旦前进立即复位，避免高频重放和告警刷屏。
GAP_REPAIR_MIN_INTERVAL = float(os.getenv("GAP_REPAIR_MIN_INTERVAL", "1.0"))
GAP_REPAIR_MAX_INTERVAL = float(os.getenv("GAP_REPAIR_MAX_INTERVAL", "30"))
# 写锁期间错误登记的内存缓冲上限，避免长时间持锁时无限增长
MAX_PENDING_ERRORS = int(os.getenv("MAX_PENDING_ERRORS", "20"))

META_HISTORY_CURSOR = "history_cursor"


def _validate_position(raw: dict[str, Any]) -> dict[str, Any]:
    required = {"seq", "mmsi", "utc_ts", "latitude", "longitude"}
    missing = required - raw.keys()
    if missing:
        raise ValueError(f"船位缺少字段: {sorted(missing)}")
    seq = raw["seq"]
    if not isinstance(seq, int) or seq < 1:
        raise ValueError(f"非法 seq: {seq!r}")
    lat = float(raw["latitude"])
    lon = float(raw["longitude"])
    if not -90 <= lat <= 90 or not -180 <= lon <= 180:
        raise ValueError(f"经纬度越界: lat={lat}, lon={lon}")
    return {
        "seq": seq,
        "mmsi": str(raw["mmsi"]),
        "utc_ts": float(raw["utc_ts"]),
        "latitude": lat,
        "longitude": lon,
    }


class IngestEngine:
    def __init__(self, db: Database) -> None:
        self.db = db
        self._tasks: list[asyncio.Task] = []
        self._stop = asyncio.Event()
        # 仅用于状态展示的内存计数（权威计数以数据库为准）
        self.sse_connected = False
        self.sse_reconnects = 0
        self.sse_disconnects = 0
        self.sse_received = 0
        self.history_received = 0
        self.history_requests = 0
        self.history_failures = 0
        self.db_write_failures = 0
        self.gap_repairs = 0
        self.last_event_at: float | None = None
        self.last_history_at: float | None = None
        self.last_backfill_seq: int | None = None
        # 任一来源见过的最大序号，用于界定真实缺口的上界
        self.max_seen_seq = 0
        # 无人值守场景下历史游标可能与已在库的数据不一致；启动时探测
        # 历史游标之前是否存在缺口，若存在则回退游标自愈。
        self._needs_startup_repair = False
        self._last_repair_at = 0.0
        self._repair_backoff = GAP_REPAIR_MIN_INTERVAL
        self._last_watermark = 0
        self._backfill_open_error: int | None = None
        self._sse_open_error: int | None = None
        self._sse_db_error: int | None = None
        # 写锁导致错误登记失败时暂存于此，恢复后补记入库
        self._pending_errors: list[tuple[str, str]] = []
        self.pending_error_drops = 0

    # ----------------------------- 生命周期 -----------------------------

    async def start(self) -> None:
        # 启动时也可能遇到另一连接持写锁：读走读连接（WAL 快照），
        # 写失败按 DatabaseBusy 处理而不是让应用启动失败。
        await self.db.ensure_schema()
        try:
            # 重启恢复：以库内最大序号作为缺口上界的初值。
            cur = await self.db.reader.execute(
                "SELECT COALESCE(MAX(seq), 0) m FROM positions"
            )
            row = await cur.fetchone()
            self.max_seen_seq = row["m"]
            await self.db.advance_watermark()
            # 若历史游标已经越过水位，说明存在未提交成功的空洞；让历史循环
            # 在第一轮先回退游标。重启后无需人工干预即可按已提交进度接续。
            self._needs_startup_repair = (
                self.max_seen_seq > await self.db.get_watermark()
            )
        except (DatabaseBusy, sqlite3.Error):
            # 写锁尚未释放或表暂不可用：历史循环会在首轮自愈（force 修复）
            # 时补做，启动绝不能被写锁拖死。
            self._needs_startup_repair = True
        self._stop.clear()
        self._tasks = [
            asyncio.create_task(self._sse_loop(), name="sse-loop"),
            asyncio.create_task(self._history_loop(), name="history-loop"),
        ]

    async def stop(self) -> None:
        self._stop.set()
        for t in self._tasks:
            t.cancel()
        for t in self._tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass

    # ----------------------------- 错误登记（遇锁不退出） -----------------------------

    async def _record_error(self, kind: str, message: str) -> int | None:
        """登记一条未恢复错误；写锁未释放时暂存内存，稍后补记。"""
        try:
            return await self.db.add_error(kind, message)
        except asyncio.CancelledError:
            raise
        except Exception:
            # 登记失败（写锁/建表未完成）不影响采集：暂存，稍后补记
            if len(self._pending_errors) < MAX_PENDING_ERRORS:
                self._pending_errors.append((kind, message))
            else:
                self.pending_error_drops += 1
            return None

    async def _resolve_error(self, error_id: int | None) -> None:
        if error_id is None:
            return
        try:
            await self.db.resolve_error(error_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            # 恢复状态晚一点落库没关系；循环不能因此退出
            pass

    async def _flush_pending_errors(self) -> None:
        if not self._pending_errors:
            return
        pending, self._pending_errors = self._pending_errors, []
        for kind, message in pending:
            try:
                eid = await self.db.add_error(kind, message)
                await self.db.resolve_error(eid)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._pending_errors.append((kind, message))
                return

    # ----------------------------- 公共写入路径 -----------------------------

    async def _update_watermark(self) -> int:
        """推进连续水位；写锁冲突时保留旧水位，绝不让调用方崩溃。

        水位只是“可见性上界”，稍后任一成功写入会再次尝试推进；
        这里捕获写失败是为了保证位置已提交但水位更新遇锁时，
        采集循环仍继续运转而不是退出。
        """
        try:
            self._last_watermark = await self.db.advance_watermark()
        except asyncio.CancelledError:
            raise
        except Exception:
            # 水位落后于已提交数据：自愈逻辑会在下轮重新推进/补齐
            pass
        return self._last_watermark

    async def _ingest(self, raw: dict[str, Any], source: str) -> bool:
        """写入一条船位；DatabaseBusy 向上抛出，由循环退避重试。

        失败写入没有任何副作用：不推进连续水位（缺口挡住），也不推进
        历史游标（由调用方在本页全部提交后才持久化）。
        """
        try:
            pos = _validate_position(raw)
        except (ValueError, TypeError, KeyError) as exc:
            await self._record_error(
                ERROR_KIND_PARSE, f"{source} 记录解析失败: {exc}"
            )
            return False
        # 即使写入失败也要记住已见序号：历史循环据此发现水位之后的
        # 真实缺口/空洞并自动回补。
        if pos["seq"] > self.max_seen_seq:
            self.max_seen_seq = pos["seq"]
        inserted = await self.db.upsert_position(pos, source)
        # SSE 侧代际检测：已入库的最大序号远大于当前序号（回退超过一个补采页），
        # 且当前序号很小，判定岸站重置；回退历史游标重拉新一代际（去重仍安全）
        if (
            source == "sse"
            and not inserted
            and self.max_seen_seq - pos["seq"] > HISTORY_PAGE_SIZE
        ):
            cursor_now = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
            if cursor_now > pos["seq"]:
                await self.db.set_meta(META_HISTORY_CURSOR, "0")
                eid = await self._record_error(
                    ERROR_KIND_SSE,
                    f"SSE 收到回退序号 seq={pos['seq']}（已见最大 "
                    f"{self.max_seen_seq}，历史游标 {cursor_now}），"
                    "判定岸站重置，已回退补采游标",
                )
                await self._resolve_error(eid)
        if inserted:
            if source == "sse":
                self.sse_received += 1
            else:
                self.history_received += 1
            self.last_event_at = time.time()
        # 无论新老记录都尝试推进水位（新记录可能恰好补上缺口）
        await self._update_watermark()
        return inserted

    async def _repair_cursor_if_gap(self, force: bool = False) -> bool:
        """连续水位落后于已见最大序号且历史游标已越过水位时回退游标。

        这是“锁释放后自行补齐历史缺口”的实现：无需重启服务，也无需
        调用 /admin/rewind-backfill。回退后的重放由 seq 主键去重。
        """
        watermark = await self._next_watermark()
        if self.max_seen_seq <= watermark:
            return False
        cursor = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
        if cursor <= watermark:
            return False
        now = time.monotonic()
        if not force and now - self._last_repair_at < self._repair_backoff:
            return False
        await self.db.set_meta(META_HISTORY_CURSOR, str(watermark))
        self._last_repair_at = now
        self.gap_repairs += 1
        self._repair_backoff = min(
            self._repair_backoff * 2, GAP_REPAIR_MAX_INTERVAL
        )
        eid = await self._record_error(
            ERROR_KIND_HISTORY,
            f"连续水位 {watermark} 落后于已见最大序号 {self.max_seen_seq}，"
            f"历史游标自 {cursor} 自动回退到 {watermark} 重新补采缺口",
        )
        await self._resolve_error(eid)
        return True

    def _note_watermark(self, watermark: int) -> None:
        """水位前进说明缺口在收敛，重置自愈退避。"""
        if watermark > self._last_watermark:
            self._repair_backoff = GAP_REPAIR_MIN_INTERVAL
        self._last_watermark = watermark

    async def _next_watermark(self) -> int:
        """读取水位并刷新自愈退避状态。"""
        watermark = await self.db.get_watermark()
        self._note_watermark(watermark)
        return watermark

    @staticmethod
    async def _sleep_stop(stop: asyncio.Event, delay: float) -> None:
        """可被停止事件立即打断的等待。"""
        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
        except asyncio.TimeoutError:
            pass

    # ----------------------------- SSE 实时循环 -----------------------------

    async def _sse_loop(self) -> None:
        backoff = SSE_RECONNECT_BASE
        lock_backoff = LOCK_RETRY_BASE
        while not self._stop.is_set():
            self.sse_connected = False
            # 每次循环代表一次连接尝试（含首次连接），先计数再握手
            self.sse_reconnects += 1
            try:
                timeout = httpx.Timeout(connect=10, read=SSE_READ_TIMEOUT,
                                        write=10, pool=10)
                async with httpx.AsyncClient(timeout=timeout) as client:
                    async with client.stream(
                        "GET", f"{STATION_URL}/stream"
                    ) as resp:
                        if resp.status_code != 200:
                            body = await resp.aread()
                            raise httpx.HTTPStatusError(
                                f"SSE 握手失败 HTTP {resp.status_code}: "
                                f"{body[:200]!r}",
                                request=resp.request,
                                response=resp,
                            )
                        # 连接成功：退避与未恢复错误归零
                        backoff = SSE_RECONNECT_BASE
                        self.sse_connected = True
                        # 首次成功连接也算一次 reconnect（=1 表示已连上）
                        await self._flush_pending_errors()
                        if self._sse_open_error is not None:
                            await self._resolve_error(self._sse_open_error)
                            self._sse_open_error = None
                        async for raw_line in resp.aiter_lines():
                            if self._stop.is_set():
                                break
                            if not raw_line or not raw_line.startswith("data:"):
                                # 注释心跳、event: 行、空行均忽略
                                continue
                            payload = raw_line[len("data:"):].strip()
                            if payload == "stream interrupted":
                                raise ConnectionError("岸站主动中断实时流")
                            try:
                                obj = json.loads(payload)
                            except json.JSONDecodeError as exc:
                                await self._record_error(
                                    ERROR_KIND_PARSE,
                                    f"SSE 数据非合法 JSON: {exc}",
                                )
                                continue
                            try:
                                await self._ingest(obj, "sse")
                            except asyncio.CancelledError:
                                raise
                            except Exception as exc:
                                # 写锁超过重试窗口或写失败：不退出、不断流。
                                # 本序号未提交，历史补采会自动补齐（见
                                # _repair_cursor_if_gap）。
                                self.db_write_failures += 1
                                if self._sse_db_error is None:
                                    self._sse_db_error = await self._record_error(
                                        ERROR_KIND_DB,
                                        f"SSE 写入等待写锁失败: "
                                        f"{type(exc).__name__}: {exc}",
                                    )
                                delay = min(lock_backoff, LOCK_RETRY_MAX)
                                lock_backoff = min(
                                    lock_backoff * 2, LOCK_RETRY_MAX
                                )
                                await self._sleep_stop(self._stop, delay)
                            else:
                                lock_backoff = LOCK_RETRY_BASE
                                if self._sse_db_error is not None:
                                    await self._resolve_error(self._sse_db_error)
                                    self._sse_db_error = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                # 断流/超时/连接拒绝都走这里：记录一次未恢复错误，退避重连
                self.sse_connected = False
                self.sse_disconnects += 1
                if self._sse_open_error is None:
                    self._sse_open_error = await self._record_error(
                        ERROR_KIND_SSE, f"实时流中断: {type(exc).__name__}: {exc}"
                    )
            if self._stop.is_set():
                break
            # 指数退避 + 抖动，避免重连风暴
            delay = min(backoff, SSE_RECONNECT_MAX) * (
                0.5 + random.random() * 0.5
            )
            backoff = min(backoff * 2, SSE_RECONNECT_MAX)
            await self._sleep_stop(self._stop, delay)

    # ----------------------------- 历史补采循环 -----------------------------

    async def _history_loop(self) -> None:
        backoff = HISTORY_RETRY_BASE
        lock_backoff = LOCK_RETRY_BASE
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(15)
        ) as client:
            while not self._stop.is_set():
                try:
                    more = await self._history_step(client)
                except asyncio.CancelledError:
                    raise
                except DatabaseBusy as exc:
                    # 连游标都读不到/写不回：等锁释放后重试，循环不退出
                    self.db_write_failures += 1
                    self.history_failures += 1
                    if self._backfill_open_error is None:
                        self._backfill_open_error = await self._record_error(
                            ERROR_KIND_DB,
                            f"历史补采等待写锁失败: {type(exc).__name__}: {exc}",
                        )
                    delay = min(lock_backoff, LOCK_RETRY_MAX) * (
                        0.5 + random.random() * 0.5
                    )
                    lock_backoff = min(lock_backoff * 2, LOCK_RETRY_MAX)
                    await self._sleep_stop(self._stop, delay)
                    continue
                except Exception as exc:
                    # 兜底：任何未预期异常都只记录并退避，绝不让循环结束
                    self.history_failures += 1
                    if self._backfill_open_error is None:
                        self._backfill_open_error = await self._record_error(
                            ERROR_KIND_HISTORY,
                            f"历史翻页/补采失败: {type(exc).__name__}: {exc}",
                        )
                    delay = min(backoff, HISTORY_RETRY_MAX) * (
                        0.5 + random.random() * 0.5
                    )
                    backoff = min(backoff * 2, HISTORY_RETRY_MAX)
                    await self._sleep_stop(self._stop, delay)
                    continue
                lock_backoff = LOCK_RETRY_BASE
                backoff = HISTORY_RETRY_BASE
                if more:
                    # 远端还有更多页：立即继续，但仍让出事件循环
                    await asyncio.sleep(0)
                else:
                    await self._sleep_stop(self._stop, STATION_CATCHUP_IDLE)

    async def _history_step(self, client: httpx.AsyncClient) -> bool:
        """执行一轮“自愈 + 取一页 + 全页提交 + 推进游标”。

        返回 True 表示应尽快继续拉取（远端还有更多页或发生了游标回退）。
        任何写失败都会抛出 DatabaseBusy，调用方据此退避；此时游标保持
        在最近一次全部提交成功的位置，水位也不会因失败记录而前进。
        """
        force_repair = self._needs_startup_repair
        self._needs_startup_repair = False
        try:
            repaired = await self._repair_cursor_if_gap(force=force_repair)
        except DatabaseBusy:
            # 自愈需要写 meta：锁未释放时抛出，让循环退避后重试，
            # 不能在这里丢失自愈意图。
            self._needs_startup_repair = force_repair
            raise
        cursor = int(await self.db.get_meta(META_HISTORY_CURSOR, "0"))
        self.history_requests += 1
        resp = await client.get(
            f"{STATION_URL}/history",
            params={"cursor": cursor, "limit": HISTORY_PAGE_SIZE},
        )
        resp.raise_for_status()
        page = resp.json()
        records = page.get("records", [])

        # 岸站代际检测（空页路径）：游标很高却拿不到任何记录，
        # 探测第 1 页判断岸站是否已重置（新代际 seq 从 1 重新开始）
        if not records and cursor > 0:
            try:
                probe = await client.get(
                    f"{STATION_URL}/history",
                    params={"cursor": 0, "limit": 1},
                )
                probe.raise_for_status()
                probe_records = probe.json().get("records", [])
            except (httpx.HTTPError, json.JSONDecodeError, KeyError):
                # 探测失败不影响主流程：本轮按空页正常处理，下轮再探
                probe_records = []
            if probe_records and probe_records[0]["seq"] <= cursor:
                await self.db.set_meta(META_HISTORY_CURSOR, "0")
                # 这是可自愈的代际切换告警，直接标记已解决
                eid = await self._record_error(
                    ERROR_KIND_HISTORY,
                    f"检测到岸站序号重置(探测 seq="
                    f"{probe_records[0]['seq']} <= cursor={cursor})，"
                    "已自动回退补采游标重新补采",
                )
                await self._resolve_error(eid)
                return True

        # 岸站代际检测（非空页路径）：页首 seq 回退到游标之前
        if records and cursor > 0 and records[0]["seq"] <= cursor:
            await self.db.set_meta(META_HISTORY_CURSOR, "0")
            eid = await self._record_error(
                ERROR_KIND_HISTORY,
                f"检测到岸站序号重置(页首 seq={records[0]['seq']} "
                f"<= cursor={cursor})，已自动回退补采游标",
            )
            await self._resolve_error(eid)
            return True

        # 关键：整页全部提交成功后才推进游标。中途任何一条写入失败都
        # 立即抛出，游标保持原值；下次重放整页，已提交部分按主键去重。
        for raw in records:
            await self._ingest(raw, "history")
        self.last_history_at = time.time()

        if records:
            self.last_backfill_seq = records[-1]["seq"]
            # 以本页最后一条 seq 作为下次请求游标，持久化以便重启接续。
            # 写入遇锁时抛出 DatabaseBusy：本轮不推进游标，下轮重放整页，
            # 已提交记录由主键去重，不会重复入库。
            await self.db.set_meta(
                META_HISTORY_CURSOR, str(records[-1]["seq"])
            )

        # 成功一整页：恢复历史错误状态，并补记锁期间暂存的错误
        if self._backfill_open_error is not None:
            await self._resolve_error(self._backfill_open_error)
            self._backfill_open_error = None
        await self._flush_pending_errors()

        return bool(records) and bool(page.get("has_more")) or repaired
