"""SQLite 持久化层。

关键设计：
- positions 以岸站全局序号 seq 为主键，INSERT OR IGNORE 天然去重，
  SSE 与历史补采重叠的记录只入库一次。
- 连续进度 contiguous_seq 持久化在 meta 表：只有 seq=1..w 全部存在时
  水位才允许推进到 w；记录只增不改，水位单调不减，重启后自动恢复。
- ingest_errors 记录断流、翻页失败、写锁冲突等错误，恢复后写 resolved_at。

并发与写锁（这是本服务可用性的核心）：
- 写连接与读连接分离。写锁等待/重试期间，读连接仍能读取已提交快照，
  /status、/errors、/trajectories 不会被写锁拖住（WAL 允许读写并发）。
- 所有写操作都在同一把 asyncio 锁下串行执行，并在遇到 SQLite
  "database is locked" 时进行有限次重试；重试耗尽后抛出 DatabaseBusy，
  由采集循环记录错误并退避重试，绝不让循环永久退出。
- 每次写入都在同一事务内完成并提交；失败的写入不会留下半提交状态，
  因此不会推进连续水位或历史游标。
"""

from __future__ import annotations

import asyncio
import os
import sqlite3
import time
from collections.abc import Awaitable, Callable
from typing import Any, TypeVar

import aiosqlite

SQLITE_BUSY_TIMEOUT_MS = int(os.getenv("SQLITE_BUSY_TIMEOUT_MS", "5000"))
# 每次写操作的锁重试预算 = busy_timeout × attempts + 退避间隔，默认约 10.5s：
# 恰好覆盖“另一连接持锁超过 5 秒”的短暂抖动，同时保证等待有界。
# 超出预算即抛 DatabaseBusy，交由采集循环退避重试（绝不在数据库层无限阻塞）。
DB_WRITE_RETRY_ATTEMPTS = max(1, int(os.getenv("DB_WRITE_RETRY_ATTEMPTS", "2")))
DB_WRITE_RETRY_DELAY = float(os.getenv("DB_WRITE_RETRY_DELAY", "0.5"))
DB_READ_RETRY_ATTEMPTS = max(1, int(os.getenv("DB_READ_RETRY_ATTEMPTS", "3")))
# 错误登记专用：短超时单次尝试。写锁被远端长期持有时，错误登记不能
# 把采集循环阻塞数十秒——失败即转存内存，锁释放后再补记。
ERROR_WRITE_BUSY_TIMEOUT_MS = int(
    os.getenv("ERROR_WRITE_BUSY_TIMEOUT_MS", "250")
)

SCHEMA = """
CREATE TABLE IF NOT EXISTS positions (
    seq        INTEGER PRIMARY KEY,
    mmsi       TEXT    NOT NULL,
    utc_ts     REAL    NOT NULL,
    latitude   REAL    NOT NULL,
    longitude  REAL    NOT NULL,
    source     TEXT    NOT NULL CHECK (source IN ('sse', 'history')),
    received_at REAL   NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_positions_mmsi_seq ON positions(mmsi, seq);

CREATE TABLE IF NOT EXISTS meta (
    key   TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ingest_errors (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    kind        TEXT NOT NULL,
    message     TEXT NOT NULL,
    occurred_at REAL NOT NULL,
    resolved_at REAL
);
CREATE INDEX IF NOT EXISTS idx_errors_occurred ON ingest_errors(occurred_at);
"""

ERROR_KIND_SSE = "sse"
ERROR_KIND_HISTORY = "history_page"
ERROR_KIND_PARSE = "parse"
ERROR_KIND_DB = "db_write"

T = TypeVar("T")


class DatabaseBusy(RuntimeError):
    """写锁在重试窗口内始终未释放（或提交失败）。采集循环可安全重试。"""


def _is_busy_error(exc: BaseException) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).lower()
    return "locked" in message or "busy" in message


class Database:
    def __init__(self, path: str) -> None:
        self.path = path
        # 写连接：所有 INSERT/UPDATE/事务提交都走它
        self._writer: aiosqlite.Connection | None = None
        # 读连接：状态/错误/轨迹查询专用，写锁等待期间仍可读取已提交数据
        self._reader: aiosqlite.Connection | None = None
        self._write_lock = asyncio.Lock()
        # 仅用于观测：重试期间累计遇到的锁冲突次数
        self.write_lock_waits = 0
        self._schema_ready = False

    async def connect(self) -> None:
        self._writer = await self._open_connection()
        self._reader = await self._open_connection()
        await self.ensure_schema()

    async def ensure_schema(self) -> None:
        """幂等建表；写锁被远端持有时不阻塞启动，由采集循环稍后重试。

        建表失败（锁太久）时仍允许应用启动：若表已存在，一切照常；
        若确实缺失，采集循环每轮调用本方法，锁释放后自动补建。
        """
        assert self._writer is not None, "Database 尚未 connect"
        if self._schema_ready:
            return
        async with self._write_lock:
            if self._schema_ready:
                return
            # 建表尝试使用短超时：若写锁被远端持有，不能把每个写操作
            # 都卡满一个 busy_timeout，快速失败后下一轮再补建。
            await self._set_busy_timeout(ERROR_WRITE_BUSY_TIMEOUT_MS)
            try:
                await self._writer.executescript(SCHEMA)
                await self._writer.commit()
                self._schema_ready = True
            except Exception as exc:
                try:
                    await self._writer.rollback()
                except Exception:
                    pass
                if not _is_busy_error(exc):
                    raise
                self.write_lock_waits += 1
            finally:
                await self._set_busy_timeout(SQLITE_BUSY_TIMEOUT_MS)

    async def _open_connection(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        # WAL 允许采集写入与轨迹查询并发；busy_timeout 是单次尝试的等待上限
        await conn.execute("PRAGMA journal_mode=WAL")
        await conn.execute("PRAGMA synchronous=NORMAL")
        await conn.execute(f"PRAGMA busy_timeout={SQLITE_BUSY_TIMEOUT_MS}")
        return conn

    async def close(self) -> None:
        for attr in ("_writer", "_reader"):
            conn = getattr(self, attr)
            if conn is not None:
                try:
                    await conn.close()
                finally:
                    setattr(self, attr, None)

    @property
    def conn(self) -> aiosqlite.Connection:
        """兼容旧调用方：默认返回写连接。"""
        assert self._writer is not None, "Database 尚未 connect"
        return self._writer

    @property
    def reader(self) -> aiosqlite.Connection:
        assert self._reader is not None, "Database 尚未 connect"
        return self._reader

    # ----------------------------- 执行辅助 -----------------------------

    async def _write(
        self,
        op: Callable[[aiosqlite.Connection], Awaitable[T]],
        *,
        attempts: int | None = None,
        retry_delay: float | None = None,
    ) -> T:
        """串行执行写操作；遇到锁冲突按退避重试，耗尽后抛 DatabaseBusy。"""
        if attempts is None:
            attempts = DB_WRITE_RETRY_ATTEMPTS
        if retry_delay is None:
            retry_delay = DB_WRITE_RETRY_DELAY
        assert self._writer is not None, "Database 尚未 connect"
        await self.ensure_schema()
        async with self._write_lock:
            last_exc: BaseException | None = None
            for attempt in range(attempts):
                try:
                    return await op(self._writer)
                except Exception as exc:
                    last_exc = exc
                    # 失败后回滚，避免把半成品事务带入下一次尝试
                    try:
                        await self._writer.rollback()
                    except Exception:
                        pass
                    if not _is_busy_error(exc):
                        raise
                    self.write_lock_waits += 1
                    if attempt + 1 < attempts:
                        await asyncio.sleep(retry_delay * (2**attempt))
            raise DatabaseBusy(
                f"写锁重试 {attempts} 次仍未释放: {last_exc}"
            ) from last_exc

    async def _read(self, op: Callable[[aiosqlite.Connection], Awaitable[T]]) -> T:
        """读操作走独立连接；偶发 checkpoint 锁冲突时短暂重试。"""
        assert self._reader is not None, "Database 尚未 connect"
        last_exc: BaseException | None = None
        for attempt in range(DB_READ_RETRY_ATTEMPTS):
            try:
                return await op(self._reader)
            except Exception as exc:
                last_exc = exc
                if not _is_busy_error(exc) or attempt + 1 >= DB_READ_RETRY_ATTEMPTS:
                    raise
                await asyncio.sleep(DB_WRITE_RETRY_DELAY * (2**attempt))
        raise DatabaseBusy(f"读连接持续繁忙: {last_exc}") from last_exc

    @staticmethod
    async def _meta(conn: aiosqlite.Connection, key: str) -> str | None:
        cur = await conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
        row = await cur.fetchone()
        return row["value"] if row else None

    @staticmethod
    async def _seq_exists(conn: aiosqlite.Connection, seq: int) -> bool:
        cur = await conn.execute("SELECT 1 FROM positions WHERE seq = ?", (seq,))
        return await cur.fetchone() is not None

    # ----------------------------- meta -----------------------------

    async def get_meta(self, key: str, default: str | None = None) -> str | None:
        value = await self._read(lambda conn: self._meta(conn, key))
        return value if value is not None else default

    async def set_meta(self, key: str, value: str) -> None:
        async def op(conn: aiosqlite.Connection) -> None:
            await conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (key, value),
            )
            await conn.commit()

        await self._write(op)

    # ----------------------------- 船位写入 -----------------------------

    async def upsert_position(self, pos: dict[str, Any], source: str) -> bool:
        """插入一条船位；若 seq 已存在则忽略。返回是否为新记录。

        锁冲突在重试窗口内无法解决时抛出 DatabaseBusy——此时没有任何
        副作用，调用方必须据此保持历史游标/水位不变后重试。
        """

        async def op(conn: aiosqlite.Connection) -> bool:
            cur = await conn.execute(
                "INSERT OR IGNORE INTO positions"
                "(seq, mmsi, utc_ts, latitude, longitude, source, received_at) "
                "VALUES(?, ?, ?, ?, ?, ?, ?)",
                (
                    pos["seq"],
                    pos["mmsi"],
                    pos["utc_ts"],
                    pos["latitude"],
                    pos["longitude"],
                    source,
                    time.time(),
                ),
            )
            rowcount = cur.rowcount
            await conn.commit()
            return rowcount > 0

        return await self._write(op)

    async def seq_exists(self, seq: int) -> bool:
        return await self._read(lambda conn: self._seq_exists(conn, seq))

    async def advance_watermark(self) -> int:
        """从当前连续水位向后推进，遇到第一个缺口即停。返回最新水位。

        整个“探测 + 更新”在单个写事务内完成：要么水位整体推进并提交，
        要么失败回滚、水位保持原值，不会出现半更新。
        """

        async def op(conn: aiosqlite.Connection) -> int:
            stored = await self._meta(conn, "contiguous_seq")
            w = int(stored) if stored is not None else 0
            new_w = w
            while await self._seq_exists(conn, new_w + 1):
                new_w += 1
            if new_w != w:
                await conn.execute(
                    "INSERT INTO meta(key, value) VALUES('contiguous_seq', ?) "
                    "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (str(new_w),),
                )
                await conn.commit()
            return new_w

        return await self._write(op)

    async def get_watermark(self) -> int:
        return int(await self.get_meta("contiguous_seq", "0"))

    # ----------------------------- 查询（读连接） -----------------------------

    async def count_positions(self) -> int:
        async def op(conn: aiosqlite.Connection) -> int:
            cur = await conn.execute("SELECT COUNT(*) c FROM positions")
            return (await cur.fetchone())["c"]

        return await self._read(op)

    async def count_mmsi(self) -> int:
        async def op(conn: aiosqlite.Connection) -> int:
            cur = await conn.execute(
                "SELECT COUNT(DISTINCT mmsi) c FROM positions"
            )
            return (await cur.fetchone())["c"]

        return await self._read(op)

    async def first_missing_after(
        self, watermark: int, upper_bound: int, probe: int = 100
    ) -> list[int]:
        """返回水位之后、upper_bound 以内真实缺失的 seq（最多 20 个）。"""
        upper = min(watermark + probe, upper_bound)
        if upper <= watermark:
            return []

        async def op(conn: aiosqlite.Connection) -> list[int]:
            cur = await conn.execute(
                "SELECT seq FROM positions WHERE seq > ? AND seq <= ? ORDER BY seq",
                (watermark, upper),
            )
            present = {row["seq"] for row in await cur.fetchall()}
            return [
                s for s in range(watermark + 1, upper + 1) if s not in present
            ][:20]

        return await self._read(op)

    async def list_vessels(self) -> list[aiosqlite.Row]:
        async def op(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
            cur = await conn.execute(
                "SELECT mmsi, COUNT(*) AS cnt, MIN(seq) AS min_seq, "
                "MAX(seq) AS max_seq FROM positions "
                "GROUP BY mmsi ORDER BY mmsi"
            )
            return await cur.fetchall()

        return await self._read(op)

    async def trajectory(
        self, mmsi: str, watermark: int, limit: int
    ) -> tuple[list[aiosqlite.Row], int, int]:
        """只返回 seq <= 连续水位的船位（缺口之后的记录不可见）。

        返回 (可见行[按 seq 倒序限量], 该 MMSI 总接收数, 水位内可见总数)。
        """

        async def op(
            conn: aiosqlite.Connection,
        ) -> tuple[list[aiosqlite.Row], int, int]:
            cur = await conn.execute(
                "SELECT seq, mmsi, utc_ts, latitude, longitude, source, "
                "received_at FROM positions "
                "WHERE mmsi = ? AND seq <= ? ORDER BY seq DESC LIMIT ?",
                (mmsi, watermark, limit),
            )
            rows = await cur.fetchall()
            cur2 = await conn.execute(
                "SELECT COUNT(*) c FROM positions WHERE mmsi = ?", (mmsi,)
            )
            total = (await cur2.fetchone())["c"]
            cur3 = await conn.execute(
                "SELECT COUNT(*) c FROM positions WHERE mmsi = ? AND seq <= ?",
                (mmsi, watermark),
            )
            visible = (await cur3.fetchone())["c"]
            return rows, total, visible

        return await self._read(op)

    # ----------------------------- 错误记录 -----------------------------

    async def add_error(self, kind: str, message: str) -> int:
        """登记错误。

        错误登记绝不能拖住采集：这里只做一次短超时尝试，写锁被远端持有
        时快速失败并抛出 DatabaseBusy，由采集循环暂存内存、锁释放后补记。
        """

        async def op(conn: aiosqlite.Connection) -> int:
            cur = await conn.execute(
                "INSERT INTO ingest_errors(kind, message, occurred_at) "
                "VALUES(?, ?, ?)",
                (kind, message[:2000], time.time()),
            )
            lastrowid = cur.lastrowid
            await conn.commit()
            return int(lastrowid)

        await self._set_busy_timeout(ERROR_WRITE_BUSY_TIMEOUT_MS)
        try:
            return await self._write(op, attempts=1)
        finally:
            await self._set_busy_timeout(SQLITE_BUSY_TIMEOUT_MS)

    async def _set_busy_timeout(self, ms: int) -> None:
        assert self._writer is not None, "Database 尚未 connect"
        try:
            await self._writer.execute(f"PRAGMA busy_timeout={ms}")
        except Exception:
            # 忙时无法改 PRAGMA 也无碍：下一次尝试仍会在超时后失败
            pass

    async def resolve_error(self, error_id: int) -> None:
        """标记错误已恢复；同样使用短超时，避免长期持锁时阻塞采集。"""

        async def op(conn: aiosqlite.Connection) -> None:
            await conn.execute(
                "UPDATE ingest_errors SET resolved_at = ? "
                "WHERE id = ? AND resolved_at IS NULL",
                (time.time(), error_id),
            )
            await conn.commit()

        await self._set_busy_timeout(ERROR_WRITE_BUSY_TIMEOUT_MS)
        try:
            await self._write(op, attempts=1)
        finally:
            await self._set_busy_timeout(SQLITE_BUSY_TIMEOUT_MS)

    async def list_errors(
        self, limit: int, include_resolved: bool
    ) -> list[aiosqlite.Row]:
        async def op(conn: aiosqlite.Connection) -> list[aiosqlite.Row]:
            if include_resolved:
                sql = (
                    "SELECT * FROM ingest_errors "
                    "ORDER BY occurred_at DESC LIMIT ?"
                )
            else:
                sql = (
                    "SELECT * FROM ingest_errors WHERE resolved_at IS NULL "
                    "ORDER BY occurred_at DESC LIMIT ?"
                )
            cur = await conn.execute(sql, (limit,))
            return await cur.fetchall()

        return await self._read(op)

    async def count_open_errors(self) -> int:
        async def op(conn: aiosqlite.Connection) -> int:
            cur = await conn.execute(
                "SELECT COUNT(*) c FROM ingest_errors "
                "WHERE resolved_at IS NULL"
            )
            return (await cur.fetchone())["c"]

        return await self._read(op)

    # ----------------------------- 管理接口（写连接） -----------------------------

    async def reset_all(self) -> None:
        async def op(conn: aiosqlite.Connection) -> None:
            await conn.execute("DELETE FROM positions")
            await conn.execute("DELETE FROM meta")
            await conn.execute("DELETE FROM ingest_errors")
            await conn.commit()

        await self._write(op)

    async def recompute_watermark(self) -> int:
        """从头重算连续水位（故障注入/自愈后需要回退水位时使用）。"""

        async def op(conn: aiosqlite.Connection) -> int:
            w = 0
            while await self._seq_exists(conn, w + 1):
                w += 1
            await conn.execute(
                "INSERT INTO meta(key, value) VALUES('contiguous_seq', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(w),),
            )
            await conn.commit()
            return w

        return await self._write(op)

    async def delete_positions(self, seqs: list[int]) -> int:
        if not seqs:
            return 0
        placeholders = ",".join("?" for _ in seqs)

        async def op(conn: aiosqlite.Connection) -> int:
            cur = await conn.execute(
                f"DELETE FROM positions WHERE seq IN ({placeholders})", seqs
            )
            rowcount = cur.rowcount
            await conn.commit()
            return rowcount

        return await self._write(op)
