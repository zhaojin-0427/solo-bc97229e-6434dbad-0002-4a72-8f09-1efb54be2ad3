# AIS 船位连续归档系统

港口数据工程场景：本地**模拟岸站**持续产生带全局连续序号的 AIS 船位，
**采集归档服务**通过 SSE 实时流 + HTTP 历史分页两条通道完整归档，
为靠离泊分析提供无缺口轨迹。FastAPI + SQLite，Docker Compose 一键启动。

## 1. 架构与核心语义

```
                 ┌──────────────────────┐        SSE /stream（只推新记录，不补发）
                 │  模拟岸站 station:8000 │◄──────────────┐
                 │  seq 从 1 全局连续递增  │               │
                 │  记录只增不改           │        HTTP /history 分页补采
                 └──────────────────────┴──────────────┐│
                                                       ││
                                            ┌──────────▼▼───────────┐
                                            │ 采集器 collector:8001  │
                                            │  ┌──────────────────┐ │
                                            │  │ SSE 实时循环      │ │  断流指数退避重连
                                            │  │ 历史补采循环      │ │  翻页失败重试、互不影响
                                            │  └────────┬─────────┘ │
                                            │     INSERT OR IGNORE  │  seq 主键去重（重叠只入一次）
                                            │  连续水位 contiguous_seq │  仅 1..w 全在库才推进
                                            │  SQLite (WAL, 持久卷)   │
                                            └───────────────────────┘
```

**关键保证**

| 需求 | 实现机制 |
|---|---|
| 重叠记录只入库一次 | `positions.seq` 主键 + `INSERT OR IGNORE`，SSE/历史任一路径先到先得 |
| 补采期间新船位不漏 | 两个循环并发独立运行；SSE 不补发的部分由历史循环按持久化游标补齐 |
| 断流不丢数据 | SSE 循环断线指数退避重连；断线期间船位由历史循环补回 |
| 翻页失败不影响实时 | 历史循环仅记录错误并重试，SSE 循环完全独立 |
| 连续进度严谨 | 每次写入后从水位逐序号探测，遇缺口即停；水位持久化、单调不减 |
| 轨迹不越过缺口 | 查询以 `contiguous_seq` 为可见上界，缺口之后的记录一律不返回 |
| 重启自动接续 | 船位、水位、历史游标、错误全部在 SQLite；启动即恢复并继续补采 |
| 岸站重置可自愈 | 检测到序号代际回退时自动把补采游标回 0 重新补采（记录为已解决告警） |
| **写锁不中断采集** | 写连接有界重试（`busy_timeout × 重试次数`）后抛 `DatabaseBusy`，采集循环退避重试、永不退出；失败写入不推进水位/游标，稍后重放 |
| **持锁期间可查询** | 读写连接分离，WAL 快照让 `/status`、`/errors`、`/trajectories` 在写锁等待期间仍能读到已提交数据 |
| **锁释放后自动补齐** | 水位落后于已见最大序号且游标已越过水位时，历史循环自动把游标回退到水位重放，无需重启或调用回退接口 |
| **错误登记遇锁不丢** | 锁被远端持有时错误登记快速失败并暂存内存，锁释放后补记并标记已恢复 |

## 2. 快速启动（Docker Compose）

```bash
docker compose up --build
```

- 岸站 API 文档（含故障注入接口）：http://localhost:8000/docs
- 采集器 API 文档（状态/错误/轨迹/管理）：http://localhost:8001/docs
- SQLite 数据位于命名卷 `collector-data`，容器重建不丢数据。

停止与清理：

```bash
docker compose down            # 停止，保留数据卷
docker compose down -v         # 停止并删除归档数据
```

> 采集器通过健康检查等待岸站就绪后再启动。

## 3. API 一览

### 采集器（端口 8001，演示主入口）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/status` | 已接收数、连续进度 `contiguous_seq`、真实缺口、两通道计数、游标 |
| GET | `/errors?limit=&include_resolved=` | 错误查询（默认仅未恢复） |
| GET | `/vessels` | 已接收船舶清单及各自序号范围 |
| GET | `/trajectories/{mmsi}?limit=` | 按 MMSI 读取轨迹（仅返回连续水位内记录，按 seq 升序） |
| POST | `/admin/reset` | 清空归档并重置进度（演示用） |
| POST | `/admin/inject-gap` | 故障注入：删除指定序号，水位回退，演示轨迹截断语义 |
| POST | `/admin/rewind-backfill` | 历史游标回退到水位（仅演示；采集器会自动自愈，无需调用） |

`/status` 额外暴露本次故障的观测指标：`db_write_failures`（写锁等待失败次数）、
`db_write_lock_waits`（数据库层观测到的锁冲突次数）、`gap_repairs`（自动回退游标
补齐缺口的次数）、`pending_error_records`（锁期间暂存在内存、待补记的错误数）。

### 模拟岸站（端口 8000）

| 方法 | 路径 | 说明 |
|---|---|---|
| GET | `/stream` | SSE 实时流（`text/event-stream`，只推连接后的新船位） |
| GET | `/history?cursor=&limit=` | 历史分页，返回 `seq > cursor` 的记录 |
| GET | `/station/status` | 岸站生成状态 |
| POST | `/station/stream-stop` / `stream-resume` | 模拟实时流中断 / 恢复 |
| POST | `/station/history-fail-rate` | 设置翻页失败率 `{"rate": 0.5}`（0~1） |
| POST | `/station/reset` | 岸站清空并重新生成（注意见第 6 节） |

## 4. 在 Swagger UI 中的演示流程

打开 http://localhost:8001/docs ，按顺序操作（均可点 **Try it out** 执行）：

1. **观察初始采集**：GET `/status`
   - `received_total` 增长，`contiguous_seq` 紧随其后，`complete=true`，`next_missing_seqs=[]`。
2. **看实时与补采协同**：`sse_received` 与 `history_received` 同时增长，重叠序号不产生重复。
3. **演练历史翻页失败**（在 8000 文档中）：
   - POST `/station/history-fail-rate`，body `{"rate": 1.0}`；
   - 回 8001 看 `/status`：`history_failures` 上升、`sse_connected=true`、接收仍在增长；
   - GET `/errors` 可见 `history_page` 未恢复错误；
   - 再设 `{"rate": 0.0}`，错误自动 resolved，缺口被补齐、`complete` 恢复 true。
4. **演练断流重连**：
   - POST `/station/stream-stop` → `/status` 中 `sse_connected=false`、`/errors` 出现 `sse` 错误；
   - POST `/station/stream-resume` → 自动重连（`sse_reconnects` 增加），错误恢复、数据补齐。
5. **演练缺口语义**：
   - 先 GET `/vessels` 取一个 MMSI，GET `/trajectories/{mmsi}` 看完整轨迹；
   - POST `/admin/inject-gap`，body 例如 `{"seqs": [50]}`；
   - `/status` 显示 `contiguous_seq=49`、`next_missing_seqs=[50]`；
   - 再查轨迹：所有返回点 `seq ≤ 49`，缺口之后的记录不可见；
   - 无需任何手工操作：采集器会自动把历史游标回退到水位并重放缺口，
     数秒内补齐、轨迹恢复（`/status` 的 `gap_repairs` 增加）；
     `/admin/rewind-backfill` 仅保留为演示用的强制手段。
6. **错误查询**：GET `/errors?include_resolved=true` 查看完整故障史与恢复时间。
7. **演练写锁不中断采集**（命令行，非 Swagger）：

   在采集器容器内用另一连接对 `/data/collector.db` 持写锁 20 秒
   （超过 5s `busy_timeout`）：

   ```bash
   docker compose exec collector python - <<'PY'
   import sqlite3, time
   conn = sqlite3.connect('/data/collector.db', timeout=30, isolation_level=None)
   conn.execute('BEGIN IMMEDIATE'); time.sleep(20); conn.rollback()
   PY
   ```

   持锁期间反复 GET `/status`、`/errors`、`/trajectories`：立即返回已提交数据；
   锁释放后观察 `/status`：自动补齐、`complete=true`，无需重启或回退接口。

   `tests/lock_regression_test.py` 会用同样的方式自动注入持锁并断言上述行为。

## 5. 中断重启演示

```bash
docker compose restart collector        # 重启采集器，卷中数据保留
# 或本地：
# tests/restart_test.py 会真正杀掉进程再拉起，断言进度保留与自动接续
```

重启后 `/status` 中 `received_total`、`contiguous_seq`、`history_cursor` 均为重启前的值，
随后继续增长并最终 `complete=true`。

## 6. 关于“重置”的正确姿势

- 重新开始一轮干净演练时，**两侧都要重置**（端到端测试即如此）：
  先 POST 采集器 `/admin/reset`，再 POST 岸站 `/station/reset`。
- 若只重置岸站：采集器能检测到序号代际回退并自动回退补采游标（不会卡死），
  但旧代际船位因 `seq` 主键冲突会保留在库中。这是去重主键的预期安全行为；
  需要全新数据集请使用 `/admin/reset` 清空归档。

## 7. 本地开发（无 Docker 时）

```bash
python3 -m pip install -r requirements.txt   # 建议虚拟环境
# 终端 1：岸站
uvicorn shore_station.app:app --port 8000
# 终端 2：采集器
STATION_URL=http://127.0.0.1:8000 DB_PATH=./data/collector.db \
  uvicorn collector.app:app --port 8001
```

## 8. 自动化测试

```bash
# 基础容错与缺口语义（32 项断言）
python3 tests/e2e_test.py
# 真正杀进程重启的接续测试
python3 tests/restart_test.py --pylibs <依赖目录>
# 写锁 / 网络恢复 / 双通道交错回归（需要运行中的两个服务）
python3 tests/lock_regression_test.py
# 数据库层确定性回归（无需启动任何服务，临时 SQLite）
PYTHONPATH=. python3 tests/lock_unit_test.py
```

`lock_regression_test.py` 在“历史接口恢复、补采开始”的时点用另一连接
`BEGIN IMMEDIATE` 持写锁超过 `busy_timeout`，断言：

- 持锁期间 `/status`、`/errors` 持续可读且读到已提交数据（读写连接分离）；
- 写入等待重试超出预算后采集循环不退出（`db_write_failures` 增加），
  失败写入不推进水位/游标；
- 锁释放后自动补齐缺口（`gap_repairs` 增加），`complete=true`，无需重启、
  未调用 `/admin/rewind-backfill`；
- 两路重叠仍只入库一次、库内 seq 唯一；缺口后的船位不可查询；
- 持锁期间杀掉进程再重启，按已提交进度接续并最终补齐。

数据库层测试用可控的短超时验证同一语义（持锁写入抛 `DatabaseBusy` 而非卡死、
锁期间读已提交数据、整页游标不前进、释放后重放去重、错误登记暂存后补记）。
Docker 环境下可传 `--restart-command "docker compose restart collector"`。


## 9. 目录结构

```
.
├── docker-compose.yml        # station + collector 两服务，SQLite 命名卷
├── Dockerfile                # 共用镜像，compose 中以不同 command 启动
├── requirements.txt
├── shore_station/app.py      # 模拟岸站：SSE 流、历史分页、断流/失败注入
├── collector/
│   ├── db.py                 # SQLite：去重写入、连续水位、错误、查询
│   ├── ingest.py             # SSE 循环 + 历史补采循环、退避重试、代际检测
│   └── app.py                # FastAPI：状态/错误/船舶/轨迹/管理接口
└── tests/
    ├── e2e_test.py
    ├── restart_test.py
    ├── lock_regression_test.py   # 持写锁/网络恢复/双通道交错回归
    └── lock_unit_test.py         # 数据库层确定性回归（无需服务）
```

## 10. 主要环境变量

| 变量 | 默认 | 作用 |
|---|---|---|
| `GEN_INTERVAL` | 0.25 | 岸站船位生成间隔（秒） |
| `INITIAL_COUNT` | 60 | 岸站启动时预生成历史条数 |
| `HISTORY_PAGE_LIMIT` | 100 | 岸站历史分页上限 |
| `STATION_URL` | http://station:8000 | 采集器访问的岸站基址 |
| `DB_PATH` | /data/collector.db | SQLite 文件路径 |
| `HISTORY_PAGE_SIZE` | 50 | 采集器补采分页大小 |
| `SSE_READ_TIMEOUT` | 60 | SSE 读超时（秒），超时判定断流并重连 |
| `SSE_RECONNECT_MAX` / `HISTORY_RETRY_MAX` | 30 / 30 | 两类重试的最大退避（秒） |
| `SQLITE_BUSY_TIMEOUT_MS` | 5000 | 单次写锁等待上限（毫秒） |
| `DB_WRITE_RETRY_ATTEMPTS` / `DB_WRITE_RETRY_DELAY` | 2 / 0.5 | 数据库层重试次数与退避基数（秒）；预算约 10.5s |
| `ERROR_WRITE_BUSY_TIMEOUT_MS` | 250 | 错误登记专用短超时，避免持锁时拖住采集 |
| `LOCK_RETRY_BASE` / `LOCK_RETRY_MAX` | 0.5 / 10 | 采集循环遇 `DatabaseBusy` 的退避（秒） |
| `GAP_REPAIR_MIN_INTERVAL` | 1.0 | 自动回退游标补缺口的间隔下限（秒） |
