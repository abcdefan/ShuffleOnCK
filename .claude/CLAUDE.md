When working with a branch, do not use rebase or amend - add new commits instead.

Do not commit to the master branch. Create a new branch for every task.

When writing text such as documentation, comments, or commit messages, wrap literal names from ClickHouse SQL language, classes and functions, or literal excerpts from log messages inside inline code blocks, such as: `MergeTree`.

When writing text such as documentation, comments, or commit messages, write names of functions and methods as `f` instead of `f()` - we prefer it for mathematical purity when it refers a function itself rather than its application.

When mentioning logical errors, say "exception" instead of "crash", because they don't crash the server in the release build.

Links to ClickHouse CI should be analyzed using the tool at `.claude/tools/fetch_ci_report.js`, which directly fetches the underlying JSON data without requiring a browser. It accepts GitHub PR URLs (fetches all CI reports) or direct S3/CI HTML URLs.

```bash
# Fetch all CI reports for a PR
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345"

# Show only failed tests with CIDB links
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345" --failed --cidb

# Fetch only a specific report from a PR (by index)
node .claude/tools/fetch_ci_report.js "https://github.com/ClickHouse/ClickHouse/pull/12345" --report 2

# Filter by test name, show artifact links
node .claude/tools/fetch_ci_report.js "<url>" --test peak_memory --links

# Download logs and show failed tests
node .claude/tools/fetch_ci_report.js "<url>" --failed --download-logs

# Options:
#   --test <name>               Filter tests by name
#   --failed                    Show only failed tests
#   --all                       Show all test results
#   --links                     Show artifact links (logs.tar.gz, etc.)
#   --cidb                      Show CIDB links for failed tests
#   --report <number>           For PR URLs: fetch only one specific report
#   --download-logs [path]      Download logs to path (default: /tmp/ci_logs.tar.{gz,zst})
#   --credentials <user,password>  HTTP Basic Auth for private repositories
```

After downloading logs, extract specific test logs:
```bash
tar -xzf /tmp/ci_logs.tar.gz ci/tmp/pytest_parallel.jsonl
grep "test_name" ci/tmp/pytest_parallel.jsonl | python3 -c "import sys,json; [print(json.loads(l).get('longrepr','')) for l in sys.stdin if 'failed' in l]"
```

To analyze CI performance comparison results (slower/faster queries, unstable queries), use the tool at `.claude/tools/fetch_perf_report.py`. It fetches the machine-readable `all-query-metrics.tsv` from S3 for each performance shard, filters to `client_time`, and classifies queries as changed or unstable using the same thresholds as `compare.sh`.

```bash
# Show performance changes for a PR (default: changed + unstable queries only)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345"

# Filter by architecture
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --arch amd

# Show only per-shard summary (no individual queries)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --summary

# Filter by test name
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --test group_by

# Show all queries (not just changes)
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --all --sort times

# JSON output for structured analysis
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --json

# TSV output for piping
python3 .claude/tools/fetch_perf_report.py "https://github.com/ClickHouse/ClickHouse/pull/12345" --tsv

# Also accepts CI HTML URLs
python3 .claude/tools/fetch_perf_report.py "https://s3.amazonaws.com/clickhouse-test-reports/json.html?PR=12345&sha=abc123"
```

Key options: `--arch <amd|arm|all>` to filter architecture, `--metric <name>` to change metric (default `client_time`), `--shard <n>` for a specific shard, `--test <name>` / `--query <text>` for substring filtering, `--sort <diff|times|threshold|test>` for ordering, `--summary` for shard-level overview only, `--json` / `--tsv` for machine-readable output.

To compile and run C++ code snippets against the ClickHouse codebase without modifying any source files, use the tool at `.claude/tools/cppexpr.sh`. This is a wrapper around `utils/c++expr` that auto-detects build directories and handles working directory setup. When asked about the size, layout, or alignment of ClickHouse data structures, or asked to compare performance of code snippets, use this tool to get a definitive answer instead of guessing.

```bash
# Query the size of a ClickHouse data structure
.claude/tools/cppexpr.sh -i Core/Block.h 'OUT(sizeof(DB::Block))'

# Query multiple expressions at once
.claude/tools/cppexpr.sh -i Core/Field.h 'OUT(sizeof(DB::Field)) OUT(sizeof(DB::Array))'

# Use global code for helper functions or custom types
.claude/tools/cppexpr.sh -g 'struct Foo { int a; double b; };' 'OUT(sizeof(Foo)) OUT(alignof(Foo))'

# Benchmark a code snippet (100000 iterations, 5 tests)
.claude/tools/cppexpr.sh -i Common/Stopwatch.h -b 100000 'Stopwatch sw;'

# Standalone mode (no ClickHouse headers, just standard C++)
.claude/tools/cppexpr.sh --plain 'OUT(sizeof(std::string))'
```

Key options: `-i HEADER` to include headers, `-g 'CODE'` for global-scope code, `-b STEPS` for benchmarking, `-l LIB` to link extra libraries, `--plain` for standalone compilation without ClickHouse. The `OUT(expr)` macro prints `expr -> value`.

When asked to analyze assembly, inspect generated code, find register spills, check branch density, compare codegen between builds, or investigate optimization opportunities in compiled functions, use the tool at `.claude/tools/analyze-assembly.py`. It disassembles functions from a compiled binary, builds a CFG, computes metrics (spill/branch/call density), and reports findings. Use it instead of manually running `llvm-objdump` or `llvm-nm`.

```bash
# Basic analysis of a function
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>"

# Search for overloaded/templated functions by regex
python3 .claude/tools/analyze-assembly.py <binary> "insertRangeFrom" --search

# Pick a specific overload from ambiguous results
python3 .claude/tools/analyze-assembly.py <binary> "insertRangeFrom" --search --select 3

# JSON output for structured analysis
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --format json

# Source-interleaved disassembly (needs debug info)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --source

# Microarchitectural analysis of loop bodies (--mcpu is required)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --mca --mcpu=znver3

# Profile-weighted analysis (re-ranks findings by runtime impact)
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" --perf-map tmp/perf.map.jsonl

# Compare codegen between two builds
python3 .claude/tools/analyze-assembly.py --before <old_binary> --after <new_binary> "<function_name>"

# Analyze function at a specific address (useful for heavily-templated symbols)
python3 .claude/tools/analyze-assembly.py <binary> 0x0dc7c780

# Verbose mode to see tool commands
python3 .claude/tools/analyze-assembly.py <binary> "<function_name>" -v
```

Key options: `--search` for regex matching, `--fuzzy` for substring matching, `--select N` to pick from ambiguous results, `--all` to analyze all matches, `--context N` to show surrounding symbols, `--max-instructions N` to control output size, `--mca --mcpu=<model>` for llvm-mca throughput analysis, `--perf-map <file>` for runtime-weighted scoring, `--before`/`--after` for diff mode. Hex addresses (e.g. `0x0dc7c780`) are resolved to the enclosing symbol automatically — useful when symbol names are too long for regex matching. The tool caches symbol tables by build-id for fast repeated queries.

You can build multiple versions of ClickHouse inside `build_*` directories, such as `build`, `build_debug`, `build_asan`, etc.

You can run integration tests as in `tests/integration/README.md` using: `python -m ci.praktika run "integration" --test <selectors>` invoked from the repository root.

When writing tests, do not add "no-*" tags (like "no-parallel") unless strictly necessarily.

When writing tests in tests/queries, prefer adding a new test instead of extending existing ones.

When adding a new test, consult `./tests/queries/0_stateless/add-test` to determine the correct name prefix for the new test.

When writing C++ code, always use Allman-style braces (opening brace on a new line). This is enforced by the style check in CI.

Never use sleep in C++ code to fix race conditions - this is stupid and not acceptable!

When writing messages, say ASan, not ASAN, and similar (because there are two words: Address Sanitizer).

When checking the CI status, pay attention to the comment from robot with the links first. Look at the Praktika reports first. The logs of GitHub actions usually contain less info.

Do not use `-j` argument with ninja; do not use `nproc` - let it decide automatically.

When building ClickHouse (running ninja), always redirect output to the build log file in the build directory. Always use a subagent to analyze the log and return only a concise summary.

When running tests, always redirect output to a log file in the build directory (e.g. `<build_directory>/test_<test_name>.log`). Use unique file names per test so multiple tests can run in parallel. Always use a subagent to analyze each log and return only a concise summary.

If I provided a URL with the CI report, logs, or examples, include it in the commit message.

When creating or updating a pull request, use `.github/PULL_REQUEST_TEMPLATE.md` as the PR body template. The body should contain: a short description of the change and motivation, then the Changelog category (leave one from the list), then the Changelog entry, then the Documentation entry checkbox. Do not invent a custom "## Summary" or "## Test plan" structure — follow the template exactly. The "Bug Fix" category should be used only for real bug fixes, while for fixing CI reports you can use the "CI Fix or improvement" category. Include the URL to CI report I provided if any. If the PR is about a CI failure, search for the corresponding open issues and provide a link in the PR description.

ARM machines in CI are not slow. They are similar to x86 in performance.

Use `tmp` subdirectory in the current directory for temporary files (logs, downloads, scripts, etc.), do not use `/tmp`. Create the directory if needed.

## Distributed shuffle join development notes

This section records the current plan for implementing real distributed `shuffle join`. It is intentionally written in Chinese because the design discussion for this branch is in Chinese.

### 设计方案

当前 `exchange` 分支的设计以仓库根目录的 `shuffle_join_design.md` 为准。旧的内部 `DistributedShuffleJoinExchangeRegistry` / receiver 方案已经不再作为主线，相关代码也已删除。第一版目标是实现一个基于普通 `Memory` shuffle 表、`RemoteSink` / `INSERT` 和多阶段 coordinator 的真实 push-based `shuffle join`。

现有分布式 `JOIN` 在 `distributed_product_mode = 'allow'` 时，本质是每个 shard 都拉取完整右表并在本地 `JOIN`。右表较大时会产生重复网络传输和重复内存占用。旧 demo 通过 SQL 改写加 `cityHash64(join_key) % N = shardNum()` 只能证明分桶正确性，本质仍是拉模式：接收端拉全量后过滤，网络没有真正减少。

新方案必须满足：每个 source shard 只读本地左右表一次，在发送端按 `JOIN` key 分桶，每一行只发送到一个 target shard。target shard 收到属于自己的 left/right bucket 后，执行本地 `JOIN`，initiator 汇总所有 target shard 的结果。

### MVP 支持范围

第一版只做窄范围 MVP，用来证明执行模型是真正的 push-based shuffle：

- `enable_analyzer = 1`。
- 实验开关使用当前代码里的 `distributed_shuffle_join`，设计文档里的 `enable_shuffle_join` 先理解为同一个能力的外部命名。
- `INNER ALL JOIN`。
- 单个等值 `JOIN` key，例如 `USING (id)` 或简单 `ON a.id = b.id`。
- 左右两边都是直接的 `StorageDistributed` 表。
- 左右两边属于同一个 `cluster`。
- 每个 shard 只使用一个 replica，暂时不支持 parallel replicas。
- shuffle 中间数据使用普通 `Memory` 表，数据超限可以先报错。
- 暂时不支持 spill、复杂子查询、多 key、多种 `JOIN` strictness、`LEFT` / `RIGHT` / `FULL` / `ASOF` / `SEMI` / `ANTI` 等复杂语义。

### 执行流程

完整流程由 initiator 侧 `ShuffleExchangeCoordinator` 编排：

```text
Prepare:
  initiator 向所有 shard 发送 CREATE TABLE
  每个 shard 创建 left/right 两张 _shuffle_* Memory 表
  全部确认后才能进入 Exchange

Exchange:
  initiator 向所有 shard 发送 Exchange 请求
  每个 source shard 读取本地 left/right 表
  按 JOIN key 计算目标 shard
  目标是自己时直接写本地 _shuffle_* 表
  目标是远端时通过 RemoteSink / INSERT 写远端 _shuffle_* 表

Barrier:
  initiator 等所有 shard 的 Exchange 请求返回
  所有 Exchange 成功后，说明每个 target shard 的 left/right bucket 已完整

Local JOIN:
  initiator 向所有 shard 下发读取 _shuffle_* 表的本地 JOIN SQL
  每个 shard 返回自己 bucket 的 JOIN 结果
  initiator 汇总所有 shard 的结果

Cleanup:
  initiator 正常或异常退出时清理所有 shard 上的 _shuffle_* 表
```

每个 shard 上都有同名 `_shuffle_*` 表，但每张表只保存该 shard 负责的 hash bucket，不是完整全量表。比如 3 shard 下 `hash(id) % 3 = 1` 的 left/right 行才会写入 shard 1 的 `_shuffle_*` 表。

### Shuffle 表

不要使用 `CREATE TEMPORARY TABLE`。ClickHouse temporary table 是 session 级别的，Exchange 阶段的 `INSERT` 连接和 Local `JOIN` 阶段的查询连接不是同一个 session，后者看不到前者创建的 temporary table。

MVP 使用普通 `Memory` 表，创建在分布式表所在 database 中：

```sql
CREATE TABLE IF NOT EXISTS {database}._shuffle_{query_id}_{join_id}_left (...) ENGINE = Memory
CREATE TABLE IF NOT EXISTS {database}._shuffle_{query_id}_{join_id}_right (...) ENGINE = Memory
```

表名必须包含 query 级唯一标识，避免并发查询冲突。正常路径下 Local `JOIN` 完成后 drop；异常路径下 coordinator 用 RAII 清理；未来再加后台 cleanup 线程扫描超时的 `_shuffle_*` 残留表作为兜底。

shuffle 表 schema 来自原始查询需要的最小列集合：

- `JOIN` key 列。
- `SELECT` 中涉及该表的列。
- 单表 `WHERE` 下推条件涉及的列。
- post-join filter 仍然需要的列。

列类型从原始本地表 metadata 获取。第一版可以先只覆盖 MVP 查询形态，不要一开始做完整表达式列分析。

### Exchange Pipeline

每个 source shard 上，左右表各执行一遍 push pipeline：

```text
ReadFromLocal
  -> ExpressionTransform / selector 计算 target shard
  -> ShufflePartitionTransform 或 DistributedShuffleJoinSink 拆分 block
  -> target == self: 写本地 Memory 表
  -> target != self: SquashingTransform -> RemoteSink / RemoteInserter
```

分桶逻辑参考 `DistributedSink` / `createBlockSelector` / `IColumn::scatter`。当前代码已经有 `DistributedShuffleJoinSelector` 和 `DistributedShuffleJoinSink` 骨架，可以继续用它们推进 MVP；如果后续要更贴合 processor 模型，可以把 sink 内部拆分能力演进成 1-to-N 的 `ShufflePartitionTransform`。

注意本地短路：目标 shard 是自己时必须直接写本地 `_shuffle_*` 表，不要通过网络连回自己。

### 多阶段协调

`ShuffleExchangeCoordinator` 是后续主入口，职责是：

1. 根据 `DistributedShuffleJoinAnalyzer` 的结果生成 `_shuffle_*` 表名和 schema。
2. 对所有 shard 并行发送 `CREATE TABLE`，全部成功才开始 Exchange。
3. 对所有 shard 并行发送 Exchange 请求。第一版可以优先实现设计文档中的方案 A：内部 `SYSTEM SHUFFLE EXCHANGE ...` 或等价内部命令；方案 B 是通过 settings 携带 shuffle 上下文的 `INSERT SELECT`，但更 hack。
4. 等所有 Exchange 请求返回，作为 barrier。
5. 下发读取 `_shuffle_*` 表的 local `JOIN` SQL，汇总结果。
6. 正常或异常都尽力 `DROP TABLE IF EXISTS` 清理。

Barrier 不需要再靠 target 侧 `finish` 计数对象表达；第一版以“所有 shard 的 Exchange 请求都返回成功”为 barrier。

### WHERE 下推和列裁剪

Exchange 前应尽量下推只涉及单表的条件，减少 shuffle 数据：

- 只引用左表列的条件下推到左表本地读取。
- 只引用右表列的条件下推到右表本地读取。
- 同时引用左右两表的条件保留到 Local `JOIN` 后处理。
- `OR`、子查询和复杂条件第一版可以保守处理，不强行拆分。

列裁剪同样是性能必要项。Exchange 阶段只读取 shuffle 表 schema 所需列，不要 `SELECT *`。

### 错误处理和清理

整体原则：任一阶段出错，终止后续阶段，向客户端返回 exception，并清理已创建的 `_shuffle_*` 表。

- `Prepare` 失败：清理已经创建成功的表。
- `Exchange` 失败：取消还在运行的 Exchange 请求，清理所有 `_shuffle_*` 表。某些 target 表里已有部分数据也没关系，整个查询已经失败。
- Local `JOIN` 失败：按普通查询失败处理，然后清理。
- initiator 失败：第一版可能留下 `_shuffle_*` 表，未来需要后台 cleanup 线程按前缀和超时清理。

### 测试要求

integration tests 至少覆盖：

- 左右表故意不按 `JOIN` key 落位，结果仍然正确。
- 每个 source shard 每侧输入只扫描一次。
- target shard 的 `_shuffle_*` 表能接收来自多个 source shard 的同一 side 数据。
- `distributed_shuffle_join = 0` 时不改变现有路径。
- query 取消、异常、source shard 失败时尽量清理 `_shuffle_*` 表。

跑 integration tests 时输出必须重定向到 build 目录日志文件。当前系统指令要求只有用户显式要求 sub-agent 时才使用 sub-agent，因此默认不要为了日志分析自动 spawn sub-agent。

### 当前实现进度

截至 `2026-05-21`，`exchange` 分支相对 `master` 的 `shuffle join` 相关进度如下。这个小节用于协作交接；后续每完成一个独立阶段，都需要同步更新这里。

已提交到当前分支的基础改动：

- `.claude/CLAUDE.md` / `AGENTS.md`：增加并维护当前 `shuffle join` MVP 的中文设计记录、开发步骤和注意事项。
- `.gitignore`：增加本地 `Codex` 配置忽略项，避免把个人 `.codex` 配置提交进仓库。
- `src/Core/Settings.cpp`：新增实验 setting `distributed_shuffle_join`，默认关闭。当前只作为功能入口开关，尚未接入执行路径。
- `src/Storages/DistributedShuffleJoinAnalyzer.h` 和 `src/Storages/DistributedShuffleJoinAnalyzer.cpp`：新增 `DistributedShuffleJoinAnalyzer` helper。它只负责判断一条 query 是否满足 MVP `shuffle join` 条件，并提取左右 `StorageDistributed`、`JOIN` key、cluster、shard 数、所需列等信息；它不负责改写 SQL，也不负责执行。
- `src/Storages/DistributedShuffleJoinSelector.h` 和 `src/Storages/DistributedShuffleJoinSelector.cpp`：新增 MVP 版 `JOIN` key selector builder。
  - `createDistributedShuffleJoinSelector` 接收 `ClusterPtr` 和 key column name，返回 `DistributedShuffleJoinSelector`。
  - 当前只支持 key 已经是 block 中的直接列，暂不支持复杂表达式 key。
  - 内部复用 `createBlockSelector` 和 cluster 的 `slot_to_shard`，因此遵循 `Distributed` sharding 的 slot/weight 规则。
  - 当前类型范围与 `StorageDistributed` 的整数 sharding key 路径对齐，支持整数类型和整数字典的 `LowCardinality`，暂不支持 `String`、`Nullable` 等 key。

当前未提交的新一阶段改动：

- 已根据 `shuffle_join_design.md` 将 `AGENTS.md` 的设计思路调整为普通 `Memory` shuffle 表方案，旧的 registry/internal exchange 设计不再作为主线。
- `src/Storages/DistributedShuffleJoinTables.h` 和 `src/Storages/DistributedShuffleJoinTables.cpp`：新增 `_shuffle_*` 表名和 SQL helper。
  - `DistributedShuffleJoinExchangeId` 已移动到这里，只作为生成 `_shuffle_*` 表名的 query/join 标识。
  - `createDistributedShuffleJoinTableNames` 根据 `DistributedShuffleJoinExchangeId` 和 database 生成 left/right 表名。
  - `createDistributedShuffleJoinMemoryTableQuery` 生成 `CREATE TABLE IF NOT EXISTS ... ENGINE = Memory`。
  - `dropDistributedShuffleJoinTableQuery` 生成 `DROP TABLE IF EXISTS ...`。
  - 当前只负责生成 SQL 字符串，不负责发送 SQL，也不负责建表清理的生命周期。
- `src/Interpreters/DistributedShuffleJoinCoordinator.h` 和 `src/Interpreters/DistributedShuffleJoinCoordinator.cpp`：新增 initiator 侧 coordinator 骨架。
  - `IDistributedShuffleJoinQueryExecutor` 抽象“向某个 shard 执行一条 SQL”的动作。
  - `ClusterDistributedShuffleJoinQueryExecutor` 是当前真实执行器：本地 shard 通过 `executeQuery` 执行内部 query，远端 shard 通过 `ConnectionPoolWithFailover` 获取连接并用 `Connection::sendQuery` 发送 query，然后等待 `EndOfStream` 或远端 exception。
  - `DistributedShuffleJoinCoordinator` 当前实现 `prepareShuffleTables` 和 `cleanupShuffleTables`。
  - `prepareShuffleTables` 会向所有 shard 发送 left/right 两张 `_shuffle_*` `Memory` 表的 `CREATE TABLE`。
  - `exchangeShuffleTables` 会在 `Prepare` 完成后对所有 source shard 调用 Exchange executor；Exchange 中途失败会立即 cleanup。
  - `joinShuffleTables` 会在 `Exchange` 完成后对所有 target shard 调用 Local `JOIN` executor；Local `JOIN` 中途失败会立即 cleanup。
  - `cleanupShuffleTables` 会 best-effort 向所有 shard 发送 left/right 两张表的 `DROP TABLE IF EXISTS`，析构时也会触发清理。
  - 如果 prepare 中途抛 exception，会立即执行 cleanup 再继续抛出原 exception。
- `src/Storages/DistributedShuffleJoinSink.h` 和 `src/Storages/DistributedShuffleJoinSink.cpp`：`DistributedShuffleJoinSink` 现在携带 `DistributedShuffleJoinTableNames`，并把表名上下文传给 `DistributedShuffleJoinBlockSender`。
  - 这样后续 remote sender 可以直接使用 side 对应的 `_shuffle_*` 表执行 `INSERT`。
  - 已删除 `source_shard_count`、`source_shard_index` 和 registry-style finish 语义；barrier 由 coordinator 等待所有 Exchange 请求返回来表达。
- `src/Storages/DistributedShuffleJoinExchangePipeline.h` 和 `src/Storages/DistributedShuffleJoinExchangePipeline.cpp`：新增 Exchange side pipeline helper。
  - `DistributedShuffleJoinExecutionPlan` 将一次 MVP `shuffle join` 执行所需的 `_shuffle_*` 表名、left/right table header 和 target shard 本地 `JOIN` SQL 收在一起。
  - `createDistributedShuffleJoinExecutionPlan` 可以从 analyzer 输出生成上述执行计划；analyzer 现在会记录 `shuffle_database`，真实接入时不需要调用方再猜 shuffle 表建在哪个 database。
  - `executeDistributedShuffleJoinStages` 可以用已有 coordinator 按 `Prepare -> Exchange -> Local JOIN -> Cleanup` 顺序执行一个 plan；目前 Local `JOIN` 仍通过 executor 下发，不负责把结果 pipeline 接回 initiator。
  - `prepareDistributedShuffleJoinExchange` 可以只执行 `Prepare -> Exchange` 并返回仍持有 `_shuffle_*` 表生命周期的 coordinator，供后续真实 `SELECT` 路径在读结果 pipeline 期间延后 cleanup。
  - `holdDistributedShuffleJoinCoordinator` 可以把 coordinator 包进 `QueryPlanResourceHolder` 的 custom resource，后续真实结果 pipeline 可以持有该资源并在 pipeline 生命周期结束时触发 coordinator 析构 cleanup。
  - `attachDistributedShuffleJoinCoordinator` 可以直接把 coordinator resource attach 到 `QueryPipeline`，后续真实结果 pipeline 可以复用这个 helper 保证读取完成或失败后 cleanup。
  - `executeDistributedShuffleJoinLocalJoinPipeline` 可以执行 plan 中的本机本地 `JOIN` SQL，返回 `BlockIO`，并把 coordinator attach 到返回的 result pipeline。
  - `buildDistributedShuffleJoinClusterLocalJoinQueryPlan` 可以把 plan 中的本地 `JOIN` SQL 包装成 `ClusterProxy::executeQuery` 读取所有 target shard 的 `QueryPlan`。
  - `executeDistributedShuffleJoinClusterLocalJoinPipeline` 可以从所有 target shard 汇总 `_shuffle_*` 本地 `JOIN` 结果，返回带 cleanup 生命周期的 result `BlockIO`。当前还没有接到真实用户 `SELECT` 路径。
  - `executeDistributedShuffleJoinLocalJoinAndCleanup` 可以在已有 coordinator 上执行 Local `JOIN` 后清理表，保留了单测/阶段调用的简单路径。
  - `createDistributedShuffleJoinExchangeHeader` 根据 `DistributedShuffleJoinInfo` 和 side 生成 left/right shuffle 输入 header。
  - `createDistributedShuffleJoinExchangeSourceQuery` 根据 required columns 和本地表名生成 source shard 本地读取 SQL。
  - `createDistributedShuffleJoinExchangeSourceQuery` 也可以直接从 `DistributedShuffleJoinInfo` 中的 left/right `StorageDistributed` 取 remote database/table 来生成本地读取 SQL。
  - `createDistributedShuffleJoinLocalJoinQuery` 根据 `_shuffle_*` 表名和 left/right key 列生成 target shard 上执行的本地 `INNER ALL JOIN` SQL。
  - `createDistributedShuffleJoinRemoteExchangeQuery` 生成 initiator 后续可发给远端 source shard 的内部 `SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE ...` 命令。
  - `createDistributedShuffleJoinExchangeSink` 根据 analyzer 输出、cluster、table names、side 和 sender 创建对应的 `DistributedShuffleJoinSink`。
  - `executeDistributedShuffleJoinExchangeQuery` 会执行内部 source query，并把输出 pipeline 接到对应的 `DistributedShuffleJoinSink`。
  - `executeDistributedShuffleJoinExchangeSide` 会从 analyzer 结果生成 side source query 并执行到对应 sink。
  - `executeDistributedShuffleJoinExchangeSource` 会在 source shard 本地顺序执行 left/right 两侧 Exchange；任一侧失败会 cancel sender。
  - `serializeDistributedShuffleJoinExchangePayload` / `parseDistributedShuffleJoinExchangePayload` 可以把远端 source shard Exchange 所需的 cluster、shard count、`_shuffle_*` 表名、左右本地 source 表、key 列和 required columns 编成 JSON payload 并解析回来。
  - `createDistributedShuffleJoinInfoFromExchangePayload` 可以从 payload 还原执行 Exchange sink/selector 需要的 `DistributedShuffleJoinInfo` 子集。
  - `executeDistributedShuffleJoinExchangePayload` 可以在收到远端 `SYSTEM` 请求的 shard 上解析 payload，读取左右本地 source 表，并把数据通过 `ClusterDistributedShuffleJoinBlockSender` 写入目标 `_shuffle_*` 表。
  - `LocalDistributedShuffleJoinExchangeSideExecutor` 是 source shard 进程内的真实 side executor，内部复用 `executeDistributedShuffleJoinExchangeSide`。
  - `CurrentShardDistributedShuffleJoinExchangeExecutor` 可以把 coordinator 的 Exchange executor 接口落到当前 shard 进程内执行；它既支持测试注入 fake side executor，也支持通过 `context`、`cluster`、analyzer info 和 sender 自己持有真实 `LocalDistributedShuffleJoinExchangeSideExecutor`。如果 coordinator 要它执行非当前 shard，会明确报 `NOT_IMPLEMENTED`；远端 source shard 现在走内部 `SYSTEM` 命令入口，但 coordinator 还没自动生成 payload 并下发。
  - `SystemQueryDistributedShuffleJoinExchangeExecutor` 可以把 coordinator 的 Exchange executor 接口转换成对 source shard 的内部 `SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE '<json>'` SQL 下发；它复用 `IDistributedShuffleJoinQueryExecutor`，后续 coordinator 可以用同一套 shard SQL 执行器触发远端 source Exchange。
  - `createCurrentShardDistributedShuffleJoinExchangeExecutor` 是当前 shard Exchange executor 的真实工厂，会创建 `ClusterDistributedShuffleJoinBlockSender` 并接上 `LocalDistributedShuffleJoinExchangeSideExecutor`。
  - 这一步把 analyzer 输出、source query、selector、header 和 sink 串成稳定入口，后续 Exchange 请求处理可以按 left/right side 调用它。
- `src/Parsers/ASTSystemQuery.h`、`src/Parsers/ASTSystemQuery.cpp`、`src/Parsers/ParserSystemQuery.cpp` 和 `src/Interpreters/InterpreterSystemQuery.cpp`：新增内部远端 Exchange 触发命令骨架。
  - 新增 `SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE '<payload>'` 的 AST 类型、parser 和 formatter。
  - interpreter 分支现在会调用 `executeDistributedShuffleJoinExchangePayload`，解析 JSON payload 并执行当前 source shard 的 left/right Exchange。
  - 这一步已经铺通远端 source shard 收到内部 `SYSTEM` 请求后的本地执行入口；后续还需要 coordinator 自动生成 payload 并通过 `ClusterDistributedShuffleJoinQueryExecutor` 下发到所有 source shard。
- `src/Storages/DistributedShuffleJoinBlockSender.h` 和 `src/Storages/DistributedShuffleJoinBlockSender.cpp`：新增基于 cluster 的真实 table sender。
  - `ClusterDistributedShuffleJoinBlockSender` 以 `(target shard, side)` 为粒度复用 pushing pipeline。
  - target shard 是本地节点时，使用 `InterpreterInsertQuery` 构造本地 `INSERT INTO _shuffle_*` pipeline。
  - target shard 是远端节点时，使用 `ConnectionPoolWithFailover` 获取连接，再通过 `RemoteSink` 推送 `INSERT INTO _shuffle_*` block。
  - `finish` 会收口对应 target/side 的 pushing pipeline；`cancel` 和析构会 best-effort 取消仍在运行的 job。
- 已删除 `src/Storages/DistributedShuffleJoinExchange.h` 和 `src/Storages/DistributedShuffleJoinExchange.cpp`。
  - 这些文件属于旧的内部 registry 方案，不再符合当前 `Memory` 表 MVP 主线。
- `src/Core/Settings.cpp`：新增实验 setting `shuffle_exchange_timeout_ms`，默认 `300000`。
- `src/Storages/tests/gtest_distributed_shuffle_join.cpp`：将本地闭环测试改为 table-sender 形态，并新增 coordinator 测试。
  - `CreatesMemoryTableNamesAndQueries` 验证 `_shuffle_*` 表名、`CREATE TABLE` 和 `DROP TABLE` SQL 生成。
  - `CreatesLocalJoinQuery` 验证 target shard 本地 `JOIN` SQL 生成。
  - `CreatesRemoteExchangeQuery` 验证远端 source Exchange 内部 `SYSTEM` 命令生成。
  - `SerializesAndParsesRemoteExchangePayload` 验证远端 source Exchange JSON payload 可以保留 cluster、表名、source 表、key 和 required columns，并能用 payload 生成 source query。
  - `ParsesRemoteExchangeSystemQuery` 验证远端 source Exchange 内部 `SYSTEM` 命令可以解析并格式化回等价 SQL。
  - `CurrentShardExchangeExecutorRunsSourceExchange` 验证当前 shard Exchange executor 会运行 left/right 两侧 source exchange。
  - `CurrentShardExchangeExecutorRejectsRemoteShard` 验证当前 shard Exchange executor 不会假装支持远端 source shard 触发。
  - `CurrentShardExchangeExecutorFactoryValidatesInputs` 验证当前 shard Exchange executor 工厂会拒绝缺失的 `context` / `cluster`。
  - `SystemQueryExchangeExecutorNeedsDistributedStorages` 验证基于内部 `SYSTEM` SQL 的 Exchange executor 不会在缺少左右 `StorageDistributed` source 信息时下发无效请求。
  - `CoordinatorPreparesAndCleansMemoryTables` 验证 coordinator 会对每个 shard 发送 `CREATE TABLE` 和 `DROP TABLE`。
  - `CoordinatorCleansMemoryTablesAfterPrepareFailure` 验证 prepare 中途失败时会清理已创建的 shuffle 表。

当前已实现能力：

1. 功能开关和资格分析

   - 已有实验 setting `distributed_shuffle_join`，默认关闭。
   - 已有 `DistributedShuffleJoinAnalyzer`，用于判断 MVP 查询是否能走 `shuffle join`：目前目标是 `enable_analyzer = 1`、左右都是直接 `StorageDistributed`、同 cluster、简单等值 `INNER ALL JOIN`。
   - analyzer 已经提取 left/right 直接 key 列名；`ON` 形态目前收窄为左右两侧直接列等值条件，复杂表达式 key 仍不走 MVP 路径。
   - analyzer 目前只做资格判断和信息提取，不会自动改写 query，也不会启动执行。

2. 分桶 selector

   - 已有 `DistributedShuffleJoinSelector`。
   - `createDistributedShuffleJoinSelector` 可以基于 `ClusterPtr` 和直接 key 列名创建 selector。
   - `createDistributedShuffleJoinSelector` 也可以基于 `DistributedShuffleJoinInfo` 和 left/right side 创建 selector，后续 Exchange pipeline 可以直接使用 analyzer 输出。
   - selector 复用 `createBlockSelector` 和 cluster 的 `slot_to_shard`，不是测试里手写的固定 `% 2`。
   - 当前只支持直接列 key 和整数类 key，复杂表达式、`String`、`Nullable` 等还没支持。

3. `_shuffle_*` 表名和 SQL 生成

   - 已有 `DistributedShuffleJoinTableNames` 和 `DistributedShuffleJoinExchangeId`。
   - 已能生成 query/join 唯一的普通 `Memory` 表名，例如 `_shuffle_<query_id>_<join_id>_left` 和 `_shuffle_<query_id>_<join_id>_right`。
   - 已能根据 header 生成 `CREATE TABLE IF NOT EXISTS ... ENGINE = Memory`。
   - 已能生成 `DROP TABLE IF EXISTS ...`。
   - 已能生成内部远端 Exchange 触发 SQL：`SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE '<payload>'`。

4. `Prepare` 和 `Cleanup` coordinator

   - 已有 `DistributedShuffleJoinCoordinator`。
   - `prepareShuffleTables` 会向所有 shard 发送 left/right 两张 `_shuffle_*` 表的 `CREATE TABLE`。
   - `exchangeShuffleTables` 会在 `Prepare` 后对所有 source shard 执行 Exchange executor，作为 `Exchange` 阶段的 coordinator 调度入口。
   - 如果 `Exchange` 中途发生 exception，会先 cleanup 已创建的 `_shuffle_*` 表，再继续抛出原 exception。
   - `joinShuffleTables` 会在 `Exchange` 后对所有 target shard 执行 Local `JOIN` SQL，作为 `Barrier -> Local JOIN` 阶段的 coordinator 调度入口。
   - 如果 Local `JOIN` 中途发生 exception，会先 cleanup 已创建的 `_shuffle_*` 表，再继续抛出原 exception。
   - `cleanupShuffleTables` 会向所有 shard best-effort 发送 left/right 两张 `_shuffle_*` 表的 `DROP TABLE IF EXISTS`。
   - `DistributedShuffleJoinCoordinator` 析构时会触发清理。
   - 如果 `Prepare` 中途发生 exception，会先执行 cleanup，再继续抛出原 exception。

5. shard SQL 执行器

   - 已有 `IDistributedShuffleJoinQueryExecutor` 抽象。
   - 已有 `ClusterDistributedShuffleJoinQueryExecutor` 真实实现。
   - 本地 shard 使用 `executeQuery` 执行内部 query。
   - 远端 shard 使用 `ConnectionPoolWithFailover` 获取连接，再用 `Connection::sendQuery` 发送 query，并等待 `EndOfStream` 或远端 exception。
   - `ClusterDistributedShuffleJoinQueryExecutor` 同时实现 Local `JOIN` executor 接口，Local `JOIN` 阶段可以复用同一套 shard SQL 执行逻辑。

6. source 侧 block 分桶 sink

   - 已有 `DistributedShuffleJoinSink`。
   - sink 接收输入 `Block`，按 selector 计算每行目标 target shard。
   - sink 使用 `IColumn::scatter` 将 block 拆成多个目标 shard block。
   - sink 会把拆出来的 block 交给 `DistributedShuffleJoinBlockSender` 抽象。
   - analyzer 输出现在包含 `shuffle_database`，用于决定 `_shuffle_*` 普通 `Memory` 表所在 database。
   - 已有 `DistributedShuffleJoinExecutionPlan`，可以把 analyzer 输出稳定转换为 `_shuffle_*` 表名、left/right header 和 target shard 本地 `JOIN` SQL。
   - 已有 `executeDistributedShuffleJoinStages`，可以把 execution plan、query executor、Exchange executor 和 Local `JOIN` executor 串成 `Prepare -> Exchange -> Local JOIN -> Cleanup` 阶段调用。
   - 已有 `prepareDistributedShuffleJoinExchange`，可以把 `Prepare -> Exchange` 和结果读取生命周期分开，避免真实 `SELECT` 返回结果前过早 cleanup。
   - 已有 `holdDistributedShuffleJoinCoordinator` 和 `attachDistributedShuffleJoinCoordinator`，可以把 cleanup 生命周期挂到 `QueryPlanResourceHolder` 或直接 attach 到结果 `QueryPipeline`。
   - 已有 `executeDistributedShuffleJoinLocalJoinPipeline`，可以把当前节点本地 `_shuffle_*` 表的 Local `JOIN` SQL 变成带 cleanup 生命周期的 result `BlockIO`。
   - 已有 `buildDistributedShuffleJoinClusterLocalJoinQueryPlan`，可以用 `ClusterProxy::executeQuery` 为所有 target shard 上的 `_shuffle_*` 本地 `JOIN` 构造分布式读取 `QueryPlan`。
   - 已有 `executeDistributedShuffleJoinClusterLocalJoinPipeline`，可以把所有 target shard 的 `_shuffle_*` 本地 `JOIN` 结果汇总成 result `BlockIO`，并把 coordinator cleanup 生命周期挂到结果 pipeline 上。
   - 已有 `DistributedShuffleJoinExchangePipeline` helper，可以根据 `DistributedShuffleJoinInfo` 为 left/right side 生成 source query、创建带正确 header/selector/table names 的 sink，也可以把内部 source query 的输出接到这个 sink 执行。
   - 已有远端 source Exchange JSON payload helpers，可以序列化/反序列化 cluster、source 表、`_shuffle_*` 表名、key 和 columns，并从 payload 还原执行 Exchange 所需的 `DistributedShuffleJoinInfo`。
   - 已有 source shard 本地 Exchange 封装：顺序执行 left/right 两侧，任一侧失败会 cancel sender，避免留下未收口的写入 pipeline。
   - 已有 `CurrentShardDistributedShuffleJoinExchangeExecutor`，能把 coordinator 的 Exchange executor 接口连接到当前 shard 的 source-local Exchange 执行；真实工厂会创建 `ClusterDistributedShuffleJoinBlockSender` 并接上本地 side executor。
   - 已有远端 source Exchange 内部 `SYSTEM` 命令的 parser/formatter/interpreter 入口；interpreter 会解析 payload，并在收到请求的 shard 上执行 left/right source Exchange。
   - 已有 `SystemQueryDistributedShuffleJoinExchangeExecutor`，能把 coordinator 的 Exchange executor 调用转换为内部 `SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE ...` SQL 并通过已有 shard SQL executor 下发。
   - coordinator 还没有在真实用户 `SELECT` 路径中选择并调用这个 system-query Exchange executor。
   - cluster Local `JOIN` 结果 pipeline 还没有接到真实用户 `SELECT` 路径。
   - gtest 里仍保留 capturing fake，用来做纯分桶断言。

7. 真实 table sender

   - 已有 `ClusterDistributedShuffleJoinBlockSender`。
   - 本地 target shard 走 `InterpreterInsertQuery` + `PushingPipelineExecutor`，把 block 写入本机 `_shuffle_*` `Memory` 表。
   - 远端 target shard 走 `ConnectionPoolWithFailover` + `RemoteSink`，把 block 通过 `INSERT INTO _shuffle_*` 推到目标 shard。
   - sender 以 `(target shard, side)` 为单位懒初始化 pipeline，并在 `finish` 时结束对应写入会话。
   - 这一步已经补上“拆桶之后如何把 bucket block 真正发往目标 `_shuffle_*` 表”的独立组件，但它还没有被真实 Exchange 查询路径实例化并串起来。

8. 单测覆盖

   - 已有 `DistributedShuffleJoin.*` gtest。
   - 当前覆盖 selector 分桶、从 analyzer info 创建 selector、Exchange side sink 创建、Exchange source SQL 生成、缺失 source storage 保护、远端 Exchange payload 序列化/反序列化、执行计划生成、完整阶段顺序执行、`Prepare -> Exchange` 生命周期拆分、coordinator resource 析构 cleanup、coordinator resource attach 到 `QueryPipeline` 后 cleanup、Local `JOIN` pipeline 失败 cleanup、cluster Local `JOIN` pipeline 失败 cleanup、source shard left/right 执行顺序、source shard Exchange 失败 cancel、当前 shard Exchange executor、system-query Exchange executor 保护、当前 shard Exchange executor 工厂校验、远端 Exchange 内部 `SYSTEM` 命令生成和解析、`_shuffle_*` 表名和 SQL 生成、Local `JOIN` SQL 生成、required columns 到 header 的转换、coordinator prepare/exchange/local join/cleanup、prepare 失败清理、Exchange 失败清理、Local `JOIN` 失败清理。
   - 最近一次运行 `build/src/unit_tests_dbms '--gtest_filter=DistributedShuffleJoin.*'` 通过，32 个测试全部成功。

当前已实现能力对应的真实 SQL 例子：

假设用户最终想执行的 SQL 是：

```sql
SELECT a.id, a.v, b.name
FROM a_dist AS a
INNER ALL JOIN b_dist AS b ON a.id = b.id
SETTINGS distributed_shuffle_join = 1
```

再假设有 2 个 shard，当前 query id 是 `q123`，这是该 query 中第 0 个 `shuffle join`。当前代码已经能覆盖下面这些局部步骤，但这些步骤还没有被真实 `SELECT` 自动串起来。

1. 判断这个 SQL 是否能走 MVP `shuffle join`

   `DistributedShuffleJoinAnalyzer` 的目标是识别：

   - 左边是 `StorageDistributed` 表 `a_dist`。
   - 右边是 `StorageDistributed` 表 `b_dist`。
   - `JOIN` 是 `INNER ALL JOIN`。
   - 条件是简单等值条件，例如 `a.id = b.id`。
   - 左右表属于同一个 cluster。

   目前 analyzer 只做资格判断和信息提取；真实执行一条 SQL 时，还不会自动调用后续 shuffle 流程。

2. 为这次 `JOIN` 生成 shuffle 表名

   `DistributedShuffleJoinTables` 会根据 query/join 标识生成普通 `Memory` 表名：

   ```text
   initial_query_id = q123
   join_id = 0
   database = default
   ```

   对应表名类似：

   ```sql
   default._shuffle_q123_0_left
   default._shuffle_q123_0_right
   ```

   左表 `a_dist` 参与 shuffle 的数据后续写入 `_left`，右表 `b_dist` 参与 shuffle 的数据后续写入 `_right`。

3. 生成每个 shard 上要执行的建表 SQL

   如果左侧需要列是 `id UInt64, v String`，右侧需要列是 `id UInt64, name String`，当前 helper 能生成类似：

   ```sql
   CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_left
   (
       id UInt64,
       v String
   )
   ENGINE = Memory
   ```

   以及：

   ```sql
   CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_right
   (
       id UInt64,
       name String
   )
   ENGINE = Memory
   ```

4. coordinator 向所有 shard 发送 `CREATE TABLE`

   `DistributedShuffleJoinCoordinator` 加 `ClusterDistributedShuffleJoinQueryExecutor` 当前能把建表 SQL 发到所有 shard：

   ```text
   shard0:
     CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_left ...
     CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_right ...

   shard1:
     CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_left ...
     CREATE TABLE IF NOT EXISTS default._shuffle_q123_0_right ...
   ```

   本地 shard 使用 `executeQuery`，远端 shard 使用 `Connection::sendQuery`。

5. source 侧按 `JOIN` key 给 block 分桶

   `DistributedShuffleJoinSelector` 加 `DistributedShuffleJoinSink` 当前能在拿到一个 `Block` 后按 key 拆成多个 target shard block。

   例如 source shard 读到左表本地 block：

   ```text
   a_local:
   id | v
   1  | a1
   2  | a2
   3  | a3
   ```

   2 个 target shard 下，如果 selector 算出：

   ```text
   id = 1 -> shard1
   id = 2 -> shard0
   id = 3 -> shard1
   ```

   sink 会拆成：

   ```text
   target shard0 left block:
   id | v
   2  | a2

   target shard1 left block:
   id | v
   1  | a1
   3  | a3
   ```

   右表也会按同一个 key 规则拆桶。比如右表本地 block：

   ```text
   b_local:
   id | name
   1  | n1
   2  | n2
   4  | n4
   ```

   可能拆成：

   ```text
   target shard0 right block:
   id | name
   2  | n2
   4  | n4

   target shard1 right block:
   id | name
   1  | n1
   ```

6. query 结束或异常时清理 shuffle 表

   `cleanupShuffleTables` 会向所有 shard 发送：

   ```sql
   DROP TABLE IF EXISTS default._shuffle_q123_0_left
   DROP TABLE IF EXISTS default._shuffle_q123_0_right
   ```

   如果 `Prepare` 阶段中途发生 exception，也会尽力清理已经创建过的 `_shuffle_*` 表。

7. 生成 target shard 上的本地 `JOIN` SQL

   Exchange barrier 之后，当前 helper 已经能生成每个 target shard 上读取 `_shuffle_*` 表的本地 `JOIN` SQL：

   ```sql
   SELECT *
   FROM default._shuffle_q123_0_left AS _shuffle_left
   INNER ALL JOIN default._shuffle_q123_0_right AS _shuffle_right
   ON _shuffle_left.id = _shuffle_right.id
   ```

   `DistributedShuffleJoinCoordinator::joinShuffleTables` 已经能在 `Exchange` 完成后把这条 SQL 下发到所有 shard。当前它仍是阶段骨架：还没有把结果 pipeline 接回 initiator 的真实 `SELECT` 输出。

当前这个例子里还没实现的是中间最关键的完整执行链：

```text
读 a_local / b_local
  -> 调用 DistributedShuffleJoinSink 分桶
  -> 把 shard0 bucket 写入 shard0 的 _shuffle_* 表
  -> 把 shard1 bucket 通过 RemoteSink / INSERT 写入 shard1 的 _shuffle_* 表
  -> 等所有 source shard 完成
  -> 在每个 shard 上执行本地 JOIN
  -> initiator 汇总结果
```

所以当前实现可以概括为：已经有“能判断、能生成执行计划、能建表、能清理、能把 block 拆桶、也已经有真实 table sender”的组件；source shard 本地执行入口、远端 `SYSTEM` payload 执行入口、coordinator 可用的 system-query Exchange executor，以及 `Prepare -> Exchange -> Local JOIN -> Cleanup` 阶段编排 helper 也已经有了。`Prepare -> Exchange` 已经能和后续结果读取生命周期拆开，coordinator 也可以 attach 到 `QueryPipeline` 生命周期上；当前节点本地 `_shuffle_*` Local `JOIN` 也已经能产出带 cleanup 生命周期的 `BlockIO`。还没有完成的是“把这个阶段编排接进真实用户 `SELECT` 路径，以及把所有 target shard 的 Local `JOIN` 结果汇总回 initiator”。

当前主线判断：

- 第一版 end-to-end 优先走 `Memory` 表方案：`Prepare` 建普通表，`Exchange` 用 source 端分桶后 `INSERT` 到目标表，`Barrier` 等所有 Exchange 返回，`Local JOIN` 查询 `_shuffle_*` 表，最后 `DROP TABLE` 清理。
- 不要再恢复或继续实现旧的内部 registry/receiver 路线；后续围绕 `ShuffleExchangeCoordinator`、`RemoteSink` / `INSERT` sender 和 `_shuffle_*` 表推进。

当前已经验证过的编译目标：

- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinAnalyzer.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Core/Settings.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinBlockSender.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinExchangePipeline.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Interpreters/DistributedShuffleJoinCoordinator.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Interpreters/InterpreterSystemQuery.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinSink.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinSelector.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinTables.cpp.o`
- `ninja -C build src/CMakeFiles/unit_tests_dbms.dir/Storages/tests/gtest_distributed_shuffle_join.cpp.o`
- `ninja -C build unit_tests_dbms`

当前已经运行过的测试：

- `build/src/unit_tests_dbms '--gtest_filter=DistributedShuffleJoin.*'`

当前代码还没有完成的部分：

- `DistributedShuffleJoinSink` 的 selector 目前仍由外部传入，但已经有 `DistributedShuffleJoinExchangePipeline` helper 可以根据 `DistributedShuffleJoinAnalyzer` 输出的 left/right key column、required columns 和 side 构造对应 sink。复杂表达式 key 尚未接入。
- 已有 `ClusterDistributedShuffleJoinBlockSender`、当前 shard Exchange executor、远端 `SYSTEM` payload 执行入口、system-query Exchange executor、完整阶段编排 helper、可延后 cleanup 的 `Prepare -> Exchange` helper，以及 pipeline resource holder；当前 shard 进程内已经能把 source side executor 接到 coordinator Exchange executor 接口，收到远端 `SYSTEM DISTRIBUTED SHUFFLE JOIN EXCHANGE ...` 的 shard 也能解析 payload 并执行本地 left/right source Exchange。coordinator 还没有接入真实用户 `SELECT` 路径来自动调用它。
- coordinator 目前完成 `Prepare`、`Exchange` 调度入口、`Barrier -> Local JOIN` 调度入口和 `Cleanup`，并已有基于 `ConnectionPoolWithFailover` 的真实 query executor；但它还没有接入真实 distributed query path，也还没有把所有 target shard 的 Local `JOIN` 结果 pipeline 汇总回 initiator。
- 还没有把 `_shuffle_*` 普通 `Memory` 表写入、查询、清理接入真实 distributed query path。
- 尚未接入 `StorageDistributed::read`、`ClusterProxy::executeQuery` 或更合适的 planner 入口。
- 尚未添加 integration tests。

下一步建议：

1. 找到真实用户 `SELECT` 的接入点：在 analyzer 判断满足 MVP 后，调用 `createDistributedShuffleJoinExecutionPlan`，构造 `ClusterDistributedShuffleJoinQueryExecutor` 和 `SystemQueryDistributedShuffleJoinExchangeExecutor`，再调用 `prepareDistributedShuffleJoinExchange`。
2. 把所有 target shard 上读取 `_shuffle_*` 表的 Local `JOIN` 结果 pipeline 汇总回 initiator；当前只有当前节点本地 `JOIN` pipeline helper。

### 未来产品化方向

MVP 完成后，再逐步做产品化能力：

- 支持 spill 到磁盘。`StorageMemory` 只能用于 MVP，大数据场景必须有内存上限和落盘策略，可以参考 `GraceHashJoin` 的 bucket 文件思路或 ClickHouse 现有 temporary data 组件。
- 增加网络 backpressure、限流、压缩、block squashing，避免 all-to-all shuffle 在大集群中打满连接池和网络。
- 支持失败传播和取消清理，包括 source 失败、target 失败、initiator 取消、超时、连接断开。
- 支持 parallel replicas 和 replica 选择策略。
- 支持更多 `JOIN` 类型、strictness、多 key、表达式 key、nullable key。
- 增加 skew 观测和保护，例如每 bucket 行数、每 bucket 字节数、最大 bucket 限制和 fallback。
- 增加 profile events 和 query log 字段，用于观察 shuffle rows、shuffle bytes、send time、receive time、barrier wait time、spill bytes。
- 增加 cost model，在 `GLOBAL JOIN`、普通 distributed `JOIN`、`shuffle join` 之间自动选择。右表很小或过滤很强时，broadcast 可能比 shuffle 更合适。

### 开发注意事项

- 不要把当前 MVP 做成 SQL rewrite。真正目标是 block-level exchange。
- 不要在第一版承诺大表稳定运行。MVP 可以在内存超限时报错。
- 不要在 C++ 代码中用 sleep 修 race condition。
- 所有文档、注释、commit message 中提到 ClickHouse SQL 名称、类名、函数名、日志原文时，用 inline code 包住，例如 `StorageDistributed`、`GLOBAL JOIN`、`createSelector`。
- 新增 C++ 代码保持 Allman-style braces。
