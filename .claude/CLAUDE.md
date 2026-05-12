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

### 背景结论

当前已弃用早期 `shuffle` 分支里的 SQL 改写方案。那个方案把原始 `JOIN` 改写成每个 shard 一条带 bucket 过滤的 `Distributed` 子查询，例如左右两边都加 `cityHash64(join_key) % shard_count = bucket`。这种方式只能从形式上得到按 bucket 执行的结果，但本质上仍然是嵌套分布式读，会导致每个 bucket 重复扫描源表，不是真正的数据重分布。

新的 `exchange` 分支应从 `master` 重新开发，不复用早期 `buildQueryTreeDistributedForShuffle` 这类 SQL rewrite 代码。可以参考旧分支中的测试思想，但不要把旧分支作为实现基础。

`GLOBAL JOIN` 的代码分析给我们的主要启发是：

- `GLOBAL JOIN` 会先在发起节点执行右侧子查询，把结果写入 query context 中的临时表，例如 `_data_<hash>`。
- 这个临时表通过 external table 机制随远端 query 一起发送到各 shard。
- 远端 query 在执行前通过 `TCPHandler` 初始化 external tables，然后在本地 `JOIN` 中读取 `StorageMemory`。
- `GLOBAL JOIN` 的临时表生命周期天然绑定到单个远端 query context，query 结束后释放。

真正的 `shuffle join` 和 `GLOBAL JOIN` 的区别是：`GLOBAL JOIN` 是 initiator 单生产者广播一份右表临时表；`shuffle join` 是所有 source shard 同时作为生产者，把左右两侧数据按 `JOIN` key 拆分后发送到不同 target shard。target shard 上的临时表会被多个 source shard 追加写入，并且 final `JOIN` 必须等待所有 source shard 发送完成。因此 `shuffle join` 需要显式的 `exchange` 生命周期、barrier、异常传播和清理逻辑。

### MVP 支持范围

第一版只做一个窄范围 MVP，用来证明执行模型是真正的 `shuffle exchange`，不要一开始追求完整产品化。

MVP 暂定只支持：

- `enable_analyzer = 1`。
- `INNER ALL JOIN`。
- 单个等值 `JOIN` key，例如 `USING (id)` 或简单 `ON a.id = b.id`。
- 左右两边都是直接的 `StorageDistributed` 表。
- 左右两边属于同一个 `cluster`。
- 每个 shard 只使用一个 replica，暂时不支持 parallel replicas。
- 临时数据先使用内存中的 `StorageMemory`，数据超限可以先报错。
- 暂时不支持 spill、复杂子查询、多 key、多种 `JOIN` strictness、`LEFT` / `RIGHT` / `FULL` / `ASOF` / `SEMI` / `ANTI` 等复杂语义。

MVP 的正确执行形态应该是：

```text
source shard:
  read local left table once
  read local right table once
  split each block by JOIN key
  send each bucket to corresponding target shard

target shard:
  receive left buckets from all source shards
  receive right buckets from all source shards
  wait until both sides are complete
  run local JOIN over two temporary tables
  return result to initiator

initiator:
  coordinate exchange
  collect target shard JOIN results
  union final result
```

第一版必须能证明每个 source shard 对每侧输入只扫描一次，不能退化成按 bucket 重复扫描。

### 当前代码改动步骤

建议按下面顺序推进，避免过早把复杂逻辑塞进 `StorageDistributed::read`。

1. 增加实验 setting

   新增 `distributed_shuffle_join`，默认关闭。必要时增加内部 setting，例如 `distributed_shuffle_join_internal`，用于防止 worker 上递归触发 shuffle rewrite。setting 文档要明确这是实验功能。

2. 增加 eligibility analyzer

   新增独立 helper，负责判断一条 query 是否可以进入 MVP `shuffle join` 路径。它应只做资格判断和提取信息，不做执行：

   - 左右 table expression。
   - 左右 `StorageDistributed`。
   - `JOIN` kind、strictness、locality。
   - 左右 `JOIN` key 表达式。
   - cluster 名称和 shard 数。
   - 需要读取的左表列和右表列。

3. 设计 `ShuffleExchange` registry

   增加 query-scoped 或 server-scoped 的 exchange registry，用 `initial_query_id` 加 `join_id` 形成 `exchange_id`。target shard 上至少需要保存：

   - `exchange_id`。
   - left temporary table。
   - right temporary table。
   - expected source shard count。
   - each side 的 finished producer count。
   - cancellation flag。
   - first exception。

   这个 registry 是 MVP 的核心。不要只依赖普通 external table 生命周期，因为 external table 更适合 `GLOBAL JOIN` 的单连接、query 前置传输模型。

4. 实现 receiver 侧临时表写入

   target shard 收到 `exchange_id`、side、block 后，应找到对应 `ShuffleExchange`，把 block 追加写入 left 或 right 临时表。MVP 可以先用 `TemporaryTableHolder` 和 `StorageMemory`，但要明确并发写入是否安全；如果不安全，receiver 侧需要串行化写入。

5. 实现 source 侧 `ShuffleExchangeSink`

   source shard 读取本地表产生 block 后，由 `ShuffleExchangeSink` 按 `JOIN` key 计算目标 shard selector 并拆分 block。分片逻辑应参考或复用 `DistributedSink::createSelector`、`DistributedSink::splitBlock` 和 `StorageDistributed::createSelector`，不要手写固定的 `cityHash64(key) % shard_count` 作为最终方案。

6. 增加 exchange 网络传输路径

   需要明确 source shard 如何向 target shard 发送 shuffle block、finish、exception。MVP 可以先使用内部连接上的专用命令或较小范围的协议扩展，但不要把它伪装成普通 SQL rewrite。最终代码应让数据流是 block 级别的 exchange，而不是 SQL 层重复查询。

7. 增加 barrier

   target shard 的 final `JOIN` 必须等待所有 source shard 对 left side 和 right side 都发送 finished。任何 source 抛异常或 query 被取消时，所有 target shard 都应停止等待并清理临时数据。

8. 接入 distributed query plan

   当 eligibility analyzer 通过时，`StorageDistributed::read` 或相邻的 planner 入口应构建特殊的 shuffle execution path：

   - initiator 创建 `exchange_id`。
   - 在所有 source shard 启动 left producer 和 right producer。
   - 在所有 target shard 启动 final local `JOIN` query。
   - initiator union 所有 target shard 的结果。

   这里应尽量把实现拆到独立类，例如 `DistributedShuffleJoinAnalyzer`、`ShuffleExchangeCoordinator`、`ShuffleExchangeSink`，不要把所有逻辑直接写进 `StorageDistributed::read` 或 `ClusterProxy::executeQuery`。

9. 增加测试

   先写 integration tests 覆盖：

   - 左右表都故意不按 `JOIN` key 落位，结果仍然完整正确。
   - 每个 source shard 每侧输入只扫描一次。
   - target shard 能看到来自多个 source shard 的同一 side 数据。
   - query 取消、异常、source shard 失败时，exchange 临时表能清理。
   - `distributed_shuffle_join = 0` 时不改变现有执行路径。

   跑 integration tests 时输出必须重定向到 build 目录日志文件，并让子 agent 分析日志摘要。

### 当前实现进度

截至 `2026-05-12`，`exchange` 分支相对 `master` 的 `shuffle join` 相关进度如下。这个小节用于协作交接；后续每完成一个独立阶段，都需要同步更新这里。

已提交到当前分支的改动：

- `.claude/CLAUDE.md` / `AGENTS.md`：增加当前 `shuffle join` MVP 的中文设计记录、开发步骤和注意事项。
- `.gitignore`：增加本地 `Codex` 配置忽略项，避免把个人 `.codex` 配置提交进仓库。
- `src/Core/Settings.cpp`：新增实验 setting `distributed_shuffle_join`，默认关闭。当前只作为功能入口开关，尚未接入执行路径。
- `src/Storages/DistributedShuffleJoinAnalyzer.h` 和 `src/Storages/DistributedShuffleJoinAnalyzer.cpp`：新增 `DistributedShuffleJoinAnalyzer` helper。它只负责判断一条 query 是否满足 MVP `shuffle join` 条件，并提取左右 `StorageDistributed`、`JOIN` key、cluster、shard 数、所需列等信息；它不负责改写 SQL，也不负责执行。

当前 `exchange` 分支相对 `master` 已提交的新增文件和作用：

- 最新提交 `dc1c853f5e5 Add distributed shuffle join selector builder` 已把 selector builder 和对应测试收进分支。

- `src/Storages/DistributedShuffleJoinExchange.h` 和 `src/Storages/DistributedShuffleJoinExchange.cpp`：新增 receiver-side 的 `exchange` 状态与内存数据容器。
  - `DistributedShuffleJoinExchangeId` 用 `initial_query_id + join_id` 标识一次 `shuffle join`。
  - `DistributedShuffleJoinExchange` 维护单个 target shard 上某次 `exchange` 的 left/right side blocks、header、rows/bytes 统计、每个 source shard 的 finished 状态、`waitReady` barrier、`cancel` 和 `clearData`。
  - `DistributedShuffleJoinExchangeRegistry` 按 `exchange_id` 管理本节点上的 exchange 对象。
  - `DistributedShuffleJoinExchangeReceiver` 是本地 receiver facade，提供 `prepareExchange`、`receiveBlock`、`finishSource`、`cancelExchange`，供后续网络 receive 入口复用。
- `src/Storages/DistributedShuffleJoinSink.h` 和 `src/Storages/DistributedShuffleJoinSink.cpp`：新增 source-side 发送 sink 骨架。
  - `DistributedShuffleJoinBlockSender` 是发送抽象，定义 `sendBlock`、`finish`、`cancel`。
  - `LocalDistributedShuffleJoinBlockSender` 是本地 sender，用于先在单进程内打通 `sink -> receiver -> exchange`，不经过网络。
  - `DistributedShuffleJoinSink` 继承 `SinkToStorage`，接收 input chunk 后转成 `Block`，通过传入的 selector 计算每行目标 target shard，用 `IColumn::scatter` 分块，然后调用 sender 发送；`onFinish` 会向所有 target shard 发送 finish，`onCancel` 会传播 cancel。
- `src/Storages/DistributedShuffleJoinSelector.h` 和 `src/Storages/DistributedShuffleJoinSelector.cpp`：新增 MVP 版 `JOIN` key selector builder。
  - `createDistributedShuffleJoinSelector` 接收 `ClusterPtr` 和 key column name，返回 `DistributedShuffleJoinSelector`。
  - 当前只支持 key 已经是 block 中的直接列，暂不支持复杂表达式 key。
  - 内部复用 `createBlockSelector` 和 cluster 的 `slot_to_shard`，因此遵循 `Distributed` sharding 的 slot/weight 规则。
  - 当前类型范围与 `StorageDistributed` 的整数 sharding key 路径对齐，支持整数类型和整数字典的 `LowCardinality`，暂不支持 `String`、`Nullable` 等 key。
- `src/Storages/tests/gtest_distributed_shuffle_join.cpp`：新增本地闭环 gtest。该测试构造两个 source shard 和两个 target shard，用测试 selector `id % 2` 驱动 `DistributedShuffleJoinSink -> LocalDistributedShuffleJoinBlockSender -> DistributedShuffleJoinExchangeReceiver -> DistributedShuffleJoinExchange`，验证：
  - left/right 两侧都能按 target shard 正确分块。
  - 多个 source shard 能写入同一个 target exchange。
  - 每个 target exchange 在所有 source 对 left/right 都发送 finish 后变为 ready。
  - target exchange 中的 blocks、rows 统计符合预期。
  - 当前测试已改为通过 `createDistributedShuffleJoinSelector` 构造 selector，不再直接在测试中硬编码 `% 2` 作为 sink 的 selector。
  - 新增 `SelectorUsesJoinKeyColumn` 测试，验证 left/right block 只要 key value 相同，就会被 selector 分配到同一个 target shard。

上述文件目前是已提交状态，不是未提交草稿。后续开发如果新增一个阶段性能力，例如 remote sender/receiver、final local `JOIN` source 或 planner 接入，需要继续在本节追加新的阶段进度。

当前已经验证过的编译目标：

- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinAnalyzer.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Core/Settings.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinExchange.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinSink.cpp.o`
- `ninja -C build src/CMakeFiles/dbms.dir/Storages/DistributedShuffleJoinSelector.cpp.o`
- `ninja -C build src/CMakeFiles/unit_tests_dbms.dir/Storages/tests/gtest_distributed_shuffle_join.cpp.o`
- `ninja -C build unit_tests_dbms`

当前已经运行过的测试：

- `build/src/unit_tests_dbms --gtest_filter=DistributedShuffleJoin.LocalSinkReceiverExchangeRoundTrip`
- `build/src/unit_tests_dbms '--gtest_filter=DistributedShuffleJoin.*'`

当前代码还没有完成的部分：

- `DistributedShuffleJoinExchangeRegistry` 还没有挂到 server-level 或 query-accessible 的 `Context` 中。
- `DistributedShuffleJoinSink` 的 selector 目前仍由外部传入，但已经有 MVP 版 `createDistributedShuffleJoinSelector` 可根据直接 key 列构造 selector。复杂表达式 key 尚未接入。
- 目前只有 `LocalDistributedShuffleJoinBlockSender`，还没有真正的 remote sender / receiver 网络传输路径。
- exchange 中的临时数据当前是内存中的 `std::vector<Block>`，还没有包装成 final local `JOIN` 可直接读取的 `Source` 或 `StorageMemory`。
- 尚未接入 `StorageDistributed::read`、`ClusterProxy::executeQuery` 或更合适的 planner 入口。
- 尚未添加 integration tests。

下一步建议：

1. 设计 remote sender / receiver 网络路径，让 source shard 能把 `sendBlock`、`finish`、`cancel` 发送到目标 target shard。
2. 设计 final local `JOIN` 如何读取 exchange blocks，可以先评估包装成 `SourceFromChunks`，再决定是否切换到 `StorageMemory`。
3. 把 `DistributedShuffleJoinAnalyzer` 提取到的 left/right key column 接到 `createDistributedShuffleJoinSelector`，让左右 sink 使用各自的 key 列名构造 selector。
4. 最后再接 distributed query plan，不要过早把未稳定的生命周期和网络逻辑塞进 `StorageDistributed::read`。

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
