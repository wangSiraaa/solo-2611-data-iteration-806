# Parquet footer 统计驱动的保守 plan/scan

用 Python 3.11 + PyArrow 19 实现的小型 Parquet 查询工具：计划阶段只读 footer 统计、
保守剪枝行组；执行阶段先读过滤列做 SQL 三值逻辑精确过滤，再只为命中行读投影列。
不解析 SQL、不做损坏恢复、不使用 page index、不含远程 Range 服务与性能压测。

## 目录

```
fixtures/                 输入：files.json、queries.json、stats.parquet、no-stats.parquet
  occupancy/{main,shifted,transitivity}/  占位冲突与交付示例（事件 + 规则 + values.json）
tools/make_fixtures.py    从同一份数据生成“有统计/无统计”两个 Parquet（行组与行序一致）
tools/make_occupancy_fixtures.py  生成占位冲突/交付示例夹具
src/pqscan/
  expr.py       谓词树解析、schema 校验、PyArrow 表达式翻译、纯 Python 3VL 逐行求值
  stats.py      只读 footer 的行组统计提取与可靠性判定
  planner.py    三值逻辑保守行组推理（can_be_true / can_be_false、trace）
  scanner.py    两阶段列读取、精确行选择、读取请求在执行入口即时登记
  reference.py  独立核对：无剪枝全扫描 + 纯 Python 3VL（与引擎不同代码路径）
  runner.py     串联计划/扫描/核对，落盘 plans、results、run_record、verification
  occupancy.py  业务入口一：按 (工单号, 配对号) 重建占位区间 + 独立冲突清单
  delivery.py   业务入口二：基于冲突清单求最大价值可同时交付集合（精确、平局定序）
  cli.py        命令行入口（run / verify / occupancy / deliver）
tests/          pytest 套件（35 个用例）
docs/plan_format.md  计划与运行记录的 JSON 格式契约
output/         运行产物（git 忽略）
```

## 运行

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
.venv/bin/python tools/make_fixtures.py           # 重新生成两个夹具（可选，仓库已带）
PYTHONPATH=src .venv/bin/python -m pqscan.cli run \
    --files fixtures/files.json --queries fixtures/queries.json --out output
PYTHONPATH=src .venv/bin/python -m pytest tests/ -q
```

输出：`output/plans/*.json`（行组选择说明）、`output/results/*.parquet`（查询结果）、
`output/run_record.json`（实际列读取请求）、`output/verification.json`（独立核对结论）。

## 支持范围
- 平面 schema；整数（有/无符号 int32/int64 等）与字符串列；
- 比较 `eq/ne/lt/le/gt/ge`、`is_null`、`and/or/not`、列投影；
- 谓词以 JSON 前缀树表达（见 `fixtures/queries.json`），不解析 SQL；
- 类型不匹配、列不存在、嵌套 schema 等在计划期报错；不处理文件损坏。

## 关键设计约束（与需求逐条对应）

1. **计划只读 footer。** `stats.py` 只访问 `FileMetaData/RowGroupMetaData/Statistics`，
   不调用任何数据读取接口。
2. **三值逻辑双结论。** 每个谓词子树求 `can_be_true`/`can_be_false`，
   NULL 产生的 UNKNOWN 既不算 TRUE 也不算 FALSE。`NOT` 同时翻转两个结论，
   不会出现“把 UNKNOWN 当 FALSE 再取反”的错误剪枝（见 `planner.py`）。
3. **统计缺失/不可靠即保守保留。** 无 Statistics、`has_min_max=False`（全 NULL 组的
   典型形态）、`null_count` 缺失、物理类型不受支持，叶子结论退化为
   `(true, true)` 并在 trace 的 `unknown` 中登记原因。PyArrow 19 的 Statistics
   不暴露 min/max 截断标记，因此对存在且类型受支持的 min/max 视为精确；
   若环境提供截断信息，应在 `stats.py` 将其降级为不可靠。
4. **AND/OR 不跨列关联极值。** 只组合独立子树的两个结论，不假设不同列的
   min/max 来自同一行。只有证明 `can_be_true=false` 才 `skip`。
5. **两阶段扫描。** 保留组先读过滤列；`Table.filter` 的 Kleene 语义丢弃
   UNKNOWN/FALSE、只留 TRUE，再用临时行号列取回组内物理行号。无命中组到此结束，
   不读 payload 等投影专用列；有命中组才读剩余投影列。过滤列同时在投影中时复用
   第一阶段数组，不会发起第二次读取（空列集合不产生请求）。
6. **读取计数来自执行入口。** `InstrumentedParquetFile.read_row_group` 在真实
   调用发生时登记 `(文件, 行组, phase, 列集合)`，计划器不参与填写；记录的是工具
   发起的列读取请求，不是操作系统字节数，也不用 footer 的压缩大小冒充 I/O。
7. **结果与溯源。** 输出按 files.json 文件顺序、行组物理索引、组内行号排列；
   附 `_source_file/_source_row_group/_source_row_in_group` 三列；重复数据保留；
   空结果写出同 schema 的零行 Parquet。
8. **独立核对。** `reference.py` 不看任何统计、每个行组全列全量读取，用纯 Python
   3VL 逐行求值，逐行比对引擎结果的内容、顺序与溯源。

## 夹具与现成查询的预期现象

两个 Parquet 由同一份 20 行数据写成，各 5 个行组、每组 4 行；RG2 的 `k` 全 NULL。

| 查询 | stats.parquet 计划 | no-stats.parquet 计划 |
| --- | --- | --- |
| `range`（k∈[14,30)） | 跳过 RG0/RG1/RG4；RG2 全 NULL 保守保留但过滤后无命中；仅 RG3 命中 | 5 组全保留，结果完全一致 |
| `nulls`（k IS NULL） | null_count=0 的 RG1/RG3/RG4 证明跳过；RG0/RG2 命中 | 全保留，结果一致 |
| `negated`（NOT kind='A'） | 全部保留（含 NULL 行，UNKNOWN 不取反为 TRUE） | 全保留，结果一致 |
| `or_missing_stats`（kind='A' OR amount>160） | 全部保留（OR 跨列独立推理） | 全保留，结果一致 |
| `no_hits`（k>1000） | RG0/RG1/RG3/RG4 证明跳过；RG2 统计缺失保守保留、过滤后无命中 | 全保留、全部无命中 |

可在 `output/run_record.json` 验证：`no_hits` 与 `range` 中所有
`no_hits_after_filter` 行组只有 `phase=filter` 的请求，从不读取投影专用列
（`row_id/payload/kind/amount`）；`planned_skip` 行组零请求。

## 业务入口：占位冲突与可同时交付工单

在上面通用 plan/scan 之外，新增两个可分别使用的**离线业务入口**，互不依赖对方的
实现细节，只通过一份冲突清单交接。原 `run`/`verify` 接口与其物理行溯源完全保留。

事件与规则的 Parquet schema：

- 事件（一个或多个文件）：`work_order, fixture, station, pair_id, kind, ts`，
  其中 `kind ∈ {IN, OUT}`，`ts` 为整数时刻。
- 规则（一个或多个文件）：`fixture, station_a, station_b`，一行声明同台两工位互斥。

### 入口一：重建占位区间 + 冲突清单

```bash
PYTHONPATH=src .venv/bin/python -m pqscan.cli occupancy \
    --events fixtures/occupancy/main/events1.parquet fixtures/occupancy/main/events2.parquet \
    --rules fixtures/occupancy/main/rules.parquet \
    --out output/occ_main
```

- 按 `(工单号, 进退配对号)` 重建**真实占位区间**：一张工单中途退站又返回时
  是多个不连续区间，绝不从第一次进入一路占到末次离开；进入/离开可跨文件、
  跨行组、乱序，靠配对号而非位置关联。
- 每对必须恰好一个 `IN`、一个 `OUT`，且离开时刻**严格晚于**进入时刻，
  区间为半开 `[进入, 离开)`：**时间相接不算冲突**。
- 缺端、同一端内容矛盾、同端重复、进出夹具/工位不一致、时刻倒置，均单列进
  `malformed` 并带全部出处；**只要一张工单有任一配对错误，该工单整单不参加交付**。
- 冲突判定：同一夹具下，同工位（无需规则）或规则声明互斥的两工位，且区间内部
  重叠。互斥对**无向且不传递**（只声明 X-Y、Y-Z 时 X 与 Z 不互斥）。
- 产物 `occupancy.json` 含 `intervals`、`malformed`、`conflicts`、`interfering_pairs`。
  每条冲突给出工单对、作为证据的**配对号**、**实际交叠区间**、依据（同工位/规则）
  及其物理出处（文件、行组、组内行号）。

### 入口二：选择最大价值可同时交付集合

```bash
PYTHONPATH=src .venv/bin/python -m pqscan.cli deliver \
    --conflicts output/occ_main/occupancy.json \
    --values fixtures/occupancy/main/values.json \
    --out output/del_main
# 也可指定必须项：--required A
```

- 读取入口一的合格工单与冲突清单，加上每张工单的正整数价值、必须交付集合；
  返回总价值最大的可同时交付整单集合，**同价值取升序工单号列表字典序最小者**。
- 冲突是工单对级硬约束（不传递），用位掩码分支定界**精确**求解，适用最多 20 张
  合格工单，按批离线算。
- 必须项之间冲突时，整单请求被拒绝（退出码 3），写出 `rejection.json` 指出冲突
  出处；必须项不在合格工单中、价值缺失/非正同样报错。
- 价值或必须项改变时，可直接**复用同一份 `occupancy.json`** 重新报价，无需重读
  事件/规则 Parquet。

### 夹具与可复跑结果

```bash
.venv/bin/python tools/make_occupancy_fixtures.py   # 生成 fixtures/occupancy/**
```

| 场景 | 事件 | 期望交付 |
| --- | --- | --- |
| `main`：A 占 `[0,2)`+`[6,8)`，B `[1,3)`，C `[5,7)`，价值 5/3/3 | `events1/events2` | `B, C`（价值 6） |
| `main` 且 `--required A` | 同上 | `A`（价值 5） |
| `shifted`：A 第二段改为 `[7,8)`，恰好接在 C 之后 | `events` | `A, C`（价值 8） |
| `transitivity`：同台 X/Y/Z 只声明 X-Y、Y-Z | `events` | `WX, WZ`（X 与 Z 同交付） |
| `main` 且 `--required A B`（两必须项冲突） | 同上 | 拒绝，指出 A×B 交叠 `[1,2)` |

`main` 另含 M1/M2/M3 三个配对错误工单（缺端、同端矛盾、整单排除），验证
`malformed` 出处与“整单不参加交付”。

## 测试覆盖

`tests/test_pqscan.py`（12 用例）覆盖：范围剪枝、全 NULL 组、IS NULL 的
null_count 推理、NOT 的 UNKNOWN 处理与可证明 FALSE、OR 跨列不关联极值、
无统计全保留、3VL 真值表、无命中不读投影列、过滤列复用、端到端与无剪枝
全扫描一致、有/无统计结果相同而计划保守度不同、空结果 schema 与输出顺序。

`tests/test_occupancy_delivery.py`（23 用例）覆盖：多段占位不合并、跨文件/乱序配对、
缺端与同端矛盾/倒置的单列与整单排除、时间相接不冲突、同工位与无向规则、规则不传递、
题目三个数值示例、字典序平局、必须项冲突拒绝、冲突清单复用，以及与暴力枚举在随机图
和 20 张链式冲突图上的一致性。
