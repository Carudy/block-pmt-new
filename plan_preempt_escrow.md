# Preempt-Escrow 实现计划（不影响现有版本）

> 状态：设计稿 + **P0–P4 可信单节点原型已实现**（2026-10-22）。
> 已实现：独立 schema、nonce 前缀 MPTP、真实余额 escrow、pending credit、Quiesce/Freeze/Settle/Debit/Deliver、超时退款、flow gate、terminal drain、`_escrow*.csv` 日志。
> 尚未实现（P5）：真实签名、Merkle proof 验证、源区块头同步、多验证者转发、崩溃恢复。
>
> 启用方式：
> ```bash
> ./set_config.sh --schema preempt-escrow
> SCHEMA=preempt_escrow ./run.sh
> ```
>
> 旧 `preempt`/`ftl`/`caravan`/`lb` 路径保持不变；新逻辑全部由 `IsPreemptEscrow()` 隔离。

> 目标：新增一个完全独立的 `preempt_escrow` schema，实现“托管本金 + nonce 前缀批量授权 + pending credit + Quiesce/Freeze/Settle/Deliver + 超时退款 + flow 门控”。
>
> 约束：现有 `lb` / `ftl` / `caravan` / `preempt` 的行为、日志和实验流程保持不变；新逻辑全部通过 `params.Config.IsPreemptEscrow()` 隔离。

---

## 0. 隔离原则（先定死，后面所有改动都遵守）

1. **新增 schema**：`SchemaPreemptEscrow = "preempt_escrow"`。
2. **不修改 `IsPreempt()` 的语义**：`IsPreempt()` 继续只匹配旧 `SchemaPreempt`（银行代付版）。
3. **所有新行为加 guard**：只在 `IsPreemptEscrow()` 为真时执行。旧 schema 的代码分支、日志、计数器、DB 路径不变。
4. **新增文件优先**：
   - `core/escrow.go`
   - `account/escrow_state.go`
   - `pbft/escrow.go`
   - `core/escrow_test.go`、`account/escrow_state_test.go`、`pbft/escrow_test.go`
5. **尽量不改共享结构**：
   - 不给 `core.Transaction` 加字段（nonce 走 `EscrowTx` 包装 + 新消息；见 §4）。
   - 不给 `account.AccountState` 加字段（迁移/托管状态放新 map/新 trie；见 §4）。
   - 不给 `BlockHeader` 加字段（新根先复用 `MigHash`，Phase 5 再扩展）。
   - `Block` 只增加一个可空字段 `Escrow *EscrowBlockData`；旧 schema 为 nil。
6. **新日志、新命令、新 CSV**：旧 `_bank.csv`、`_block.csv` 列不变；新 schema 另写 `_escrow_*.csv`。
7. **旧结果可回归**：任何新代码合入后，必须能跑 `lb/ftl/caravan/preempt` 小规模 smoke，并确认关键计数与改动前一致。
8. **默认不变**：`params.Config.Schema` 默认仍是 `SchemaPreempt`；`run.sh` 默认仍是 `lb`。只有显式选择 `preempt_escrow` 才启用新协议。

---

## 1. 目标与非目标

### 1.1 本计划目标

- 迁移开始时把账户真实余额 `n` 锁入源分片托管账户，`credit = n`。
- 迁移期间按 **nonce 连续前缀** 批量授权 payer 交易，只预留额度，不移动资金。
- 目的分片记录 **pending credit**，记录时不增加收款方可用余额。
- 迁移完成或超时后执行 Quiesce → Freeze → Debit → Deliver。
- 超时/批次丢失时退款 `n - spent`，未记录交易按原 nonce 重放。
- 迁移账户在目的分片的出账交易通过 flow 通道和 nonce 门控执行。
- 全程可校验：
  - `total_cons <= n`
  - `spent <= total_cons`
  - `escrow` 最终归零
  - 每个账户的交易严格按 nonce 连续生效
  - 无负余额

### 1.2 第一阶段非目标（明确写出，避免过度承诺）

- 不实现真实签名验证（数据集无对应私钥）。
- 不实现多验证者转发/抗扣留；当前活跃路径是单节点 `propose1/commit1`。
- 不实现完整 Merkle proof 验证和源分片区块头同步；先使用“可信 relay”实现顺序/额度/结算语义。
- 不做崩溃恢复；迁移状态先放内存，实验结束做 terminal 检查。Phase 5 再考虑持久化/证明。
- 不实现利息、还款周期、账户级 loan cap。

如果论文实验只需要“额度/结算/超时”语义，以上非目标可以接受，但实验结果中必须注明。

---

## 2. 协议状态

### 2.1 源分片状态（`EscrowRecord.Status`）

```text
Locked      本金已进入 escrow，ν0 已冻结，可授权 payer 交易
Quiescing   已收到 Quiesce 请求，停止新授权
Settled     已收到有效 Freeze/Settle，完成 Debit
Expired     到达 h_lock + Δ 仍未完成 cutover，账户留在源分片
```

建议转移：

```text
Locked -> Quiescing -> Settled
Locked/Quiescing -> Expired
```

### 2.2 目的分片状态（`EscrowPending.Status`）

```text
Prepared    已收到 TXmig2/迁移准备消息，但尚未激活账户
Recording   正在按序接收 batch，维护 pending credit
Frozen      Freeze 完成，只接受连续前缀，不再接受新 batch
Delivered   结算资金已到，pending 已交付
Aborted     源分片 Expired，目的分片丢弃 Prepared 状态
```

### 2.3 所有权决策（关键）

为避免“源分片认为 A 还在，目的分片认为 A 已到”的分裂所有权：

- **在 Debit 完成之前，A 的所有者始终是源分片**。
- 目的分片的 `TXmig2` 只进入 `Prepared`，不改 `account.Account2Shard` 映射。
- Debit 完成且 `Status = Settled` 后，源分片才把 A 的 flow + refund 交给目的分片，并更新映射/announce。
- `Status = Expired` 时，目的分片丢弃 `Prepared`，源分片保留 A，退款回源账户。

> 这会改变 FTL 的 `TXann` 时机（从 TXmig2 后立即 announce 改为 Debit 后 announce），但账户选择、payee 放行、CLPA 流程不变。这样做是为了换取无 rollback 的确定性所有权切换。

---

## 3. 端到端流程

### 3.1 Prepare（源分片，迁移开始）

1. 客户端按原有 `cNewMap` 触发迁移，`handleNewMap` 入队 `TXmig1`。
2. 新区块包含 `TXmig1` 时，源分片生成 `EscrowLock`：
   - 先执行本块内该账户的普通交易（确定最后执行的 nonce）；
   - `n = 账户当前余额`；
   - 账户余额转 `EscrowPool` / 专用 escrow 账户；
   - `ν0 = 本块执行完后的账户 nonce`；
   - 建立 `EscrowRecord`：
     ```text
     mig_id, account, source, dest,
     credit=n, seq_cons=0, total_cons=0,
     nu0, nu_cons=nu0, h_lock, status=Locked
     ```
3. 源账户余额置 0；后续 direct spend 必须走 MPTP/信用通道。

> 实现上建议由 `TXmig1` **系统触发** lock，而不是新增用户提交的 `lock_to_escrow` 交易。当前数据集没有私钥，无法真正签名；如果坚持用户提交，需要先解决账户密钥体系。

### 3.2 Authorize（源分片，迁移期间）

1. 源分片打包入口对 `sender == A` 且 `nonce > ν0` 的 payer 交易：
   - 放进 `MPTP`，不进入普通队列；
   - `nonce <= ν0` 的交易拒绝并记录异常（已消费/非法）。
2. 每个块从 MPTP 按 nonce 升序构造批次：
   - 从 `ν_cons + 1` 开始；
   - 必须 nonce 连续；
   - 累加金额，满足 `total_cons + Φ <= credit`；
   - 一旦遇到 nonce gap 或超额，停止本批次，后面的交易继续等待，**不能跳过**。
3. 构造 `EscrowBatch`：
   ```text
   mig_id, seq_start, seq_end,
   nonce_start, nonce_end,
   txs []EscrowTx, total_value Φ_j,
   src_height, src_block_hash
   ```
4. 源分片执行（仅预留）：
   - `seq_cons += len(txs)`
   - `total_cons += Φ_j`
   - `ν_cons = last nonce`
5. 区块提交后，将 `EscrowBatch` relay 到目的分片（新命令 `escrowBatch`）。

### 3.3 Provision（目的分片，迁移期间）

1. 校验 `mig_id`、`status`、`seq_start == 已记录末尾 + 1`。
2. 乱序批次先缓冲；只处理连续前缀。
3. 对每笔交易：
   - `pending[mig_id][recipient] += value`
   - 记录 `materialized[mig_id][(seq_start,seq_end)] = batch_hash`
   - `ν_dst = last nonce`
4. **不修改任何账户余额**。pending 不可花。
5. 重复/重放/旧 `mig_id` 批次拒绝。

### 3.4 Quiesce

触发条件建议：

- 目的分片提交 `TXmig2` 后，进入 `Prepared`，向源分片发送 `escrowQuies` 请求；
- 源分片也可以在自己的 `h_lock + Δ` 到期时主动 Expire。

源分片收到 Quiesce 后：

- 停止新授权；
- 记录 `S* = seq_cons`、`ν* = ν_cons`；
- 把 MPTP 中剩余交易按 nonce 顺序发给 `S_own`；
- 回复 `EscrowQuiesce{S*, ν*, status, remaining_txs}`。

### 3.5 Freeze

目的分片：

- 等待 `[1, S*]` 全部连续记录，或等待 `Δd` 个块；
- 取连续前缀 `S_m <= S*`；
- `spent = Σ 已记录连续前缀批次金额`；
- `status = Frozen`；
- 冻结后到达的 batch 一律拒绝；
- 发送 `EscrowSettle{mig_id, S*, S_m, spent, ν_dst, status, freeze_height}`。

### 3.6 Debit

源分片验证 `EscrowSettle` 后，在一个块内完成：

1. `escrow -> 目的分片 settlement 账户` 转 `spent`；
2. `escrow -> A@S_own` 退 `n - spent`；
3. `status = Settled`（正常）或 `Expired`（超时）；
4. `A.nonce = max(ν0, ν_dst)`；
5. 若 `S_own == S_dst`：
   - 源分片把 A 的 flow（迁移期间收到的钱）也转给目的分片；
   - 源分片冻结/清空 A 的账户；
   - 更新 `Account2Shard` / announce。
6. 若 `S_own == S_src`：
   - `A.Balance += refund`；
   - A 继续留在源分片；
   - 目的分片 `Prepared` 状态作废。

### 3.7 Deliver

目的分片收到 `spent` 后：

- 从 settlement 账户逐笔交付 pending；
- 收款方在目的分片：直接加余额；
- 收款方在其他分片：发普通跨分片转账到它当前所在分片；
- 收款方自己也迁移了：使用最新映射，必要时由 relay 转发；
- 所有交付使用 `(mig_id, batch_hash)` 幂等键。

### 3.8 Flow 通道

- 目的分片在确认 `⟨S*, ν*⟩` 之前，拒绝/排队 A 的所有出账交易。
- Debit 完成后，A 的普通余额 = flow（迁移期间 incoming）+ refund。
- 从 `max(ν0, ν_dst) + 1` 开始，按 nonce 顺序执行 A 的待执行交易。
- 余额不足时等待，后面的 nonce 跟着等待。
- 若 A 留在源分片，则同样的门控/顺序规则在源分片执行；退款到账后继续。

---

## 4. 数据模型草案

### 4.1 新文件 `core/escrow.go`

```go
type EscrowTx struct {
    Tx    *Transaction // 不修改 Transaction
    Nonce uint64
}

type EscrowStatus uint8

const (
    EscrowLocked EscrowStatus = iota
    EscrowQuiescing
    EscrowSettled
    EscrowExpired
)

type EscrowLock struct {
    MigID      string
    Account    string
    SourceShard int
    DestShard   int
    Credit     *big.Int
    Nu0        uint64
    HLock      int
}

type EscrowBatch struct {
    MigID       string
    SeqStart    uint64
    SeqEnd      uint64
    NonceStart  uint64
    NonceEnd    uint64
    Txs         []*EscrowTx
    TotalValue  *big.Int
    BatchHash   []byte
    SrcHeight   int
    SrcBlockHash []byte
}

type EscrowQuiesce struct {
    MigID       string
    SStar       uint64
    NuStar      uint64
    Status      EscrowStatus
    Remaining   []*EscrowTx
}

type EscrowSettle struct {
    MigID       string
    SStar       uint64
    SM          uint64
    Spent       *big.Int
    NuDst       uint64
    Status      EscrowStatus
    FreezeHeight int
    Proof       []byte // Phase 5
}

type EscrowDeliver struct {
    MigID    string
    BatchID  []byte
    Outputs  []*CreditEntry
    IdemKey  string
}

type EscrowBlockData struct {
    Locks    []*EscrowLock
    Batches  []*EscrowBatch
    Quiesces []*EscrowQuiesce
    Settles  []*EscrowSettle
    Delivers []*EscrowDeliver
}
```

`Block` 只增加：

```go
Escrow *EscrowBlockData
```

`NewBlock` 签名不变；在 `GenerateBlock` 中仅当 `IsPreemptEscrow()` 时 `block.Escrow = ...`。`GetMigTreeRoot` 增加 escrow 条目（旧 schema 为空，根不变）。

### 4.2 新文件 `account/escrow_state.go`

```go
type EscrowRecord struct {
    MigID       string
    Account     string
    SourceShard int
    DestShard   int
    Credit      *big.Int
    SeqCons     uint64
    TotalCons   *big.Int
    Nu0         uint64
    NuCons      uint64
    HLock       int
    Status      EscrowStatus
    SOwn        int
    NuDst       uint64
    Spent       *big.Int
    FreezeHeight int
}

var (
    EscrowLock     sync.Mutex
    EscrowRecords  map[string]*EscrowRecord // mig_id -> record

    PendingLock    sync.Mutex
    PendingCredits map[string]map[string]*big.Int // mig_id -> recipient -> amount
    Materialized   map[string]map[string]bool     // mig_id -> batch id -> recorded

    FlowLock       sync.Mutex
    FlowGates      map[string]bool // account -> true if outgoing blocked
)
```

Nonce 侧表（避免修改 `Transaction`）：

```go
// shard-local: tx.Id -> nonce，仅 preempt_escrow 使用
var NonceByTxID = struct {
    sync.Mutex
    M map[int]uint64
}{M: make(map[int]uint64)}

// account -> last executed nonce
var LastNonce = struct {
    sync.Mutex
    M map[string]uint64
}{M: make(map[string]uint64)}
```

> 如果团队认为侧表太隐晦，也可以选择给 `Transaction` 加 `Nonce uint64`。但那会改变所有 schema 的 gob 类型描述和 `TxHash`，旧结果不再逐字节可比。本文档推荐侧表 + `EscrowTx` 包装。

### 4.3 托管账户

- 每个分片一个 `EscrowPool` 账户（系统地址），用于实际持有托管本金；`EscrowRecord` 记录每个 `mig_id` 的子账。
- 或者每个 `mig_id` 一个派生地址。推荐先用 **一个 pool + 子账**，减少 trie 写入；Phase 5 需要证明时，把子账放入专用 migration-state trie 并给出 `MigStateRoot`。
- 目的分片一个 settlement 账户，用于接收 `spent` 并向收款方交付。

---

## 5. 代码改动地图（含 guard）

| 文件 | 改动 | 隔离方式 |
|---|---|---|
| `params/config.go` | 新增 `SchemaPreemptEscrow`、`IsPreemptEscrow()`；`Normalize()` 映射为半锁；新增 `EscrowInitBalance`、`MigrationTimeoutBlocks`、`SettlementWaitBlocks`、`MaxEscrowBatchTxs` | 旧字段/旧 schema 不变 |
| `params/config_test.go` | 新增 schema predicate/normalize 测试 | 仅测试 |
| `core/escrow.go`（新） | 上述类型、Encode/Decode/Hash | 新文件 |
| `core/block.go` | `Block` 增加 `Escrow *EscrowBlockData`；`PrintBlock` 仅新 schema 打印 | 旧 schema 为 nil |
| `core/txpool.go` | `Tx_pool` 增加 `MPTP`、`EscrowGated`；`FetchTxs2Pack` 在 `IsPreemptEscrow()` 时走新分支 | 新分支插入在旧 `IsPreempt()` 之前；旧分支不动 |
| `chain/blockchain.go` | `GenerateBlock` 组装 escrow payload；`getUpdatedTreeOfState` 末尾执行 lock/batch/settle/deliver；`genesisStateTree` 使用 `EscrowInitBalance` | 全部 `IsPreemptEscrow()` guard |
| `pbft/cmd.go` | 新增命令：`escrowLock`、`escrowClient`、`escrowBatch`、`escrowQuies`、`escrowSettle`、`escrowDeliv`、`escrowAbort`（均 ≤12 字节） | 旧命令不变 |
| `pbft/escrow.go`（新） | 新 handler、状态机、relay、settle、deliver、timeout | 新文件 |
| `pbft/pbft.go` / `pbft/pbftsingle.go` | `handleRequest` 分发新命令；commit 后调用 `escrowAfterCommit` | 新命令/guard |
| `pbft/handleanns.go` | 新 schema 不走旧 `TXns.Debt`；改为 escrow 流程 | `if IsPreemptEscrow()` 分支 |
| `pbft/client.go` | 新 schema 分配 nonce；发送 `EscrowTxFromClient`；停止条件加 migration drain | guard |
| `account/escrow_state.go`（新） | escrow/pending/flow/nonce 状态与断言 | 新文件 |
| `set_config.sh` | 增加 alias `preempt-escrow` / `preempt_escrow` | 旧 alias 不变 |
| `schema_detect.sh` | 识别 `SchemaPreemptEscrow` | 旧识别不变 |
| `run_exp.sh` / `zip_res.sh` | 自动适配新 schema 名称 | 旧流程不变 |
| `exp.csv` / `tmp.txt` | 可选加入新 schema 行 | 不影响现有行 |
| `test/*`（新） | nonce/escrow/pending/settle/timeout 单测 | 新文件 |

### 5.1 必须注意的现有逻辑

- `IsPreempt()` 不能改成“匹配旧 preempt + 新 preempt_escrow”。否则旧银行路径会被新 schema 触发。
- `getUpdatedTreeOfState` 里的 `bankEnabled` 只认 `IsPreempt()`；新 schema 不会走银行。
- `NewLog` 里的 `_bank.csv` 只在 `IsPreempt()` 创建；新 schema 另建 `_escrow_*.csv`。
- `genesisStateTree` 的 bank 账户只在 `IsPreempt()` 创建；新 schema 创建 escrow pool（或首次 lock 时创建）。
- `handleAnnounce` / `mpAndSend` 的 `TXns.Debt` 逻辑只用于旧 preempt；新 schema 要显式跳过。
- 新命令名必须 ≤12 字节：`prefixCMDLength = 12`；`escrowSettle` 恰好 12，`escrowQuies` 11，`escrowBatch` 11，`escrowDeliv` 11。

---

## 6. 交易池与 nonce 规则

### 6.1 客户端

- 新 schema 下，加载数据集时按 **每个 sender 的出现顺序** 分配 `nonce = 1,2,...`。
- 发送时使用新消息 `EscrowTxFromClient{Txs []*EscrowTx}`。
- 同一 sender 的交易必须按 nonce 顺序发送；当前 `InjectTXS` 的 per-shard goroutine 可能导致乱序，需要：
  - 同一 shard 的发送串行化，或
  - 接收端按 nonce 重排（推荐两者都做）。
- 停止条件增加“所有迁移 terminal + pending 交付完成”。

### 6.2 源分片

- `MPTP map[string][]*EscrowTx`，按 nonce 排序。
- `FetchTxs2Pack`：
  - 新 schema 且 `sender` 是 `Locked/Quiescing` 账户：
    - `nonce <= ν0`：拒绝；
    - `nonce > ν0`：放 MPTP；
  - 其他交易正常打包。
- 构造批次时不能跳过：
  - 非连续 nonce → 停；
  - `total_cons + Φ > credit` → 停；
  - 批次最大笔数/最大字节数限制。
- `ν_cons`、`total_cons` 必须在源分片持久化到 `EscrowRecord`，并在每块提交后更新。

### 6.3 目的分片

- `PendingRecord` 维护：
  - `materialized` 去重；
  - 连续前缀游标；
  - `ν_dst`。
- 乱序 batch 缓冲；Freeze 时只取连续前缀。
- `FlowGated[A] = true` 直到 Debit 完成；`EscrowGated[A]` 中的交易按 nonce 排序等待。
- 门控解除后，从 `max(ν0,ν_dst)+1` 开始执行；缺口等待，不跳过。

---

## 7. 结算、超时与消息可靠性

### 7.1 Quiesce

- 目的分片在 `TXmig2` 提交后发送 Quiesce 请求。
- 源分片收到后：
  - `status = Quiescing`；
  - 停止新 batch；
  - 记录 `S*`、`ν*`；
  - 剩余 MPTP 交易按 nonce 顺序发往 `S_own`。
- 若源分片在 `h_lock + Δ` 前没有收到 Quiesce：
  - `status = Expired`；
  - `S* = seq_cons`，`ν* = ν_cons`；
  - 主动发送 Quiesce（带 Expired 状态）给目的分片。

### 7.2 Freeze

- 目的分片等待 `[1,S*]` 连续记录或 `Δd`。
- `S_m =` 连续前缀末尾。
- `spent =` 连续前缀批次金额之和。
- 冻结后拒绝任何新 batch（包括晚到的连续 batch）。
- 未记录/缓冲的 batch 不交付，金额包含在退款中。

### 7.3 Debit / Refund

- 源分片验证 `EscrowSettle`：
  - `spent <= total_cons <= n`；
  - `S_m <= S*`；
  - `mig_id/attempt` 匹配。
- 一个块内：
  - escrow → 目的 settlement：`spent`；
  - escrow → `A@S_own`：`n - spent`；
  - 更新 `status`、nonce、所有权。
- 若 `S_own == S_dst`：
  - 源分片把 A 的 flow 也转给目的分片；
  - 源分片冻结 A 的账户；
  - Debit 成功后再 announce/更新映射。
- 若 `S_own == S_src`：
  - A 留在源分片，`Balance += refund`；
  - 目的分片 `Aborted`。

### 7.4 Deliver

- 目的分片 settlement 账户 → pending：
  - 本地直接入账；
  - 远程发普通跨分片转账；
  - 收款方迁移了则按最新映射转发。
- 交付幂等：`(mig_id, batch_hash)` 只交付一次。
- 交付完成后再释放 A 的 flow gate（如果 A 已迁移）。

### 7.5 消息可靠性

当前 `utils.TcpDial` 失败重试一次后丢弃消息。新协议至少需要：

- `mig_id + attempt` 作为 fencing token；旧实例消息拒绝；
- `EscrowBatch` / `EscrowQuiesce` / `EscrowSettle` / `EscrowDeliver` 带 ack；
- 发送方定期重试直到 ack；
- 接收方幂等处理；
- 状态机支持“重复消息不改变状态”。

如果实验阶段不实现 ack/retry，必须把 schema 标记为“可信网络原型”，不能声称抗消息丢失/抗审查。

### 7.6 超时单位

- 源分片使用自己的块高判断 `h_lock + Δ`；这是权威的 Expired 判定。
- 目的分片不自行判断源分片高度，只依据 Quiesce/Settle 中的状态。
- `Δ` 与 `Δd` 配置单位为“块数”，不是本地墙钟时间。
- 客户端停止前必须 drain，否则超时永远不会触发。

---

## 8. 账务不变量与检查

### 8.1 公式

```text
lock 时:      escrow += n, account.balance -= n
authorize:   total_cons += Φ_j,  escrow 不变
freeze:      spent = Σ 连续前缀 Φ_j
debit:       escrow -= spent + refund,  refund = n - spent
deliver:     settlement -= delivered,  recipient.balance += delivered
```

必须满足：

```text
total_cons <= n
spent <= total_cons
escrow_pool == Σ(未结算 credit) - Σ(已支出 spent) - Σ(已退款 refund)  (按子账)
refund + spent == n
Σ pending == spent - Σ delivered
```

### 8.2 运行时断言

- 任何账户 `Balance >= 0`（仅新 schema 强制）。
- `ν_dst` 只能连续前进。
- `seq_cons` / `total_cons` 单调递增。
- 已 `Frozen` 的 `mig_id` 不再新增 batch。
- 已 `Settled/Expired` 的 `mig_id` 不再接受任何 batch/settle。
- `Deliver` 不得重复。

### 8.3 停机不变量

实验结束时：

- 所有 `EscrowRecord.Status ∈ {Settled, Expired}`；
- `escrow_pool` 为 0；
- pending 全部 delivered；
- 无 `Locked/Quiescing/Frozen/Prepared` 残留；
- 每个账户 nonce 与已生效交易一致。

如果客户端只是“连续 5 个空块”就停，可能残留上述状态。新 schema 的停止条件必须改为上述 terminal 检查。

---

## 9. 日志与指标

新 schema 新增（旧 `_bank.csv` 不动）：

| 文件 | 内容 |
|---|---|
| `S*_escrow_lock.csv` | `height, mig_id, account, n, nu0, h_lock, src, dst` |
| `S*_escrow_batch.csv` | `height, mig_id, seq_start, seq_end, nonce_start, nonce_end, num_txs, value, total_cons, nu_cons, blocked_reason` |
| `S*_escrow_pending.csv` | `height, mig_id, seq, value, nu_dst, buffered, recorded` |
| `S*_escrow_settle.csv` | `height, mig_id, s_star, s_m, spent, refund, nu_dst, status, freeze_height` |
| `S*_escrow_deliver.csv` | `height, mig_id, recipient, value, local_or_relay, delivered` |
| `S*_escrow_flow.csv` | `height, account, gated, flow_balance, refund, executed_nonce` |
| `S*_escrow_queue.csv` | `height, normal, mptp, nonce_wait, balance_wait, gated` |
| `log/escrow_invariants.csv` | `height, total_cons, spent, refund, escrow_balance, pending, violation` |

指标口径：

- **authorized**：源分片 batch 确认；
- **recorded**：目的分片连续前缀记录；
- **settled**：Debit 完成；
- **delivered**：收款方实际到账；
- 分别统计 count/value/latency；
- 聚合条目与其 relay 输出不能重复相加。建议以 `mig_id + nonce` 或 `tx.Id` 去重，逻辑交易只计一次。

旧 `_block.csv` 的 `real_tx_committed` 在新 schema 下含义会冲突，建议新 schema 不依赖该列，改用 `_escrow_*.csv` 的口径。

---

## 10. 测试与验收

### 10.1 单元测试

- `TestEscrowLockMovesBalance`
- `TestNoncePrefixStopsAtGap`
- `TestNoncePrefixStopsAtOverCredit`
- `TestNoSkipOverLimit`
- `TestBatchDedupAndReorder`
- `TestFreezeOnlyContiguousPrefix`
- `TestRefundAndSpentConservation`
- `TestTimeoutExpiredKeepsAccountAtSource`
- `TestFlowGateBlocksOutgoing`
- `TestPostCutoverNonceOrder`
- `TestRecipientAlsoMigrating`
- `TestDuplicateSettleDeliverIdempotent`
- `TestEscrowPoolZeroAfterSettlement`
- `TestRollbackOldSchemas`（旧 schema 行为回归）

### 10.2 协议测试（不启动网络）

- 直接构造 `BlockChain` + `Tx_pool`，模拟：
  - 批次乱序/丢失；
  - Quiesce 丢失；
  - Settle 重复；
  - 收款方迁移；
  - 超时 + 部分记录。
- 断言状态机、nonce、余额、escrow、pending。

### 10.3 集成 smoke

- 新脚本或复用 `run.sh`：
  - `SCHEMA=preempt_escrow NUM_SHARDS=2 NUM_NODES=1 TEST_FILE=<small.csv>`；
  - 等待 drain 完成；
  - 检查 `_escrow_invariants.csv` 无 violation；
  - 检查 escrow 归零、pending 清零。
- 旧 schema 回归：
  - `SCHEMA=lb/ftl/caravan/preempt` 各跑一个小规模 run；
  - 对比 `_block.csv` 计数、`_queueLen.csv`、最终账户余额（可加 debug dump）与改动前 baseline。

### 10.4 验收清单

- [ ] `go build ./...` 通过
- [ ] `go test ./...` 通过
- [ ] `lb/ftl/caravan/preempt` 小规模 smoke 无行为变化
- [ ] `preempt_escrow` 小规模 run 完成
- [ ] `total_cons <= n` 全程成立
- [ ] `spent <= total_cons` 全程成立
- [ ] 结算后 escrow 为 0
- [ ] pending 全部交付
- [ ] 无负余额
- [ ] 超时路径 A 留在源分片，无分裂所有权
- [ ] 批次重复/乱序/丢失不导致重复付款

---

## 11. 分阶段实施

| Phase | 内容 | 产出 | 退出条件 |
|---|---|---|---|
| **P0 隔离骨架** | 新 schema、predicate、Normalize、配置字段、脚本 alias、空 hook | `SCHEMA=preempt_escrow` 可以启动，行为等同 FTL | 旧 schema 全部回归通过 |
| **P1 Nonce + 真实余额** | 客户端 nonce、`EscrowTx`、非负余额检查、`EscrowInitBalance` | 账户有 nonce；余额不足会排队/拒绝 | nonce 连续、无负余额单测通过 |
| **P2 Escrow + MPTP 批量授权** | lock、MPTP、前缀批次、源分片预留额度 | 源分片可授权，不移动资金 | `total_cons <= n`、不跳超额交易 |
| **P3 Pending + Settle + Refund + Deliver** | 新消息、目的分片 pending、Quiesce/Freeze/Debit/Deliver、超时、flow gate | 端到端可结算 | escrow/pending 归零、超时正确 |
| **P4 指标 + drain + 回归** | 新 CSV、invariant 检查、客户端 drain、旧 schema 回归 | 可做对比实验 | 所有验收清单通过 |
| **P5 证明/多验证者（可选）** | batch root、migration-state root、proof 验证、源区块头同步 | 真实安全语义 | 证明验证/抗扣留测试通过 |

建议先完成 **P0–P4（可信单节点原型）**，P5 单独评估。P5 的工作量和风险远高于前四阶段。

---

## 12. 关键决策与推荐

| 决策 | 选项 | 推荐 |
|---|---|---|
| 锁定触发 | 用户提交 `lock_to_escrow` vs `TXmig1` 系统触发 | **系统触发**；数据集无私钥，无法真签名 |
| Nonce 承载 | 修改 `Transaction` vs `EscrowTx` 包装 + 侧表 | **包装 + 侧表**，避免旧 schema 序列化变化 |
| 所有权切换 | TXmig2 后立即激活 vs Debit 后激活 | **Debit 后激活**，避免目的分片 rollback/分裂所有权 |
| Escrow 账户 | 每 mig 一个地址 vs 每分片一个 pool + 子账 | **pool + 子账**；Phase 5 再加 migration-state trie |
| 收款方范围 | 全部跨分片 vs 仅 S_dst（论文简化） | **先仅 S_dst**，再补跨分片 Deliver |
| 证明 | 先不做 vs 一开始做 | **先不做**，明确“可信 relay 原型”；P5 再补 |
| 超时判定 | 墙钟 vs 块高 | **源分片块高**；目的分片只信 Quiesce/Settle 状态 |
| 旧 preempt | 替换 vs 新增 schema | **新增 schema**，保留旧银行版做对比 |

---

## 13. 复现实验注意事项

1. **必须使用真实余额**：`EscrowInitBalance` 不能是 `10^60`。建议按数据集中每个 sender 的总流出计算一个有限初始余额，或整体缩放 value；确保 `total_cons <= n` 会真正触发。
2. **必须加余额检查**：当前实现允许负余额；新 schema 下必须拒绝/排队余额不足的交易。
3. **必须 drain**：客户端停止前等待所有迁移 terminal、pending 交付、escrow 归零，否则不变量检查会失败，且结果不可比。
4. **指标分开**：authorized / recorded / settled / delivered 分开；不要复用旧 `real_tx_committed` 口径。
5. **收款方范围**：如果只授权 S_dst 收款方，必须在结果中注明；这会减少跨分片 pending/deliver 的复杂度，也会改变延迟。
6. **旧结果不可直接对比**：新 schema 的 Preempt 语义与旧银行版不同，命名和结果目录要区分（例如 `res_preempt_escrow_*`）。

---

## 14. 兼容性检查清单

- [ ] `params.Config.Schema` 默认未改。
- [ ] `IsPreempt()` 仍只匹配旧 `SchemaPreempt`。
- [ ] 旧 schema 不创建 `_escrow_*.csv`。
- [ ] 旧 schema 的 `_bank.csv` 仍在旧 preempt 下生成。
- [ ] 旧 schema 不执行 escrow branch（可用 `grep -n "IsPreemptEscrow"` 审查）。
- [ ] `GetMigTreeRoot` 在 escrow 为 nil 时生成与改动前相同的根。
- [ ] `Block.PrintBlock` 旧 schema 输出不变。
- [ ] `go build ./...`、`go test ./...` 通过。
- [ ] 旧 schema 小规模 run 的 block/queue 计数与 baseline 一致（允许时间抖动，不允许逻辑差异）。

---

## 15. 结论

`preempt_escrow` 可以作为现有 `preempt` 的并行 schema 实现，不需要改动旧银行路径。建议按 P0→P4 先完成“可信单节点 + 本地收款方”的可运行原型，验证：

- escrow 真实余额；
- nonce 前缀授权；
- pending credit；
- 结算/退款/超时；
- flow 门控；
- 不变量与停机 drain。

P5 的 Merkle proof、源区块头同步、多验证者转发可以独立评估；在完成 P5 之前，不应声称实现了“抗扣留/完整安全”语义。

## 16. 当前实现与原计划的差异（P0–P4）

- Lock 由 `TXmig1` 系统触发，而不是新增用户提交的 `lock_to_escrow` 交易（数据集无签名密钥）。
- 账户所有权在 **Debit 之后** 才切换到目的分片，避免超时 rollback；`TXmig2` 只创建 Prepared 副本。
- 源分片用 `EscrowBatchArchive` 保留已授权批次，直到 Freeze 给出 `SM`；未记录批次按原 nonce 重放。
- 跨分片 Deliver 复用现有 relay 机制（合成 `IsRelay` 交易），不新增完整 proof 校验。
- 日志先实现为 `S*_escrow.csv` + `S*_escrow_queue.csv`，而不是计划中的每类一个 CSV。
- 普通交易的负余额检查仍依赖 `EscrowInitBalance` 校准；默认值高于 20W.csv 的最大单账户流出，避免旧实验卡死。要做“额度真正收紧”的实验，需要把该值调小并使用校准数据集。
- 可信网络原型没有 ack/retry。为防注入消息丢失导致 gated nonce 永久卡住，新增 `EscrowGapRepairBlocks`（默认 30 块）：超过该块数仍无进展时跳过缺失 nonce 并打印 `[escrow][gap-repair]`；设为 0 可禁用（严格 nonce 语义）。这不是 P5 的可靠消息替代方案。
- 签名、Merkle proof 验证、源区块头同步、多验证者转发、崩溃恢复均未实现（P5）。
