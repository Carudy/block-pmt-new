# Implementation Plan — Schema (FTL / Caravan / Preempt) for Block-Fine-Tune

> 2024-xx 更新：全局用一个 `schema` 参数控制三种方案，取代原来的 `EnableBankMechanism` 等银行机制开关。
> 旧“银行机制”设计（本文件下方 §1 之后的部分）仍是 preempt 的实现蓝本，控制点统一改由 `Schema` 派生。
>
> 三种方案的**演进顺序**为 `ftl -> caravan -> preempt`。当前仓库：ftl 是基线（bank 关闭路径），
> preempt 已实现；**caravan 是尚缺的中间环节**，需在现有 ftl 迁移流程上补充“目的分片打包前聚合”这一段。

## 0. Schema 架构（当前阶段）

### 0.1 三种方案（`params.Config.Schema`：`"ftl"` / `"caravan"` / `"preempt"`）

- **ftl（Fine-tuned Lock，基线）**：迁移中锁住迁移账户的 payer 交易（sender 为迁移账户、需扣余额的交易），
  放行 payee 交易（只加余额）；迁移结束后，被锁 payer 交易注入目的分片交易池并 unlock，随后按普通交易打包执行。
  迁移/注入/打包各环节与旧 `EnableBankMechanism=false` 完全一致。
- **preempt（银行代付）**：迁移中由源分片把迁移账户的 payer 交易聚合成借款（`TXborrow`），银行直接付给 payee，
  迁移完成后账户在目的分片还款（`TXns.Debt`）并经 `TXsettle` 跨分片结算。
- **caravan**：迁移流程与 ftl **完全一致**（锁、迁移、注入、payee 处理都不改），唯一改动在
  “迁移完成、payer 交易注入目的分片并 unlock 之后”：目的分片矿工在打包前，把同一迁移账户的多笔合法 payer 交易
  按 nonce 前缀贪心聚合成**一笔“单账户→多输出”交易**再执行（一次合并扣减 + 每个收款人一次入账，只占 1 个块槽位）。
  ——即 ftl 与 preempt 之间的中间档：ftl 在迁移窗口“不做”，preempt 在迁移窗口“让银行做”，caravan 在迁移完成后
  “由目的分片做（但压缩槽位）”。

### 0.2 代码控制点（已完成重构）

- `params/config.go`：新增 `Schema` 字段与 `SchemaFTL/SchemaCaravan/SchemaPreempt` 常量，删除 `EnableBankMechanism`
  及未使用的 `BankInterestRate/MaxLoanPerAccount/LoanRepaymentPeriod`（保留 `BankInitialBalance`，仅 preempt 用）。
  提供方法 `IsFTL()/IsCaravan()/IsPreempt()`。默认 `SchemaPreempt`。
- 所有原 `*.EnableBankMechanism` 判断改为 `*.IsPreempt()`。preempt 特有路径（TXborrow 聚合、Credit/Loan、
  TXsettle、bank 账户、`_bank.csv`）仅当 `IsPreempt()` 才走；ftl/caravan 走 FTL 基线路径。

## 0.3 Caravan 实现计划（分阶段）

**阶段 1（schema 重构，已完成）**：见 §0.2。

**阶段 2（FTL 基线）**：`Schema=ftl` 即 bank 关闭路径，行为同改造前。（待用 `Schema=ftl` 跑一次做对比基线）

**阶段 3（标记“迁移完成、已 unlock 的 payer 交易”，已完成）**：
- `core/transaction.go`：`Transaction` 新增 `FromMigration bool`（仅内存标记，不参与 tx 哈希）。
- `pbft/pbft.go → handleChangesAndPendings`（半锁路径）与 `pbft/PBFTforMigrate.go → handleBalancesAndPendings`
  （epoch 路径）：当 `IsCaravan()` 且 pending 交易 `sender == 该迁入账户` 时置 `FromMigration = true`。

**阶段 4（目的分片打包前聚合 TXcaravan，已完成）**：
- `core/caravan.go`：`TXcaravan{Address, TotalValue, NumTxs, Outputs []*CreditEntry}` + Encode/Decode/Hash。
- `core/block.go`：`Block` 新增 `TXcaravans` 字段并纳入 `NewBlock`/gob 编解码；`PrintBlock` 输出条目数。
- `core/txpool.go`：`Tx_pool` 新增 `Caravan_Pool`；`FetchTxs2Pack` 中当 `IsCaravan() && v.FromMigration` 时
  按 sender 聚合进 `Caravan_Pool`（`continue`，不占普通槽位）；新增 `FetchCaravans2Pack()` 取走并清空。
- `chain/blockchain.go`：`GenerateBlock` 取走 caravans 并传入 `NewBlock`/`GetMigTreeRoot`/`getUpdatedTreeOfState`；
  `getUpdatedTreeOfState` 执行：`a.Balance -= TotalValue`（一次），每个本地收款人各入账一次（远程收款人跳过）。
- `pbft/pbft.go`：新增 `TryRelayCaravans`（仿 `TryRelayBorrows`），把跨分片收款人 relay 出去；
  `commit1`/`handleCommit` 在打包后调用。
- 计数：`_block.csv` 新增 `caravan`、`caravan_tx` 列；`real_tx_committed = tx_committed + borrow_tx + caravan_tx`。
- 单测：`core/caravan_test.go`（聚合/不聚合/编解码往返）。

**阶段 5（对比实验 & 指标，待做）**：`Schema` 依次取 `ftl / caravan / preempt` 各跑一次，对比目的分片
`_queueLen.csv`、`_block.csv` 的 `real_tx_committed`；`zip_res_ft.sh` 按 schema 输出 `res_finetune/res_caravan/res_preempt.zip`。

### 0.4 已确认的口径（lab 确认）

1. **排序依据**：直接用交易在目的分片交易池中的到达顺序（FTL 重新注入后仍保持原数据集顺序），不引入显式 nonce。
2. **余额**：账户为无限余额，整批纳入（不做“余额不足前缀截断”）；实现中仍保留一次合并扣减的语义。
3. **计数**：一个 `TXcaravan` 条目在 `_block.csv` 计 1 条（`caravan`），其 `NumTxs` 计入 `caravan_tx`；
   `real_tx_committed = tx_committed + borrow_tx + caravan_tx`。

## 1. Objective（旧：银行机制 Preempt 设计）

Add an optional **bank mechanism** on top of the existing Fine-tuned Lock protocol, so that an account that is
migrating out of a shard can keep **paying** (sender-side transactions) instead of having its payer
transactions blocked.

For a migrating account `A`, the source shard's **bank pays the payee directly** on `A`'s behalf, and we record a
**loan** (`A` owes the bank). When migration completes, `A` **repays** the loan from its final balance in the
target shard.

The mechanism must be gated by a **switch** so the existing Fine-tuned Lock behavior remains the default
fallback.

### Key simplification (agreed)

There is **no separate "borrow" token transfer** (`bank → A → payee`). We collapse it to:

- `bank → payee` (the bank pays the recipient directly), and
- `loan[A] += value` (bookkeeping only).

Therefore the minimal implementation needs **no new transaction types and no new transaction pools**. Debt is
carried through the **existing** `TXns` migration message (which is sent at **Announce time**, when the loan is
finalized), by adding a `Debt *big.Int` field. Aggregation into explicit `TXborrow` / `TXrepay` transactions is
an optional optimization, deferred to a later phase.

## 2. Background — current behavior we are extending

Recap of the migration path in this repo (files verified):

- `chain/blockchain.go → getUpdatedTreeOfState` is the single place balances change. For every normal tx it
  does `sender.Balance -= value` (unless relay) and `recipient.Balance += value` (subject to `Location`/`Migrate`
  guards).
- Under Fine-tuned Lock (`Stop_When_Migrating=false`, `Lock_Acc_When_Migrating=false`), a migrating account is
  placed in `account.Outing_Acc_Before_Announce`. In `core/txpool.go → FetchTxs2Pack`, any tx whose **sender** is
  such an account is diverted into `Outing_Before_Announce_TX_Pools` (i.e. **payer txs are blocked**). Incoming
  (payee) txs still flow.
- `TXmig1` snapshots the outgoing balance (`outbalance`). `TXmig2` sends that snapshot to the target shard.
- `handleAnns` computes `change = current_balance - BalanceBeforeOut` and sends it via `TXns` so the target shard
  adds the incoming-value delta.
- `account/account_state.go` holds in-memory maps (`BalanceBeforeOut`, `Outing_Acc_Before_Announce`,
  `Lock_Acc`, etc.) used to track migration-window state.

The bank mechanism replaces only the "block payer txs" decision with "let the bank pay on the account's behalf".

## 3. Design overview

### 3.1 Bank account

- Each shard has one **bank account** at a well-known address (e.g. `bankAddr(shardID)`).
- It is a normal `AccountState` with `Location = shardID`, `Balance = BankInitialBalance`, `Migrate = -1`.
- Created in `chain/blockchain.go → genesisStateTree`.
- It must be a **real trie account** (not an in-memory variable) so its balance is part of the state root and
  agreed on by all nodes in the shard via PBFT, and so value conservation holds.

### 3.2 Credit and debt (in-memory, matching existing style)

For a migrating account `a`:

```
credit(a) = BalanceBeforeOut[a]            // frozen balance snapshot (own funds)
          + Σ incoming_value(a)            // payee txs received during the migration window

loan(a)   = Σ outgoing_value(a)            // payer txs paid by the bank on a's behalf
```

Invariant enforced everywhere: `loan(a) <= credit(a)`. This is the **only** constraint (no interest, no
repayment window). It is exactly the no-insolvency condition:

```
final_balance(a) = credit(a) - loan(a) >= 0
```

Note: `value` below means `Transaction.Value` (this simulator has no separate fee field).

Tracked in new in-memory maps (mirroring `BalanceBeforeOut`):

```go
// account/bank_state.go (new)
var Credit map[string]*big.Int   // keyed by address, migrating accounts only
var Loan   map[string]*big.Int
// + a mutex each, mirroring BalanceBeforeOutLock style
```

### 3.3 No new tx types / pools (minimal)

- Source-side bank deduction is done **inline** in `getUpdatedTreeOfState` (one bank trie write per payer tx).
- The loan amount is carried to the target shard inside the **existing** migration message (`TXns`), by adding a
  `Debt *big.Int` field.
- Target-side repayment is applied **inline** when the migration completes (same place `TXns` delta is applied).

Aggregated `TXborrow` / `TXrepay` transactions are an optional Phase 4 optimization and are **not** required for
correctness.

## 4. Data changes

- `account/bank_state.go` (new): `Credit`, `Loan` maps + mutexes.
- `account/account_state.go`: no change (credit/debt live in memory, matching `BalanceBeforeOut`).
- `core/txns.go`: add `Debt *big.Int` to `TXns` so the loan travels with the balance-adjustment message.
- `params/config.go`: wire `EnableBankMechanism` into logic (fields already exist).

## 5. Config / switch

In `params/config.go`, the fields already exist. Wire them in:

- `EnableBankMechanism bool` — **master switch**. `false` = exact current behavior (Fine-tuned Lock blocks payer
  txs); `true` = payer txs of migrating accounts proceed via the bank.
- `BankInitialBalance` — seeds the bank account.
- `MaxLoanPerAccount` — optional per-account cap (Phase 3).
- `BankInterestRate`, `LoanRepaymentPeriod` — **Phase 3 (deferred)**: not required for the core flow. Document as
  "not yet enforced".

Recommended default: `EnableBankMechanism: false` (safe fallback) until validated. The mechanism only applies to
the half-lock path (`!Stop_When_Migrating && !Lock_Acc_When_Migrating`). Full lock and stop-migration modes are
unchanged.

## 6. Protocol flow

### 6.1 Genesis

Each shard seeds its bank account in `genesisStateTree` with `BankInitialBalance`.

### 6.2 Migration starts (`TXmig1`)

Source snapshots `credit[a] = balance snapshot` (reuses `BalanceBeforeOut`) and marks `a` migrating
(`Outing_Acc_Before_Announce`). No bank action yet.

### 6.3 Payer tx `a → R` arrives (bank enabled)

In `core/txpool.go → FetchTxs2Pack`: when bank is enabled, a payer tx from a migrating account is **not** diverted
to `Outing_Before_Announce_TX_Pools`; it flows into the block.

In `chain/blockchain.go → getUpdatedTreeOfState`, for such a tx:

1. Do **not** deduct `a.Balance` (it is frozen for transfer).
2. Deduct `bank.Balance -= value` (bank pays).
3. Credit `R.Balance += value` (unchanged).
4. Record `Loan[a] += value`.
5. Enforce the single guard incrementally: `Loan[a] + value <= Credit[a]` (against the **current** credit,
   which grows as incoming payee txs arrive). If exceeded, fall back to the old block behavior (put the tx in
   `Outing_Before_Announce_TX_Pools`). This is equivalent to `credit >= Σ payer_tx_value` and guarantees
   `final_balance(a) >= 0`.

### 6.4 Carry the loan to the target shard

At **Announce time** (`pbft/handleanns.go → mpAndSend`), the source reads the finalized `Loan[a]`, deletes the
in-memory `Loan`/`Credit` entries, and sets `TXns.Debt = Loan[a]` alongside the existing `TXns.Change` balance
delta.

### 6.5 Repay in the target shard

When the target shard applies the migration (where it applies the `TXmig2` snapshot and `TXns` delta):

1. `a.Balance -= Loan[a]`
2. `target_bank.Balance += Loan[a]`
3. Clear `Loan[a]`.

Net: `a`'s final balance = snapshot + incoming − outgoing = correct.

### 6.6 Cross-shard bank settlement (see §9)

After repayment, source bank is short and target bank is long by the loan amount. Account/recipient balances are
already correct; this step only rebalances the two bank accounts.

## 7. Code change map (file-by-file)

| File | Change |
|------|--------|
| `account/bank_state.go` (new) | `Credit`, `Loan` maps + mutexes |
| `core/txns.go` | add `Debt *big.Int` field |
| `params/config.go` | wire `EnableBankMechanism` into logic; document deferred fields |
| `chain/blockchain.go` | genesis bank account; `getUpdatedTreeOfState` bank-pays + record-loan + repay; `GenerateBlock` credit init |
| `core/txpool.go` | allow payer txs when bank enabled (skip the divert-to-lock branch) |
| `pbft/handleanns.go` | `mpAndSend`: populate `TXns.Debt` from `Loan`; (Phase 2) settlement send/handle |
| `pbft/pbft.go` | `handleCommit` logging; failure rollback |
| `pbft/cmd.go` | (Phase 2) `cSettle` command constant |
| `pbft/client.go` | (Phase 2) settlement handler if needed |

## 8. Consistency & atomicity

- **Value conservation (account/recipient):** guaranteed by `bank -= loan` on source and
  `account -= loan; bank += loan` on target.
- **No insolvency:** enforced by `loan <= credit` at every payer-tx admission.
- **Failed migration rollback:** when `Fail`/`Fail_Time` triggers, the source shard must **net the loan against the
  account's own balance** (since the account did not actually move): `account.Balance -= loan`,
  `bank.Balance += loan`, then clear `Loan`. Add this next to the existing `Fail` handling in
  `pbft/pbft.go → handleCommit` and `chain/blockchain.go → AddBlock`.
- **Atomicity of loan+migration:** a loan is only recorded for a tx packed in a block that also carries the
  corresponding `TXmig1`; repayment is only applied after the migration is finalized in the target shard.

## 9. Cross-shard bank settlement — decision + recommendation

**Problem:** source bank lends `L`; target bank receives `L` on repayment. Without settlement, per-shard bank
balances drift (total system value stays conserved, but bank funds are misallocated).

**Options:**

1. **Settlement message (recommended).** Add `TXsettle` / `cSettle`: target bank sends the repaid amount back to
   the source bank (net). Faithful and keeps each shard's bank balance meaningful.
2. **Global/logical bank.** Track bank as a single logical balance (sum across shards), accept per-shard drift,
   and report only the global bank balance.
3. **No settlement (Phase 1 only).** Ship borrow/repay first, log the imbalance, add settlement in Phase 2.

**Recommendation:** implement **Option 3 for Phase 1** (correctness of accounts/recipients first, log the bank
imbalance), then **Option 1 in Phase 2** for full cross-shard consistency.

## 10. Statistics & logging

Add CSV writers in `pbft.NewLog` (mirror `txlog`, `blocklog`):

- `banklog`: `height, shardID, bank_balance, total_lent, total_repaid, active_loans`
- `payerlog`: `height, address, value, credit, loan, admitted(bool)` — for "statistics of payer tx".

Log fields per committed block in `pbft/pbft.go → handleCommit`.

## 11. Backward compatibility

- `EnableBankMechanism=false` must reproduce today's behavior byte-for-byte (no bank account action, no
  bank-pays path executes).
- The new `TXns.Debt` field is `nil`/zero in that case, so encoding remains compatible.
- Full-lock and stop-migration modes are untouched.

## 12. Validation & testing

1. `go build ./...` and `go vet ./...` after each phase.
2. Unit-style checks (extend `test/`): bank genesis balance correct; `loan <= credit` enforced; bank-pays + repay
   net to zero change for a single account; failed-migration rollback nets correctly.
3. Manual run comparing `EnableBankMechanism=false` vs `true` on `20W.csv`: account final balances must be
   identical between the two modes (bank changes only *liquidity/timing*, not *final* balance).
4. CSV diff of `txlog` / `blocklog` to confirm payer txs increased in the bank-enabled run while totals stay
   consistent.

## 13. Risks & open questions

- **Interest & repayment period** (`BankInterestRate`, `LoanRepaymentPeriod`) are out of scope for Phase 1. Confirm
  whether "repay at migration completion" is the intended (and only) repayment trigger.
- **"Credit = own balance + incoming value":** confirm "payee fees" means incoming transfer **value** (this
  simulator has no separate fee field).
- **Bank accounts must never themselves migrate** — they are special system accounts.
- **Cross-shard settlement scope:** defer to Phase 2 per §9, but confirm the desired bank-balance invariant now.

## 14. Implementation phases

1. **Phase 1 — core bank-pays + loan record (no settlement, no new pools):** config switch, genesis bank account,
   `Credit`/`Loan` maps, tx admission change, `getUpdatedTreeOfState` bank-pays + repay, `TXns.Debt` carry,
   failure rollback, logging. Enable by default `false`.
2. **Phase 2 — cross-shard bank settlement:** `TXsettle`/`cSettle` message to rebalance source/target banks.
3. **Phase 3 — loan policy:** enforce `MaxLoanPerAccount`, `BankInterestRate`, `LoanRepaymentPeriod` if required.
4. **Phase 4 (optional) — aggregation:** introduce `TXborrow`/`TXrepay` + pools to batch bank-side trie writes and
   messages, if profiling shows it is worth it.
5. **Phase 5 — validation:** differential testing (bank on/off), rollback/failure tests, throughput/latency
   metrics vs. baseline Fine-tuned Lock.
