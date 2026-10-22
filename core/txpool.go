package core

import (
	"blockEmulator/account"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/hex"
	"fmt"
	"math/big"
	"sort"
	"sync"
	"time"
)

// type TxHeap []*Transaction

// func (h TxHeap) Len() int {
// 	return len(h)
// }

// func (h TxHeap) Less(i, j int) bool {
// 	cmp := h[i].Utility - h[j].Utility
// 	if cmp == 0 {
// 		return h[i].Id < h[j].Id
// 	}
// 	return cmp > 0
// }

// func (h TxHeap) Swap(i, j int) {
// 	h[i], h[j] = h[j], h[i]
// }

// func (h *TxHeap) Push(x interface{}) {
// 	*h = append(*h, x.(*Transaction))
// }

// func (h *TxHeap) Pop() interface{} {
// 	res := (*h)[len(*h)-1]
// 	*h = (*h)[:len(*h)-1]
// 	return res
// }

var (
	Txs      []*Transaction
	OutAccs  []*TXmig1
	InitTime int64
)

type Tx_pool struct {
	// 交易队列
	Queue []*Transaction
	// Heap TxHeap
	// relay到不同分片的交易队列
	Relay_Pools map[string][]*Transaction
	//锁
	Relaypoollock sync.Mutex
	//锁
	Lock sync.Mutex

	Migration_Pool map[string]int

	// 用来收集交易池中Outing_Acc发出的交易，由Outing_Acc_Before_Announce_Lock控制
	Outing_Before_Announce_TX_Pools map[string][]*Transaction

	// 用来收集Outing_Acc的要进入交易池的交易，由Outing_Acc_After_Announce_Lock控制
	Outing_After_Announce_TX_Pools map[string][]*Transaction

	// 用来收集交易池中锁住账户的交易，由Lock_Acc_Lock控制
	Locking_TX_Pools map[string][]*Transaction

	// 用来收集ANNOUNCE后有关迁入账户的relay交易，由Coming_Lock控制
	Coming_TX_Pools map[string][]*Transaction

	// 用来按迁出账户聚合 pending 借款（payer 聚合模式）
	Borrow_Pool map[string]*TXborrow

	// 用来按迁移账户聚合“迁移完成后注入”的 payer 交易（Caravan 模式）
	Caravan_Pool map[string]*TXcaravan

	// preempt_escrow：迁移期间的 payer 交易（按账户、nonce 排序）
	MPTP map[string][]*EscrowTx
	// preempt_escrow：等待 flow gate / nonce 前缀的交易
	EscrowGated map[string][]*EscrowTx
	// preempt_escrow：某账户 gated 队列首次出现（或最近一次无法推进）的块高，用于 gap repair
	EscrowGatedSince map[string]int

	// 入口/丢弃计数：仅用于实验结果对账，不改变任何打包/执行逻辑。
	Intake IntakeStats

	// 本块产生的丢弃事件（原因/ txid / nonce），由 pbft 层落地到 S*_escrow_drop.csv。
	DropEvents []DropEvent
	// 已结算离开本分片、但交易还滞留在队列里待转发的交易（交接瞬间的窄窗口）。
	EscrowForward []*EscrowTx
	// 已上报过的 nonce gap（addr -> missing nonce），避免每块重复刷日志。
	EscrowGapSeen map[string]uint64
}

// DropEvent 是一条“交易没有进入区块”的记录。
// Reason: not_own | relay | duplicate | no_nonce | gap。
// gap 事件没有 txid（缺失的那笔从未到达），Nonce 记缺失的 nonce。
type DropEvent struct {
	TxID   int
	Sender string
	Nonce  uint64
	Nu0    uint64
	Reason string
	Height int
}

// IntakeStats 汇总本节点交易池在“进入 → 打包”之间各环节的交易计数。
// 这些计数是累计值，按块写进 S*_intake.csv，便于把
// “客户端注入 = 提交 + 委托 + 丢弃 + 残留” 对上。
type IntakeStats struct {
	Received       int64 // 进入到本节点交易池的交易（客户端注入或转发）
	ForwardedOut   int64 // 因 sender 不属本分片而被转发出去
	DroppedNotOwn  int64 // FetchTxs2Pack 的 own-shard 过滤丢弃（sender 不属本分片）
	DroppedRelay   int64 // FetchTxs2Pack 丢弃的 relay（收款方不属本分片）
	DroppedDup     int64 // 迁移账户的 payer 交易 nonce <= Nu0（已执行/重复）
	DroppedNoNonce int64 // 找不到 nonce 侧账（数据缺失）
	GatedIn        int64 // 进入 EscrowGated
	MPTPIn         int64 // 进入 MPTP
	GapRepair      int64 // gap repair 跳过的 nonce 数
}

func NewTxPool() *Tx_pool {
	pool := Tx_pool{
		Queue:                           make([]*Transaction, 0),
		Relay_Pools:                     make(map[string][]*Transaction),
		Migration_Pool:                  make(map[string]int),
		Outing_Before_Announce_TX_Pools: make(map[string][]*Transaction),
		Outing_After_Announce_TX_Pools:  make(map[string][]*Transaction),
		Locking_TX_Pools:                make(map[string][]*Transaction),
		Coming_TX_Pools:                 make(map[string][]*Transaction),
		Borrow_Pool:                     make(map[string]*TXborrow),
		Caravan_Pool:                    make(map[string]*TXcaravan),
		MPTP:                            make(map[string][]*EscrowTx),
		EscrowGated:                     make(map[string][]*EscrowTx),
		EscrowGatedSince:                make(map[string]int),
	}
	// heap.Init(&pool.Heap)

	return &pool
}

func (pool *Tx_pool) InjectTxs2Shard(sid int) {
	cnt := 0
	for {
		// inject_speed := 10000
		inject_speed := params.Config.Inject_speed
		time.Sleep(1 * time.Second)
		upperBound := utils.Min(cnt+inject_speed, len(Txs))

		pool.Lock.Lock()
		for i := cnt; i < upperBound; i++ {
			addr := hex.EncodeToString(Txs[i].Sender)
			if !params.Config.Stop_When_Migrating {

				account.Outing_Acc_After_Announce_Lock.Lock()
				if account.Outing_Acc_After_Announce[addr] {
					// Txs[i].RequestTime = time.Now().UnixMilli()
					// pool.Outing_After_Announce_TX_Pools[addr] = append(pool.Outing_After_Announce_TX_Pools[addr], Txs[i])
					account.Outing_Acc_After_Announce_Lock.Unlock()
					continue
				}
				account.Outing_Acc_After_Announce_Lock.Unlock()

				if account.Addr2Shard(addr) == sid { //发起者是本分片的，才进入交易池
					Txs[i].RequestTime = time.Now().UnixMilli()
					pool.AddTx(Txs[i])
				}

			} else if account.Addr2Shard(addr) == sid { //发起者是本分片的，才进入交易池
				Txs[i].RequestTime = time.Now().UnixMilli()
				pool.AddTx(Txs[i])

			}

		}
		// fmt.Printf("\n队列长度为: %v \n\n", len(pool.Queue))
		pool.Lock.Unlock()

		cnt = upperBound

		if cnt == len(Txs) {
			fmt.Println()
			// fmt.Println("注入100000")
			fmt.Println("注入10000")
			fmt.Println()
			break
		}
	}
}

func (pool *Tx_pool) NewInjectTxs2Shard(sid int) {
	fmt.Println("NewInjectTxs2Shard")
	for i := 0; i < len(Txs); i++ {
		addr := hex.EncodeToString(Txs[i].Sender)
		if account.Addr2Shard(addr) == sid { //发起者是本分片的，才进入交易池
			Txs[i].RequestTime = time.Now().UnixMilli()
			pool.AddTx(Txs[i])
		}
	}
	Txs = []*Transaction{}
}

func (pool *Tx_pool) AddTx(tx *Transaction) {
	pool.Queue = append(pool.Queue, tx)
	// heap.Push(&pool.Heap, tx)
}

func (pool *Tx_pool) AddTxs(txs []*Transaction) {
	pool.Lock.Lock()
	pool.Queue = append(pool.Queue, txs...)
	// for _, tx := range txs {
	// 	pool.AddTx(tx)
	// }
	pool.Lock.Unlock()
}

func (pool *Tx_pool) FetchTxs2Pack(left_count, blockNumber int) (txs []*Transaction, queueLen int) {
	config := params.Config
	fmt.Printf("left_count: %v\n", left_count)
	tx_cnt := left_count
	pool.Lock.Lock()
	txs = make([]*Transaction, 0)
	// newQueue 保存本区块未打包的普通交易（以及未被聚合/锁定的交易），保持原有顺序
	newQueue := make([]*Transaction, 0, len(pool.Queue))
	// 银行机制：记录本区块内已为迁移账户预留的信用额度，避免单个区块内超借
	reserved := make(map[string]*big.Int)
	// txs = append(txs, pool.Queue[0:tx_cnt]...)
	for _, v := range pool.Queue {
		from, to := hex.EncodeToString(v.Sender), hex.EncodeToString(v.Recipient)
		// 不是自己分片的交易,直接不管了
		account.Account2ShardLock.Lock()
		senderNotOwn := !account.AccountInOwnShard[from] && !v.IsRelay && !v.Relay_Lock
		relayTargetNotOwn := !account.AccountInOwnShard[to] && (v.IsRelay || v.Relay_Lock)
		account.Account2ShardLock.Unlock()
		if senderNotOwn || relayTargetNotOwn {
			// preempt_escrow：账户已经在本分片结算离开，但交易还滞留在队列里（交接窗口）。
			// 不丢，交给 pbft 层转发到它当前所在的分片。
			if config.IsPreemptEscrow() && senderNotOwn && !relayTargetNotOwn && account.HasSettledEscrowRecord(from) {
				if nu, ok := account.GetTxNonce(v.Id); ok {
					pool.EscrowForward = append(pool.EscrowForward, &EscrowTx{Tx: v, Nonce: nu})
					pool.Intake.ForwardedOut++
					continue
				}
			}
			// preempt_escrow：收款方正在本分片迁移/激活过程中，relay 先留在队列，
			// 下一块再试；直接丢掉等于吞掉一笔跨分片付款。
			if config.IsPreemptEscrow() && relayTargetNotOwn && account.InLocalEscrowFlow(to) {
				newQueue = append(newQueue, v)
				continue
			}
			// preempt_escrow：目的分片要到 Debit 提交才把 A 记为本分片账户，而客户端在
			// Settled 提交（EscrowActivate）时就已经改路由，两者之间约有一个区块的窗口。
			// 窗口内到达目的分片的 payer 交易必须继续往下走到 gated 缓存，不能在这里静默丢弃。
			if !(config.IsPreemptEscrow() && senderNotOwn && !relayTargetNotOwn && account.InLocalEscrowFlow(from)) {
				nu := uint64(0)
				if n, ok := account.GetTxNonce(v.Id); ok {
					nu = n
				}
				if relayTargetNotOwn {
					pool.Intake.DroppedRelay++
					pool.recordDropLocked(v, from, "relay", nu, 0, blockNumber)
				} else {
					pool.Intake.DroppedNotOwn++
					pool.recordDropLocked(v, from, "not_own", nu, 0, blockNumber)
				}
				continue
			}
		}

		// preempt_escrow：迁移账户的 payer 交易进入 MPTP；迁移后的账户走 flow gate。
		if config.IsPreemptEscrow() && !v.IsRelay && !v.Relay_Lock {
			if account.IsFlowBlocked(from) {
				if nu, ok := account.GetTxNonce(v.Id); ok {
					pool.addEscrowGatedLocked(from, &EscrowTx{Tx: v, Nonce: nu}, blockNumber)
					pool.Intake.GatedIn++
				} else {
					pool.Intake.DroppedNoNonce++
					pool.recordDropLocked(v, from, "no_nonce", 0, 0, blockNumber)
				}
				continue
			}
			if migID := account.LockedMigID(from); migID != "" {
				if rec := account.GetEscrowRecord(migID); rec != nil {
					if nu, ok := account.GetTxNonce(v.Id); ok {
						if nu > rec.Nu0 {
							pool.MPTP[from] = append(pool.MPTP[from], &EscrowTx{Tx: v, Nonce: nu})
							pool.Intake.MPTPIn++
						} else {
							// nonce 不超过锁定时已执行的最后一个 nonce：属于重复/已生效，不是丢交易
							pool.Intake.DroppedDup++
							pool.recordDropLocked(v, from, "duplicate", nu, rec.Nu0, blockNumber)
						}
					} else {
						pool.Intake.DroppedNoNonce++
						pool.recordDropLocked(v, from, "no_nonce", 0, rec.Nu0, blockNumber)
					}
				}
				continue
			}
			if account.IsFlowActive(from) {
				if nu, ok := account.GetTxNonce(v.Id); ok {
					pool.addEscrowGatedLocked(from, &EscrowTx{Tx: v, Nonce: nu}, blockNumber)
					pool.Intake.GatedIn++
				} else {
					pool.Intake.DroppedNoNonce++
					pool.recordDropLocked(v, from, "no_nonce", 0, 0, blockNumber)
				}
				continue
			}
		}

		//如果是锁机制，所有涉及到的交易都会被锁住。即便被锁的是收钱的账户，也不会先执行扣钱部分
		if config.Lock_Acc_When_Migrating {
			account.Lock_Acc_Lock.Lock()
			if account.Lock_Acc[from] && !v.IsRelay && !v.Relay_Lock {
				if v.LockTime > 0 {
					v.LockTime2 = time.Now().UnixMilli()
				} else {
					v.LockTime = time.Now().UnixMilli()
				}
				v.SenLock = true
				if config.Not_Lock_immediately && v.Sen_Suppose_on_chain == 0 {
					v.Sen_Suppose_on_chain = blockNumber
				}
				pool.Locking_TX_Pools[from] = append(pool.Locking_TX_Pools[from], v)
				account.Lock_Acc_Lock.Unlock()
				continue
			} else if account.Lock_Acc[to] {
				if !config.RelayLock {
					if v.LockTime > 0 {
						v.LockTime2 = time.Now().UnixMilli()
					} else {
						v.LockTime = time.Now().UnixMilli()
					}
					v.RecLock = true
					if config.Not_Lock_immediately && v.Rec_Suppose_on_chain == 0 {
						v.Rec_Suppose_on_chain = blockNumber
					}
					pool.Locking_TX_Pools[to] = append(pool.Locking_TX_Pools[to], v)
					account.Lock_Acc_Lock.Unlock()
					continue
				} else {
					encoded := v.Encode()
					decoded := DecodeTx(encoded)
					// txs = append(txs, decoded)
					// tx_cnt--

					if decoded.LockTime > 0 {
						decoded.LockTime2 = time.Now().UnixMilli()
					} else {
						decoded.LockTime = time.Now().UnixMilli()
					}
					decoded.RecLock = true
					if config.Not_Lock_immediately && decoded.Rec_Suppose_on_chain == 0 {
						decoded.Rec_Suppose_on_chain = blockNumber
					}
					decoded.Relay_Lock = true
					pool.Locking_TX_Pools[to] = append(pool.Locking_TX_Pools[to], decoded)
					// account.Lock_Acc_Lock.Unlock()
					// continue
				}
			}
			account.Lock_Acc_Lock.Unlock()
		} else if !config.Stop_When_Migrating { //如果是不锁机制，若sender属于被迁移账户，则锁住这些交易
			account.Outing_Acc_Before_Announce_Lock.Lock()
			isOuting := account.Outing_Acc_Before_Announce[from]
			account.Outing_Acc_Before_Announce_Lock.Unlock()

			if isOuting {
				// 银行机制：迁移账户的付款由银行代付，只要信用额度足够就聚合为借款
				admit := false
				if config.IsPreempt() {
					admit = canBorrow(from, v.Value, reserved)
					if admit {
						// 预留额度（避免同一区块内超借）
						if reserved[from] == nil {
							reserved[from] = big.NewInt(0)
						}
						reserved[from].Add(reserved[from], v.Value)

						// 聚合：不加入普通交易 txs，而是累积到 Borrow_Pool
						b := pool.Borrow_Pool[from]
						if b == nil {
							b = &TXborrow{Address: from, TotalValue: big.NewInt(0), NumTxs: 0, Credits: make([]*CreditEntry, 0)}
							pool.Borrow_Pool[from] = b
						}
						b.TotalValue.Add(b.TotalValue, v.Value)
						b.NumTxs++
						b.Credits = append(b.Credits, &CreditEntry{Recipient: v.Recipient, Value: new(big.Int).Set(v.Value)})
						continue // 不占用普通交易槽位
					}
				}
				if !admit {
					if v.LockTime > 0 {
						v.LockTime2 = time.Now().UnixMilli()
					} else {
						v.LockTime = time.Now().UnixMilli()
					}
					v.SenLock = true
					if config.Not_Lock_immediately && v.Sen_Suppose_on_chain == 0 {
						v.Sen_Suppose_on_chain = blockNumber
					}
					account.Outing_Acc_Before_Announce_Lock.Lock()
					pool.Outing_Before_Announce_TX_Pools[from] = append(pool.Outing_Before_Announce_TX_Pools[from], v)
					account.Outing_Acc_Before_Announce_Lock.Unlock()
					continue
				}
			}
		}

		// Caravan：迁移完成后注入目的分片的 payer 交易（FromMigration 标记），
		// 按发送方聚合成一笔 TXcaravan，不占用普通交易槽位。
		if config.IsCaravan() && v.FromMigration {
			cu := pool.Caravan_Pool[from]
			if cu == nil {
				cu = &TXcaravan{Address: from, TotalValue: big.NewInt(0), NumTxs: 0, Outputs: make([]*CreditEntry, 0)}
				pool.Caravan_Pool[from] = cu
			}
			cu.TotalValue.Add(cu.TotalValue, v.Value)
			cu.NumTxs++
			cu.Outputs = append(cu.Outputs, &CreditEntry{Recipient: v.Recipient, Value: new(big.Int).Set(v.Value)})
			continue // 不占用普通交易槽位
		}

		if tx_cnt > 0 {
			txs = append(txs, v)
			tx_cnt--
		} else {
			newQueue = append(newQueue, v)
		}
	}
	pool.Queue = newQueue
	queueLen = len(pool.Queue)

	// preempt_escrow：对 flow-active 账户按 nonce 连续前缀放行。
	if config.IsPreemptEscrow() {
		for addr := range pool.EscrowGated {
			if !account.IsFlowActive(addr) || account.IsFlowBlocked(addr) {
				continue
			}
			start := account.GetLastNonce(addr) + 1
			gated := pool.EscrowGated[addr]
			sort.Slice(gated, func(i, j int) bool { return gated[i].Nonce < gated[j].Nonce })
			since, ok := pool.EscrowGatedSince[addr]
			if !ok {
				since = blockNumber
				pool.EscrowGatedSince[addr] = since
			}
			j := 0
			for _, e := range gated {
				if e.Nonce < start {
					j++
					continue
				}
				if e.Nonce > start {
					if tx_cnt <= 0 {
						break
					}
					// 可信网络原型没有 ack/retry：如果某个 nonce 丢失，后面的交易会永久卡住。
					// 超过 EscrowGapRepairBlocks 后，把缺失 nonce 视为已丢失并跳过，保证实验可终止。
					repair := config.EscrowGapRepairBlocks
					if repair > 0 && blockNumber-since >= repair {
						fmt.Printf("[escrow][gap-repair] account=%s missing_nonce=%d skip_to=%d height=%d\n", addr, start, e.Nonce, blockNumber)
						account.SetLastNonce(addr, e.Nonce-1)
						pool.Intake.GapRepair += int64(e.Nonce - start)
						start = e.Nonce
					} else {
						// 严格模式（EscrowGapRepairBlocks=0）：不跳过缺口，只上报一次，等人工定位原因。
						pool.reportGapLocked(addr, start, blockNumber)
						break
					}
				}
				if e.Nonce != start || tx_cnt <= 0 {
					break
				}
				txs = append(txs, e.Tx)
				tx_cnt--
				start++
				j++
			}
			if j >= len(gated) {
				delete(pool.EscrowGated, addr)
				delete(pool.EscrowGatedSince, addr)
			} else {
				pool.EscrowGated[addr] = gated[j:]
				if j > 0 {
					pool.EscrowGatedSince[addr] = blockNumber
				}
			}
		}
	}

	pool.Lock.Unlock()
	return
}

// FetchBorrows2Pack 取走当前按账户聚合好的借款条目，并清空 Borrow_Pool。
func (pool *Tx_pool) FetchBorrows2Pack() []*TXborrow {
	pool.Lock.Lock()
	borrows := make([]*TXborrow, 0, len(pool.Borrow_Pool))
	for _, b := range pool.Borrow_Pool {
		borrows = append(borrows, b)
	}
	pool.Borrow_Pool = make(map[string]*TXborrow)
	pool.Lock.Unlock()
	return borrows
}

// FetchCaravans2Pack 取走当前按账户聚合好的 Caravan 条目，并清空 Caravan_Pool。
func (pool *Tx_pool) FetchCaravans2Pack() []*TXcaravan {
	pool.Lock.Lock()
	caravans := make([]*TXcaravan, 0, len(pool.Caravan_Pool))
	for _, c := range pool.Caravan_Pool {
		caravans = append(caravans, c)
	}
	pool.Caravan_Pool = make(map[string]*TXcaravan)
	pool.Lock.Unlock()
	return caravans
}

// canBorrow 判断迁移账户 addr 在已借款 loan、本区块预留 reserved 的基础上，
// 还能否再借 value（即是否仍然满足 loan+reserved+value <= credit）。
func canBorrow(addr string, value *big.Int, reserved map[string]*big.Int) bool {
	account.CreditLock.Lock()
	credit := account.Credit[addr]
	account.CreditLock.Unlock()
	if credit == nil {
		return false
	}

	account.LoanLock.Lock()
	loan := account.Loan[addr]
	account.LoanLock.Unlock()

	total := new(big.Int).Set(value)
	if loan != nil {
		total.Add(total, loan)
	}
	if reserved != nil && reserved[addr] != nil {
		total.Add(total, reserved[addr])
	}
	return total.Cmp(credit) <= 0
}

func (pool *Tx_pool) LockTX() {
	pool.Relaypoollock.Lock()
	pool.Lock.Lock()
	account.Account2ShardLock.Lock()
	j := 0
	for _, tx := range pool.Queue {
		from, to := hex.EncodeToString(tx.Sender), hex.EncodeToString(tx.Recipient)
		//全锁
		if params.Config.Lock_Acc_When_Migrating {
			// 如果不是relay且sender被锁，则放入锁池
			account.Lock_Acc_Lock.Lock()
			if !tx.IsRelay && !tx.Relay_Lock && account.Lock_Acc[from] {
				if tx.LockTime > 0 {
					tx.LockTime2 = time.Now().UnixMilli()
				} else {
					tx.LockTime = time.Now().UnixMilli()
				}
				tx.SenLock = true
				pool.Locking_TX_Pools[from] = append(pool.Locking_TX_Pools[from], tx)
				account.Lock_Acc_Lock.Unlock()
				continue
			}
			account.Lock_Acc_Lock.Unlock()

			account.Lock_Acc_Lock.Lock()
			if account.Lock_Acc[to] && (account.AccountInOwnShard[from] || tx.IsRelay || tx.Relay_Lock) {
				if tx.LockTime > 0 {
					tx.LockTime2 = time.Now().UnixMilli()
				} else {
					tx.LockTime = time.Now().UnixMilli()
				}
				tx.RecLock = true
				pool.Locking_TX_Pools[to] = append(pool.Locking_TX_Pools[to], tx)
				account.Lock_Acc_Lock.Unlock()
				continue
			}
			account.Lock_Acc_Lock.Unlock()
		}
		//半锁
		if !params.Config.Lock_Acc_When_Migrating {
			// 如果不是relay且sender被锁，则放入锁池
			account.Outing_Acc_Before_Announce_Lock.Lock()
			if !tx.IsRelay && !tx.Relay_Lock && account.Outing_Acc_Before_Announce[from] {
				if tx.LockTime > 0 {
					tx.LockTime2 = time.Now().UnixMilli()
				} else {
					tx.LockTime = time.Now().UnixMilli()
				}
				pool.Outing_Before_Announce_TX_Pools[from] = append(pool.Outing_Before_Announce_TX_Pools[from], tx)
				account.Outing_Acc_Before_Announce_Lock.Unlock()
				continue
			}
			account.Outing_Acc_Before_Announce_Lock.Unlock()
		}

		// 否则，留在被锁交易中
		pool.Queue[j] = tx
		j++
	}
	pool.Queue = pool.Queue[:j]

	account.Account2ShardLock.Unlock()
	pool.Lock.Unlock()
	pool.Relaypoollock.Unlock()
}

// func TxPoolDeepCopy(dst *[]*Transaction, src *[]*Transaction) error {
// 	if tmp, err := json.Marshal(src); err != nil {
// 		return err
// 	} else {
// 		err = json.Unmarshal(tmp, dst)
// 		return err
// 	}
// }

func TxPoolDeepCopy(dst *[]*Transaction, src []*Transaction) {
	for _, tx := range src {
		*dst = append(*dst, &Transaction{
			Sender:               tx.Sender,
			Recipient:            tx.Recipient,
			TxHash:               tx.TxHash,
			Id:                   tx.Id,
			Success:              tx.Success,
			IsRelay:              tx.IsRelay,
			SenLock:              tx.SenLock,
			RecLock:              tx.RecLock,
			Value:                new(big.Int).Set(tx.Value),
			RequestTime:          tx.RequestTime,
			Second_RequestTime:   tx.Second_RequestTime,
			CommitTime:           tx.CommitTime,
			LockTime:             tx.LockTime,
			UnlockTime:           tx.UnlockTime,
			LockTime2:            tx.LockTime2,
			UnlockTime2:          tx.UnlockTime2,
			HalfLock:             tx.HalfLock,
			Sen_Suppose_on_chain: tx.Sen_Suppose_on_chain,
			Rec_Suppose_on_chain: tx.Rec_Suppose_on_chain,
			Relay_Lock:           tx.Relay_Lock,
			FromMigration:        tx.FromMigration,
		})
	}
}

// BuildEscrowBatches 在源分片打包前，按账户构造 nonce 连续前缀批次。
// 只从 MPTP 取走被纳入的交易；未纳入的继续等待。记录状态由区块执行阶段更新。
func (pool *Tx_pool) BuildEscrowBatches(height int) []*EscrowBatch {
	if !params.Config.IsPreemptEscrow() {
		return nil
	}
	pool.Lock.Lock()
	defer pool.Lock.Unlock()

	addrs := make([]string, 0, len(pool.MPTP))
	for addr := range pool.MPTP {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)

	out := make([]*EscrowBatch, 0)
	for _, addr := range addrs {
		migID := account.LockedMigID(addr)
		if migID == "" {
			continue
		}
		rec := account.GetEscrowRecord(migID)
		if rec == nil || rec.Status != params.EscrowLocked {
			continue
		}
		list := pool.MPTP[addr]
		if len(list) == 0 {
			continue
		}
		sort.Slice(list, func(i, j int) bool { return list[i].Nonce < list[j].Nonce })

		expect := rec.NuCons + 1
		totalCons := new(big.Int).Set(rec.TotalCons)
		included := make([]*EscrowTx, 0)
		includedIDs := make(map[int]bool)
		for _, e := range list {
			// MaxEscrowBatchTxs <= 0 表示不限制，只受 nonce 连续前缀与 solvency 约束（D2）。
			if params.Config.MaxEscrowBatchTxs > 0 && len(included) >= params.Config.MaxEscrowBatchTxs {
				break
			}
			if e.Nonce < expect {
				continue // 过期/重复 nonce，跳过
			}
			if e.Nonce != expect {
				break // nonce gap：后面的都不能授权
			}
			if e.Tx.Value == nil {
				break
			}
			candidate := new(big.Int).Add(totalCons, e.Tx.Value)
			if candidate.Cmp(rec.Credit) > 0 {
				break // 超额度：本笔及之后全部等待
			}
			included = append(included, e)
			includedIDs[e.Tx.Id] = true
			totalCons = candidate
			expect++
		}
		if len(included) == 0 {
			continue
		}

		value := new(big.Int).Sub(totalCons, rec.TotalCons)
		b := &EscrowBatch{
			MigID:      migID,
			SeqStart:   rec.SeqCons + 1,
			SeqEnd:     rec.SeqCons + uint64(len(included)),
			NonceStart: rec.NuCons + 1,
			NonceEnd:   rec.NuCons + uint64(len(included)),
			Txs:        included,
			TotalValue: value,
			SrcHeight:  height,
		}
		b.BatchHash = b.Hash()
		out = append(out, b)

		remain := make([]*EscrowTx, 0, len(list)-len(included))
		for _, e := range list {
			if !includedIDs[e.Tx.Id] {
				remain = append(remain, e)
			}
		}
		pool.MPTP[addr] = remain
	}
	return out
}

// TakeMPTP 取走某账户当前所有未授权 payer 交易（用于 Quiesce 时转交 S_own）。
func (pool *Tx_pool) TakeMPTP(addr string) []*EscrowTx {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	out := pool.MPTP[addr]
	delete(pool.MPTP, addr)
	sort.Slice(out, func(i, j int) bool { return out[i].Nonce < out[j].Nonce })
	return out
}

// PeekMPTP 返回某账户当前未授权交易的排序副本（不移除）。
func (pool *Tx_pool) PeekMPTP(addr string) []*EscrowTx {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	out := append([]*EscrowTx(nil), pool.MPTP[addr]...)
	sort.Slice(out, func(i, j int) bool { return out[i].Nonce < out[j].Nonce })
	return out
}

// MoveMPTPToGated 在 Expired 且账户留在源分片时，把未授权交易按 nonce 重新放回 flow gate。
func (pool *Tx_pool) MoveMPTPToGated(addr string) {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	out := pool.MPTP[addr]
	delete(pool.MPTP, addr)
	if len(out) == 0 {
		return
	}
	pool.EscrowGated[addr] = append(pool.EscrowGated[addr], out...)
}

// MergeGatedToMPTP 在账户开始新一轮迁移时，把上一轮遗留的 gated 交易重新纳入 MPTP。
// nonce <= nu0 的交易视为已执行，直接丢弃。
func (pool *Tx_pool) MergeGatedToMPTP(addr string, nu0 uint64) {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	gated := pool.EscrowGated[addr]
	delete(pool.EscrowGated, addr)
	delete(pool.EscrowGatedSince, addr)
	for _, e := range gated {
		if e.Nonce > nu0 {
			pool.MPTP[addr] = append(pool.MPTP[addr], e)
		}
	}
}

// AddGatedTxs 把从源分片转交过来的未授权交易放入 flow gate。
func (pool *Tx_pool) AddGatedTxs(addr string, txs []*EscrowTx) {
	if len(txs) == 0 {
		return
	}
	pool.Lock.Lock()
	pool.EscrowGated[addr] = append(pool.EscrowGated[addr], txs...)
	pool.Lock.Unlock()
}

func (pool *Tx_pool) addEscrowGatedLocked(addr string, e *EscrowTx, height int) {
	if _, ok := pool.EscrowGatedSince[addr]; !ok {
		pool.EscrowGatedSince[addr] = height
	}
	pool.EscrowGated[addr] = append(pool.EscrowGated[addr], e)
}

// EscrowQueueStats 返回 MPTP / flow gate 中的交易数，用于新 schema 的日志。
func (pool *Tx_pool) EscrowQueueStats() (mptp, gated int) {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	for _, txs := range pool.MPTP {
		mptp += len(txs)
	}
	for _, txs := range pool.EscrowGated {
		gated += len(txs)
	}
	return
}

func (pool *Tx_pool) QueueLen() int {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	return len(pool.Queue)
}

// TakeQueuedFor 取走普通队列中 sender == addr 的交易（连同 nonce），
// 用于迁移结算时把还排在源分片队列里的交易一并交给 S_own。
// 这些交易若留在源分片，会在下一个区块被 own-shard 过滤当成“别人的交易”丢掉。
func (pool *Tx_pool) TakeQueuedFor(addr string) []*EscrowTx {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()

	keep := make([]*Transaction, 0, len(pool.Queue))
	out := make([]*EscrowTx, 0)
	for _, tx := range pool.Queue {
		if tx == nil {
			continue
		}
		if hex.EncodeToString(tx.Sender) == addr {
			if nu, ok := account.GetTxNonce(tx.Id); ok {
				out = append(out, &EscrowTx{Tx: tx, Nonce: nu})
				continue
			}
		}
		keep = append(keep, tx)
	}
	if len(out) != 0 {
		pool.Queue = keep
	}
	return out
}

// recordDropLocked 记一条丢弃事件（调用方需持有 pool.Lock）。
func (pool *Tx_pool) recordDropLocked(tx *Transaction, sender, reason string, nonce, nu0 uint64, height int) {
	txID := -1
	if tx != nil {
		txID = tx.Id
	}
	pool.DropEvents = append(pool.DropEvents, DropEvent{
		TxID: txID, Sender: sender, Nonce: nonce, Nu0: nu0, Reason: reason, Height: height,
	})
}

// reportGapLocked 上报一个 nonce 缺口（同一账户同一缺口只报一次）。
func (pool *Tx_pool) reportGapLocked(addr string, missing uint64, height int) {
	if pool.EscrowGapSeen == nil {
		pool.EscrowGapSeen = make(map[string]uint64)
	}
	if seen, ok := pool.EscrowGapSeen[addr]; ok && seen == missing {
		return
	}
	pool.EscrowGapSeen[addr] = missing
	pool.DropEvents = append(pool.DropEvents, DropEvent{
		TxID: -1, Sender: addr, Nonce: missing, Reason: "gap", Height: height,
	})
}

// TakeEscrowForward 取走交接窗口里需要转发的交易。
func (pool *Tx_pool) TakeEscrowForward() []*EscrowTx {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	if len(pool.EscrowForward) == 0 {
		return nil
	}
	out := pool.EscrowForward
	pool.EscrowForward = nil
	return out
}

// TakeDropEvents 取走本块累积的丢弃事件。
func (pool *Tx_pool) TakeDropEvents() []DropEvent {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	if len(pool.DropEvents) == 0 {
		return nil
	}
	out := pool.DropEvents
	pool.DropEvents = nil
	return out
}

// IntakeSnapshot 返回当前累计的入口/丢弃计数。
func (pool *Tx_pool) IntakeSnapshot() IntakeStats {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()
	return pool.Intake
}

// QueueDump 导出仍留在各队列里的交易（txid / sender / nonce），
// 用于运行结束时对账：“注入 = 提交 + 委托 + 丢弃 + 残留”里的残留部分。
// 返回 pool 名、交易 id、sender hex、nonce（普通队列没有 nonce，记 -1）。
type QueueDumpEntry struct {
	Pool   string
	TxID   int
	Sender string
	Nonce  int64
}

func (pool *Tx_pool) QueueDump() []QueueDumpEntry {
	pool.Lock.Lock()
	defer pool.Lock.Unlock()

	out := make([]QueueDumpEntry, 0, len(pool.Queue))
	for _, tx := range pool.Queue {
		if tx == nil {
			continue
		}
		out = append(out, QueueDumpEntry{Pool: "normal", TxID: tx.Id, Sender: hex.EncodeToString(tx.Sender), Nonce: -1})
	}
	for addr, list := range pool.MPTP {
		for _, e := range list {
			if e == nil || e.Tx == nil {
				continue
			}
			out = append(out, QueueDumpEntry{Pool: "mptp", TxID: e.Tx.Id, Sender: addr, Nonce: int64(e.Nonce)})
		}
	}
	for addr, list := range pool.EscrowGated {
		for _, e := range list {
			if e == nil || e.Tx == nil {
				continue
			}
			out = append(out, QueueDumpEntry{Pool: "gated", TxID: e.Tx.Id, Sender: addr, Nonce: int64(e.Nonce)})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Pool != out[j].Pool {
			return out[i].Pool < out[j].Pool
		}
		return out[i].TxID < out[j].TxID
	})
	return out
}
