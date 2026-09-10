package core

import (
	"blockEmulator/account"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/hex"
	"fmt"
	"math/big"
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
		if (!account.AccountInOwnShard[from] && !v.IsRelay && !v.Relay_Lock) || (!account.AccountInOwnShard[to] && (v.IsRelay || v.Relay_Lock)) {
			account.Account2ShardLock.Unlock()
			continue
		}
		account.Account2ShardLock.Unlock()

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
