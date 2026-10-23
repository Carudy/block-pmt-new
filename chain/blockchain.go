package chain

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"math/big"
	"os"
	"sort"
	"strings"
	"time"

	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/storage"

	// "blockEmulator/trie"
	"blockEmulator/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

var (
	blocktimelog, queuelenlog *csv.Writer
)

type BlockChain struct {
	ChainConfig *params.ChainConfig // Chain configuration

	CurrentBlock *core.Block // Current head of the block chain

	Storage *storage.Storage
	db      ethdb.Database // the leveldb database to store in the disk, for status trie
	Triedb  *trie.Database // the trie database which helps to store the status trie

	// StatusTrie *trie.Trie

	Tx_pool *core.Tx_pool

	TXmig1_pool *core.TXmig1_pool

	TXmig2_pool *core.TXmig2_pool

	TXann_pool *core.TXann_pool

	TXns_pool *core.TXns_pool

	TXsettle_pool *core.TXsettle_pool

	// preempt_escrow 专用池/出站队列（旧 schema 不使用）
	EscrowQuiesceReq_pool *core.EscrowQuiesceReqPool
	EscrowQuiesce_pool    *core.EscrowQuiescePool
	EscrowSettle_pool     *core.EscrowSettlePool
	EscrowDebit_pool      *core.EscrowDebitPool

	EscrowOutDebits      []*core.EscrowDebit
	EscrowOutActivations []*core.EscrowActivate
	EscrowOutDeliveries  []*core.EscrowDelivery
	// 交付事件（含 txid 列表），由 pbft 层落盘到 S*_escrow_tx.csv。
	EscrowDeliverLogs []*core.EscrowDeliverLog

	// 源分片保留已授权批次，直到结算；用于 lost-batch 回退/重放。仅新 schema 使用。
	EscrowBatchArchive map[string]map[uint64]*core.EscrowBatch
}

func NewBlockChain(chainConfig *params.ChainConfig) (*BlockChain, error) {
	var err error
	if chainConfig.NodeID == "N0" {
		csvFile, err := os.Create("./log/" + chainConfig.ShardID + "_blocktime.csv")
		if err != nil {
			log.Panic(err)
		}
		// defer csvFile.Close()
		blocktimelog = csv.NewWriter(csvFile)
		blocktimelog.Write([]string{"height", "block", "tx", "mig1", "mig2", "ann", "ns"})
		blocktimelog.Flush()

		csvFile1, err := os.Create("./log/" + chainConfig.ShardID + "_queueLen.csv")
		if err != nil {
			log.Panic(err)
		}
		queuelenlog = csv.NewWriter(csvFile1)
		queuelenlog.Write([]string{"block", "queueLen"})
		queuelenlog.Flush()
	}

	fmt.Printf("%v\n", chainConfig)
	bc := &BlockChain{
		ChainConfig: chainConfig,
		Storage:     storage.NewStorage(chainConfig),
		Tx_pool:     core.NewTxPool(),

		//不停止时要用的
		TXmig1_pool:   core.NewTXmig1Pool(),
		TXmig2_pool:   core.NewTXmig2Pool(),
		TXann_pool:    core.NewTXannPool(),
		TXns_pool:     core.NewTXnsPool(),
		TXsettle_pool: core.NewTXsettlePool(),

		EscrowQuiesceReq_pool: core.NewEscrowQuiesceReqPool(),
		EscrowQuiesce_pool:    core.NewEscrowQuiescePool(),
		EscrowSettle_pool:     core.NewEscrowSettlePool(),
		EscrowDebit_pool:      core.NewEscrowDebitPool(),
		EscrowBatchArchive:    make(map[string]map[uint64]*core.EscrowBatch),
	}
	//filepath
	fp := "./record/triedb/" + chainConfig.ShardID + "_" + chainConfig.NodeID
	// cache=0 和 handles=1 只要小于16，都会被设为16
	bc.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic("cannot get the level db")
	}

	blockHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		if err.Error() == "newestBlockHash is not found" {
			genesisBlock := bc.NewGenesisBlock()
			bc.AddGenesisBlock(genesisBlock)
			return bc, nil
		}
		log.Panic()
	}

	// there is a blockchain in the storage
	block, err := bc.Storage.GetBlock(blockHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = block
	// stateTree, err := bc.Storage.GetStatusTree()
	// if err != nil {
	// 	log.Panic()
	// }
	// bc.StatusTrie = stateTree
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.Triedb = triedb
	// check the existence of the trie database
	_, err = trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), triedb)
	if err != nil {
		log.Panic()
	}
	fmt.Println("The status trie can be built")

	return bc, nil
}

// 本地化存储，修改内存、存储至硬盘。返回要迁出账户的余额
func (bc *BlockChain) AddBlock(block *core.Block) map[string]*big.Int {
	iobegin := time.Now().UnixMilli()
	bc.Storage.AddBlock(block)
	var outbalance map[string]*big.Int

	if !bc.ChainConfig.Stop_When_Migrating {
		if bc.ChainConfig.IsPreemptEscrow() {
			// 新协议：TXmig2 只创建目的分片的 Prepared 记录，不改账户归属。
			for _, v := range block.TXmig2s {
				if v == nil || v.Txmig1 == nil {
					continue
				}
				if v.MigID == "" {
					continue
				}
				if account.GetPendingRecord(v.MigID) != nil {
					continue
				}
				account.PutPendingRecord(&account.PendingRecord{
					MigID:          v.MigID,
					Account:        v.Txmig1.Address,
					SourceShard:    v.Txmig1.FromshardID,
					DestShard:      v.Txmig1.ToshardID,
					NextSeq:        1,
					Status:         params.EscrowPrepared,
					PreparedHeight: block.Header.Number,
					Pending:        make(map[string]*big.Int),
					Materialized:   make(map[string]bool),
					Spent:          big.NewInt(0),
				})
			}
		} else {
			// 将迁入账户映射到本分片
			for _, v := range block.TXmig2s {
				account.Account2ShardLock.Lock()
				account.Account2Shard[v.Address] = params.ShardTable[params.Config.ShardID]
				account.AccountInOwnShard[v.Address] = true
				account.Account2ShardLock.Unlock()
			}
		}
	}

	updatetree := time.Now().UnixMilli()
	stateroothash, outbalance := bc.getUpdatedTreeOfState(1, block.Header.Number, block.Transactions, block.TXmig1s, block.TXmig2s, block.Anns, block.NSs, block.TXborrows, block.TXsettles, block.TXcaravans, block.Escrow)
	if !bytes.Equal(block.Header.StateRoot, stateroothash) {
		log.Panicf("二者不等，Ins长度为%v\n", len(block.TXmig2s))
	}
	fmt.Printf("更新树花时间为: %v\n", time.Now().UnixMilli()-updatetree)

	if !bc.ChainConfig.Stop_When_Migrating && params.Config.NodeID != "N0" && !bc.ChainConfig.IsPreemptEscrow() {
		// 将删除账户从本分片映射删去，并设为对应分片
		for _, v := range block.Anns {
			account.Account2ShardLock.Lock()
			account.Account2Shard[v.Address] = v.ToshardID
			delete(account.AccountInOwnShard, v.Address)
			account.Account2ShardLock.Unlock()

			if bc.ChainConfig.Lock_Acc_When_Migrating {
				account.Lock_Acc_Lock.Lock()
				delete(account.Lock_Acc, v.Address)
				account.Lock_Acc_Lock.Unlock()
			}
		}
	}

	// 若要锁账户，就把账户锁住
	if params.Config.Lock_Acc_When_Migrating && params.Config.NodeID != "N0" {
		account.Lock_Acc_Lock.Lock()
		for _, v := range block.TXmig1s {
			account.Lock_Acc[v.Address] = true
			// if params.Config.NodeID == "N0" {
			// 	bc.Tx_pool.Locking_TX_Pools[k] = make([]*core.Transaction, 0)
			// }
		}
		account.Lock_Acc_Lock.Unlock()
	}

	if params.Config.Fail && params.Config.Fail_Time+1 == block.Header.Number && params.Config.Lock_Acc_When_Migrating {
		account.Lock_Acc_Lock.Lock()
		account.Lock_Acc["489338d5e8d42e8c923d1f47361d979503d4ad68"] = false
		account.Lock_Acc_Lock.Unlock()
	}

	bc.CurrentBlock = block

	fmt.Printf("写数据库IO花时间为: %v\n", time.Now().UnixMilli()-iobegin)

	// relay
	// if bc.ChainConfig.NodeID == "N0" {
	// 	bc.genRelayTxs(block)
	// }
	return outbalance
}

// func (bc *BlockChain) genRelayTxs(block *core.Block) {
// 	for _, tx := range block.Transactions {
// 		shardID := account.Addr2Shard(hex.EncodeToString(tx.Recipient))
// 		if shardID != params.ShardTable[bc.ChainConfig.ShardID] {
// 			bc.Tx_pool.AddRelayTx(tx, params.ShardTableInt2Str[shardID])
// 		}
// 	}
// }

// buildEscrowBlockData 收集 preempt_escrow 在本区块要提交的系统操作。
// 只在 IsPreemptEscrow() 时调用。
func (bc *BlockChain) buildEscrowBlockData(height int) *core.EscrowBlockData {
	d := &core.EscrowBlockData{}
	self := params.ShardTable[bc.ChainConfig.ShardID]

	// 源分片：为请求 quiesce 的账户生成响应；到达 h_lock+Δ 的记录进入 Expired。
	seen := make(map[string]bool)
	for _, req := range bc.EscrowQuiesceReq_pool.Fetch() {
		rec := account.GetEscrowRecord(req.MigID)
		if rec == nil || rec.SourceShard != self || seen[req.MigID] {
			continue
		}
		if rec.Status == params.EscrowSettled || rec.Status == params.EscrowExpired {
			continue
		}
		d.Quiesces = append(d.Quiesces, &core.EscrowQuiesce{
			MigID:  rec.MigID,
			SStar:  rec.SeqCons,
			NuStar: rec.NuCons,
			Status: params.EscrowQuiescing,
		})
		seen[rec.MigID] = true
		account.SetEscrowStatus(rec.MigID, params.EscrowQuiescing)
	}

	timeout := bc.ChainConfig.MigrationTimeoutBlocks
	if timeout > 0 {
		for _, rec := range account.AllEscrowRecords() {
			if rec.SourceShard != self || seen[rec.MigID] {
				continue
			}
			if (rec.Status == params.EscrowLocked || rec.Status == params.EscrowQuiescing) && height >= rec.HLock+timeout {
				d.Quiesces = append(d.Quiesces, &core.EscrowQuiesce{
					MigID:  rec.MigID,
					SStar:  rec.SeqCons,
					NuStar: rec.NuCons,
					Status: params.EscrowExpired,
				})
				seen[rec.MigID] = true
				account.SetEscrowStatus(rec.MigID, params.EscrowExpired)
			}
		}
	}

	d.Batches = bc.Tx_pool.BuildEscrowBatches(height)
	d.Settles = bc.EscrowSettle_pool.Fetch()
	d.Debits = bc.EscrowDebit_pool.Fetch()
	return d
}

func (bc *BlockChain) GenerateBlock(id int) *core.Block {
	quota := params.Config.MaxMigSize
	mig1s := []*core.TXmig1{}
	mig2s := []*core.TXmig2{}
	anns := []*core.TXann{}
	// deleted := []*core.Delete{}
	nss := []*core.TXns{}
	settles := []*core.TXsettle{}
	if !params.Config.Stop_When_Migrating {
		//得到新的映射（只有要改变的

		if params.Config.Algorithm || params.Config.Pressure {
			mig1s = bc.TXmig1_pool.FetchTXmig1s2Pack2()
		} else {
			mig1s, quota = bc.TXmig1_pool.FetchTXmig1s2Pack()
		}

		// bc.Tx_pool.Lock.Lock()
		// new_migration := make(map[string]int)
		// tmp, _ := json.Marshal(&bc.Tx_pool.Migration_Pool)
		// _ = json.Unmarshal(tmp, &new_migration)
		// bc.Tx_pool.Migration_Pool = make(map[string]int)
		// bc.Tx_pool.Lock.Unlock()

		// account.Account2ShardLock.Lock()
		// //找到要迁移出去的
		// for addr, shard := range new_migration {
		// 	if account.AccountInOwnShard[addr] && shard != params.ShardTable[params.Config.ShardID] {
		// 		out[addr] = shard
		// 	}
		// }
		// account.Account2ShardLock.Unlock()

		if !params.Config.Bu_Tong_Shi_Jian {
			if params.Config.Algorithm || params.Config.Pressure {
				mig2s = bc.TXmig2_pool.FetchTXmig2s2Pack2()
			} else {
				//将全部迁入请求包进来
				mig2s, quota = bc.TXmig2_pool.FetchTXmig2s2Pack(quota)
			}
		} else if id == params.Config.Bu_Tong_Shi_Jian_Jian_Ge {
			//将全部迁入请求包进来
			mig2s, _ = bc.TXmig2_pool.FetchTXmig2s2Pack(10000000)
		}

		// //将全部该删除账户包进来
		// if params.Config.Algorithm || params.Config.Pressure {
		// 	deleted = bc.Delete_pool.FetchDels2Pack2()
		// } else {
		// 	deleted = bc.Delete_pool.FetchDels2Pack()
		// }

		//将全部announce包进来
		if params.Config.Algorithm || params.Config.Pressure {
			anns = bc.TXann_pool.FetchTXanns2Pack2()
		} else {
			anns, quota = bc.TXann_pool.FetchTXanns2Pack(quota)
		}

		//将全部加（减）钱请求包进来
		if params.Config.Algorithm || params.Config.Pressure {
			nss = bc.TXns_pool.FetchTXnss2Pack2()
		} else {
			nss, quota = bc.TXns_pool.FetchTXnss2Pack(quota)
		}

		// 银行机制：由本区块内的还款（TXns.Debt）生成 settle-out，并打包收到的 settle-in
		if params.Config.IsPreempt() {
			settleOut := make(map[int]*big.Int)
			for _, ns := range nss {
				if ns.Debt == nil || ns.Debt.Sign() == 0 {
					continue
				}
				if ns.Txann == nil || ns.Txann.Txmig2 == nil || ns.Txann.Txmig2.Txmig1 == nil {
					continue
				}
				fromShard := ns.Txann.Txmig2.Txmig1.FromshardID
				if settleOut[fromShard] == nil {
					settleOut[fromShard] = big.NewInt(0)
				}
				settleOut[fromShard].Add(settleOut[fromShard], ns.Debt)
			}
			selfShardID := params.ShardTable[params.Config.ShardID]
			for fromShard, amount := range settleOut {
				settles = append(settles, &core.TXsettle{FromShardID: selfShardID, ToShardID: fromShard, Amount: amount})
			}
			// 收到的 settle-in（其他分片发来的结算）
			settles = append(settles, bc.TXsettle_pool.FetchTXsettles2Pack()...)
		}
	}

	// preempt_escrow：先构建本块的 batch/quiesce/settle/debit 系统操作。
	// 每个 EscrowBatch 占 1 个 MaxBlockSize 名额（D2），所以必须在 FetchTxs2Pack
	// 之前构建，才能把名额准确预留出来。代价：本块新进 MPTP 的交易下一块才授权。
	var escrowData *core.EscrowBlockData
	escrowSlots := 0
	if params.Config.IsPreemptEscrow() {
		escrowData = bc.buildEscrowBlockData(bc.CurrentBlock.Header.Number + 1)
		escrowSlots = len(escrowData.Batches)
		for _, b := range escrowData.Batches {
			if b == nil {
				continue
			}
			m := bc.EscrowBatchArchive[b.MigID]
			if m == nil {
				m = make(map[uint64]*core.EscrowBatch)
				bc.EscrowBatchArchive[b.MigID] = m
			}
			m[b.SeqStart] = b
		}
	}

	txs := []*core.Transaction{}
	queueLen := 0
	if params.Config.Cross_Chain && id != 1 {
		if params.Config.ShardID == "S0" && (params.Config.Fail || (!params.Config.Fail && !params.Config.Lock_Acc_When_Migrating)) {
			txs, queueLen = bc.Tx_pool.FetchTxs2Pack(params.Config.MaxBlockSize, bc.CurrentBlock.Header.Number+1)
		} else if params.Config.Lock_Acc_When_Migrating && params.Config.ShardID == "S1" && !params.Config.Fail {
			account.Account2ShardLock.Lock()
			if account.AccountInOwnShard["489338d5e8d42e8c923d1f47361d979503d4ad68"] {
				account.Account2ShardLock.Unlock()
				txs, queueLen = bc.Tx_pool.FetchTxs2Pack(params.Config.MaxBlockSize, bc.CurrentBlock.Header.Number+1)
			} else {
				account.Account2ShardLock.Unlock()
			}
		}
	} else if (!params.Config.Bu_Tong_Bi_Li && !params.Config.Bu_Tong_Shi_Jian && !params.Config.Fail && !params.Config.Cross_Chain) || id != 1 {
		left := params.Config.MaxBlockSize - len(mig1s) - len(mig2s) - len(anns) - len(nss) - escrowSlots
		if left < 0 {
			left = 0
		}
		txs, queueLen = bc.Tx_pool.FetchTxs2Pack(left, bc.CurrentBlock.Header.Number+1)
	}

	// 银行机制：取走聚合好的借款条目（payer 聚合模式）
	borrows := []*core.TXborrow{}
	if params.Config.IsPreempt() {
		borrows = bc.Tx_pool.FetchBorrows2Pack()
	}

	// Caravan：取走打包前按迁移账户聚合好的“单账户→多输出”条目
	caravans := []*core.TXcaravan{}
	if params.Config.IsCaravan() {
		caravans = bc.Tx_pool.FetchCaravans2Pack()
	}

	blockHeader := &core.BlockHeader{
		ParentHash: bc.CurrentBlock.Hash,
		Number:     bc.CurrentBlock.Header.Number + 1,
		Time:       uint64(time.Now().Unix()),
	}

	block := core.NewBlock(blockHeader, txs, mig1s, mig2s, anns, nss, borrows, settles, caravans)
	block.Escrow = escrowData

	//普通交易树
	block.Header.TxHash = GetTxTreeRoot(txs)

	//迁移交易树
	block.Header.MigHash = GetMigTreeRootWithEscrow(mig1s, mig2s, anns, nss, borrows, settles, caravans, escrowData)

	//0 代表不更新到磁盘
	block.Header.StateRoot, _ = bc.getUpdatedTreeOfState(0, blockHeader.Number, txs, mig1s, mig2s, anns, nss, borrows, settles, caravans, escrowData)

	block.Hash = block.GetHash()

	if !params.Config.Stop_When_Migrating {
		if !params.Config.Lock_Acc_When_Migrating {
			account.Outing_Acc_Before_Announce_Lock.Lock()
			for _, lockedtxs := range bc.Tx_pool.Outing_Before_Announce_TX_Pools {
				queueLen += len(lockedtxs)
			}
			account.Outing_Acc_Before_Announce_Lock.Unlock()
		} else {
			account.Lock_Acc_Lock.Lock()
			for _, lockedtxs := range bc.Tx_pool.Locking_TX_Pools {
				queueLen += len(lockedtxs)
			}
			account.Lock_Acc_Lock.Unlock()
		}

	}
	s := fmt.Sprintf("%v %v", blockHeader.Number, queueLen)
	queuelenlog.Write(strings.Split(s, " "))
	queuelenlog.Flush()

	return block
}

// 输出更新后的状态树 以及 要迁出的账户的余额
func (bc *BlockChain) getUpdatedTreeOfState(commit int, height int, txs []*core.Transaction, mig1s []*core.TXmig1, mig2s []*core.TXmig2, anns []*core.TXann, nss []*core.TXns, borrows []*core.TXborrow, settles []*core.TXsettle, caravans []*core.TXcaravan, escrow *core.EscrowBlockData) ([]byte, map[string]*big.Int) {
	// build trie from the triedb (in disk)
	start_execute := time.Now().UnixMicro()
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.Triedb)
	if err != nil {
		log.Panic(err)
	}

	// 银行机制仅在“不停且不锁（fine-tuned lock）”路径下启用
	bankEnabled := bc.ChainConfig.IsPreempt() && !bc.ChainConfig.Stop_When_Migrating && !bc.ChainConfig.Lock_Acc_When_Migrating
	selfShardID := params.ShardTable[bc.ChainConfig.ShardID]

	mig2start := time.Now().UnixMicro()
	if !bc.ChainConfig.Stop_When_Migrating {
		// 将迁入账户加到状态树
		for _, v := range mig2s {
			hex_address, _ := hex.DecodeString(v.Address)
			s_state_enc := st.Get(hex_address)
			if s_state_enc == nil {
				log.Panic()
			}
			account_state := account.DecodeAccountState(s_state_enc)
			account_state.Balance.Set(v.Value)
			account_state.Migrate = -1
			account_state.Location = params.ShardTable[bc.ChainConfig.ShardID]
			st.Update(hex_address, account_state.Encode())
		}
	}
	mig2time := time.Now().UnixMicro() - mig2start

	// stateTree := bc.preExecute(txs)
	outbalance := make(map[string]*big.Int)

	txstart := time.Now().UnixMicro()
	for _, tx := range txs {
		// 确保发送地址属于此分片，即此交易不是其它分片发来的relay交易
		// if account.Addr2Shard(hex.EncodeToString(tx.Sender)) == params.ShardTable[bc.ChainConfig.ShardID] {
		if !tx.IsRelay && !tx.Relay_Lock {
			s_state_enc := st.Get(tx.Sender)
			if s_state_enc == nil {
				fmt.Printf("sender属于该分片吗：%v\n", account.AccountInOwnShard[hex.EncodeToString(tx.Sender)])
				fmt.Printf("sender地址为：%v\n", hex.EncodeToString(tx.Sender))
				fmt.Printf("sender属于分片：%v\n", account.Account2Shard[hex.EncodeToString(tx.Sender)])
				fmt.Printf("rec地址为：%v\n", hex.EncodeToString(tx.Recipient))
				fmt.Printf("rec属于分片：%v\n", account.Account2Shard[hex.EncodeToString(tx.Recipient)])
				log.Panic()
			}
			sender_state := account.DecodeAccountState(s_state_enc)
			// 银行机制：迁移账户的付款由银行代付，不扣账户余额；只记录贷款
			if bankEnabled && sender_state.Migrate != -1 {
				bank_state_enc := st.Get(account.BankAddrBytes(selfShardID))
				if bank_state_enc == nil {
					log.Panic()
				}
				bank_state := account.DecodeAccountState(bank_state_enc)
				bank_state.Balance.Sub(bank_state.Balance, tx.Value)
				st.Update(account.BankAddrBytes(selfShardID), bank_state.Encode())

				if commit == 1 {
					account.LoanLock.Lock()
					addr := hex.EncodeToString(tx.Sender)
					if account.Loan[addr] == nil {
						account.Loan[addr] = big.NewInt(0)
					}
					account.Loan[addr].Add(account.Loan[addr], tx.Value)
					account.LoanLock.Unlock()
				}
			} else {
				sender_state.Balance.Sub(sender_state.Balance, tx.Value)
				st.Update(tx.Sender, sender_state.Encode())
			}

			// preempt_escrow：记录每个账户最后执行的 nonce（仅 commit==1 写内存）。
			if bc.ChainConfig.IsPreemptEscrow() && commit == 1 && !tx.IsRelay && !tx.Relay_Lock {
				if nu, ok := account.GetTxNonce(tx.Id); ok {
					account.BumpLastNonce(hex.EncodeToString(tx.Sender), nu)
				}
			}
		}

		r_state_enc := st.Get(tx.Recipient)
		if r_state_enc == nil {
			fmt.Printf("rec属于该分片吗：%v\n", account.AccountInOwnShard[hex.EncodeToString(tx.Recipient)])
			fmt.Printf("rec地址为：%v\n", hex.EncodeToString(tx.Recipient))
			fmt.Printf("rec属于分片：%v\n", account.Account2Shard[hex.EncodeToString(tx.Recipient)])
			fmt.Printf("txid：%v\n", tx.Id)
			log.Panic()
		}
		account_state := account.DecodeAccountState(r_state_enc)
		// 接收地址不在此分片，不对该状态进行修改
		if account_state.Location != selfShardID {
			continue
			// 接收者为锁定账户，不对该状态进行修改
		} else if account_state.Migrate != -1 && params.Config.Lock_Acc_When_Migrating {
			continue
		}

		account_state.Balance.Add(account_state.Balance, tx.Value)
		if commit == 1 && account_state.Migrate != -1 {
			tx.HalfLock = true
			// 银行机制：迁移账户收到入账时，其信用额度同步增加
			if bankEnabled {
				account.CreditLock.Lock()
				to := hex.EncodeToString(tx.Recipient)
				if account.Credit[to] == nil {
					account.Credit[to] = big.NewInt(0)
				}
				account.Credit[to].Add(account.Credit[to], tx.Value)
				account.CreditLock.Unlock()
			}
		}
		st.Update(tx.Recipient, account_state.Encode())

	}
	txtime := time.Now().UnixMicro() - txstart

	mig1time := int64(0)
	anntime := int64(0)
	nstime := int64(0)
	if !bc.ChainConfig.Stop_When_Migrating {
		// 状态树中记录要迁出的账户的去向
		mig1start := time.Now().UnixMicro()
		for _, v := range mig1s {
			hex_address, _ := hex.DecodeString(v.Address)
			encoded := st.Get(hex_address)
			if encoded == nil {
				log.Panic()
			}
			account_state := account.DecodeAccountState(encoded)

			if bc.ChainConfig.IsPreemptEscrow() {
				// 迁移准备：把真实余额 n 转入 escrow，源账户余额置 0。
				// 只有 commit==1 才写内存迁移记录；trie 变更在 commit==0/1 都执行。
				if account.GetEscrowRecordByAccount(v.Address) != nil {
					continue
				}
				tn := new(big.Int).Set(account_state.Balance)
				migID := account.EscrowMigID(v.Address, v.FromshardID, v.ToshardID, height, 0)
				escAddr := account.EscrowAddr(migID)
				escState := &account.AccountState{Balance: tn, Migrate: -1, Location: selfShardID}
				st.Update(escAddr, escState.Encode())

				account_state.Balance = big.NewInt(0)
				account_state.Migrate = v.ToshardID
				st.Update(hex_address, account_state.Encode())
				outbalance[v.Address] = big.NewInt(0)

				if commit == 1 {
					rec := &account.EscrowRecord{
						MigID:       migID,
						Account:     v.Address,
						SourceShard: v.FromshardID,
						DestShard:   v.ToshardID,
						Credit:      new(big.Int).Set(tn),
						SeqCons:     0,
						TotalCons:   big.NewInt(0),
						Nu0:         account.GetLastNonce(v.Address),
						NuCons:      account.GetLastNonce(v.Address),
						HLock:       height,
						Status:      params.EscrowLocked,
						SOwn:        v.FromshardID,
						Spent:       big.NewInt(0),
					}
					account.PutEscrowRecord(rec)
					// 上一轮 flow 遗留的 gated 交易纳入本轮 MPTP，避免孤立的 nonce 队列。
					bc.Tx_pool.MergeGatedToMPTP(rec.Account, rec.Nu0)
					account.SetFlowActive(rec.Account, false)
				}
				continue
			}

			account_state.Migrate = v.ToshardID
			outbalance[v.Address] = new(big.Int).Set(account_state.Balance)
			st.Update(hex_address, account_state.Encode())
		}
		mig1time = time.Now().UnixMicro() - mig1start

		// 状态树中记录announce的账户
		annstart := time.Now().UnixMicro()
		for _, v := range anns {
			hex_address, _ := hex.DecodeString(v.Address)
			s_state_enc := st.Get(hex_address)
			if s_state_enc == nil {
				log.Panic()
			}
			account_state := account.DecodeAccountState(s_state_enc)
			account_state.Migrate = -1
			account_state.Location = v.ToshardID
			st.Update(hex_address, account_state.Encode())
			// encoded := st.Get(hex_address)
			// account_state := &account.AccountState{
			// 	Migrate:  -1,
			// 	Location: v.ToshardID,
			// }
			// if encoded != nil {
			// 	account_state = account.DecodeAccountState(encoded)
			// 	account_state.Migrate = -1
			// 	account_state.Location = v.ToshardID
			// }

			// st.Update(hex_address, account_state.Encode())
		}
		anntime = time.Now().UnixMicro() - annstart

		// // 将迁入账户加到状态树
		// for _, v := range in1s {
		// 	hex_address, _ := hex.DecodeString(v.Address)

		// 	account_state := &account.AccountState{
		// 		Balance: v.Value,
		// 		Migrate: -1,
		// 	}
		// 	stateTree.Put(hex_address, account_state.Encode())
		// }

		// // 从状态树中删除指定账户
		// for _, v := range deletes {
		// 	hex_address, _ := hex.DecodeString(v.Address)
		// 	stateTree.Delete(hex_address)
		// }

		// 将加减钱改到状态树
		nsstart := time.Now().UnixMicro()
		for _, v := range nss {
			hex_address, _ := hex.DecodeString(v.Address)

			encoded := st.Get(hex_address)
			if encoded == nil {
				log.Panic()
			}
			account_state := account.DecodeAccountState(encoded)
			account_state.Balance.Add(account_state.Balance, v.Change)

			// 银行机制：迁移账户在目标分片还款（扣账户余额，还给银行）
			if v.Debt != nil && v.Debt.Sign() != 0 {
				account_state.Balance.Sub(account_state.Balance, v.Debt)

				bankAddr := account.BankAddrBytes(selfShardID)
				bank_state_enc := st.Get(bankAddr)
				if bank_state_enc == nil {
					log.Panic()
				}
				bank_state := account.DecodeAccountState(bank_state_enc)
				bank_state.Balance.Add(bank_state.Balance, v.Debt)
				st.Update(bankAddr, bank_state.Encode())
			}

			st.Update(hex_address, account_state.Encode())
		}
		nstime = time.Now().UnixMicro() - nsstart

		// 银行机制：处理聚合的借款（源分片：银行代付、记贷款、给本地收款人入账）
		for _, v := range borrows {
			if v == nil || v.TotalValue == nil || v.TotalValue.Sign() == 0 {
				continue
			}
			// 银行扣款
			bankAddr := account.BankAddrBytes(selfShardID)
			bank_state_enc := st.Get(bankAddr)
			if bank_state_enc == nil {
				log.Panic()
			}
			bank_state := account.DecodeAccountState(bank_state_enc)
			bank_state.Balance.Sub(bank_state.Balance, v.TotalValue)
			st.Update(bankAddr, bank_state.Encode())

			// 记录贷款
			if commit == 1 {
				account.LoanLock.Lock()
				if account.Loan[v.Address] == nil {
					account.Loan[v.Address] = big.NewInt(0)
				}
				account.Loan[v.Address].Add(account.Loan[v.Address], v.TotalValue)
				account.LoanLock.Unlock()
			}

			// 给本地收款人入账（远程收款人在 commit1 里 relay）
			for _, c := range v.Credits {
				if c == nil || c.Value == nil {
					continue
				}
				r_state_enc := st.Get(c.Recipient)
				if r_state_enc == nil {
					continue
				}
				r_state := account.DecodeAccountState(r_state_enc)
				if r_state.Location != selfShardID {
					continue
				}
				r_state.Balance.Add(r_state.Balance, c.Value)
				st.Update(c.Recipient, r_state.Encode())
			}
		}

		// Caravan：执行“单账户→多输出”聚合交易：对发送方做一次合并扣减，
		// 对每个本地收款人各入账一次（远程收款人在 commit1 里 relay）。
		for _, v := range caravans {
			if v == nil || v.TotalValue == nil || v.TotalValue.Sign() == 0 {
				continue
			}
			hex_address, _ := hex.DecodeString(v.Address)
			s_state_enc := st.Get(hex_address)
			if s_state_enc == nil {
				continue
			}
			s_state := account.DecodeAccountState(s_state_enc)
			s_state.Balance.Sub(s_state.Balance, v.TotalValue)
			st.Update(hex_address, s_state.Encode())

			for _, o := range v.Outputs {
				if o == nil || o.Value == nil {
					continue
				}
				r_state_enc := st.Get(o.Recipient)
				if r_state_enc == nil {
					continue
				}
				r_state := account.DecodeAccountState(r_state_enc)
				if r_state.Location != selfShardID {
					continue
				}
				r_state.Balance.Add(r_state.Balance, o.Value)
				st.Update(o.Recipient, r_state.Encode())
			}
		}

		// 银行机制：应用跨分片银行结算（本分片是 From 则扣款，是 To 则收款）
		for _, v := range settles {
			if v == nil || v.Amount == nil || (v.FromShardID != selfShardID && v.ToShardID != selfShardID) {
				continue
			}
			bankAddr := account.BankAddrBytes(selfShardID)
			bank_state_enc := st.Get(bankAddr)
			if bank_state_enc == nil {
				log.Panic()
			}
			bank_state := account.DecodeAccountState(bank_state_enc)
			if v.FromShardID == selfShardID {
				bank_state.Balance.Sub(bank_state.Balance, v.Amount)
			} else {
				bank_state.Balance.Add(bank_state.Balance, v.Amount)
			}
			st.Update(bankAddr, bank_state.Encode())
		}
	}

	// preempt_escrow：执行 batch / quiesce / settle / debit。
	// 注意：trie 变更在 commit==0/1 都执行；内存状态/outbox 只在 commit==1 写。
	if bc.ChainConfig.IsPreemptEscrow() && escrow != nil {
		// 1) batch 只更新源分片迁移记录的已授权游标，不移动资金。
		for _, b := range escrow.Batches {
			if commit != 1 || b == nil {
				continue
			}
			rec := account.GetEscrowRecord(b.MigID)
			if rec == nil {
				continue
			}
			if b.SeqEnd > rec.SeqCons {
				rec.SeqCons = b.SeqEnd
			}
			if rec.TotalCons == nil {
				rec.TotalCons = big.NewInt(0)
			}
			rec.TotalCons.Add(rec.TotalCons, b.TotalValue)
			if b.NonceEnd > rec.NuCons {
				rec.NuCons = b.NonceEnd
			}
		}

		// 2) settle：源分片从 escrow 扣 spent + refund，并生成发往目的分片的 Debit。
		for _, s := range escrow.Settles {
			if s == nil || s.Spent == nil {
				continue
			}
			rec := account.GetEscrowRecord(s.MigID)
			if rec == nil || rec.SourceShard != selfShardID {
				continue
			}
			if rec.Status == params.EscrowSettled || rec.Status == params.EscrowExpired {
				continue
			}
			if s.Spent.Sign() < 0 || s.Spent.Cmp(rec.TotalCons) > 0 || s.Spent.Cmp(rec.Credit) > 0 {
				log.Panicf("preempt_escrow: invalid spent %v for %s (total_cons=%v credit=%v)", s.Spent, s.MigID, rec.TotalCons, rec.Credit)
			}

			status := params.EscrowSettled
			owner := rec.DestShard
			if rec.Status == params.EscrowExpired {
				status = params.EscrowExpired
				owner = rec.SourceShard
			}
			refund := new(big.Int).Sub(rec.Credit, s.Spent)

			escAddr := account.EscrowAddr(rec.MigID)
			escEnc := st.Get(escAddr)
			if escEnc == nil {
				log.Panicf("preempt_escrow: missing escrow account for %s", rec.MigID)
			}
			escState := account.DecodeAccountState(escEnc)
			escState.Balance.Sub(escState.Balance, s.Spent)
			escState.Balance.Sub(escState.Balance, refund)
			if commit == 1 && escState.Balance.Sign() != 0 {
				fmt.Printf("[escrow][invariant] escrow residual mig=%s balance=%v\n", rec.MigID, escState.Balance)
			}
			st.Update(escAddr, escState.Encode())

			aAddr, _ := hex.DecodeString(rec.Account)
			aEnc := st.Get(aAddr)
			if aEnc == nil {
				log.Panicf("preempt_escrow: missing source account %s", rec.Account)
			}
			aState := account.DecodeAccountState(aEnc)
			flow := new(big.Int).Set(aState.Balance)

			debit := &core.EscrowDebit{
				MigID:  rec.MigID,
				Spent:  new(big.Int).Set(s.Spent),
				Refund: new(big.Int).Set(refund),
				Flow:   new(big.Int).Set(flow),
				Nu0:    rec.Nu0,
				NuDst:  s.NuDst,
				Status: status,
				SStar:  s.SStar,
				SM:     s.SM,
			}

			if status == params.EscrowSettled {
				aState.Balance = big.NewInt(0)
				aState.Migrate = -1
				aState.Location = owner
				if commit == 1 {
					rec.NuDst = s.NuDst
					rec.SOwn = owner
					rec.Status = status
					rec.Spent = new(big.Int).Set(s.Spent)
					account.SetEscrowStatus(rec.MigID, status)
					account.SetLastNonce(rec.Account, maxU64(rec.Nu0, s.NuDst))
					account.DeleteEscrowLock(rec.Account)
					account.SetFlowActive(rec.Account, false)
					debit.Remaining = bc.escrowRemainingTxs(rec, s.SM, true)
					// 保留重放集：目的分片发现 nonce 缺口时可以按 nonce 回查。
					core.ArchiveEscrowRemaining(rec.MigID, debit.Remaining)

					account.Account2ShardLock.Lock()
					account.Account2Shard[rec.Account] = owner
					delete(account.AccountInOwnShard, rec.Account)
					account.Account2ShardLock.Unlock()

					bc.EscrowOutDebits = append(bc.EscrowOutDebits, debit)
					bc.EscrowOutActivations = append(bc.EscrowOutActivations, &core.EscrowActivate{
						MigID: rec.MigID, Address: rec.Account, SourceShard: rec.SourceShard, DestShard: rec.DestShard, NuStart: maxU64(rec.Nu0, s.NuDst),
					})
					account.MarkTerminal(rec.MigID)
				}
			} else {
				// Expired：A 留在源分片，退款回 A；未授权交易回到 flow gate。
				aState.Balance.Add(aState.Balance, refund)
				aState.Migrate = -1
				aState.Location = owner
				debit.Refund = big.NewInt(0)
				debit.Flow = big.NewInt(0)
				debit.Status = params.EscrowExpired
				if commit == 1 {
					rec.NuDst = s.NuDst
					rec.SOwn = owner
					rec.Status = status
					rec.Spent = new(big.Int).Set(s.Spent)
					account.SetEscrowStatus(rec.MigID, status)
					account.SetLastNonce(rec.Account, maxU64(rec.Nu0, s.NuDst))
					account.DeleteEscrowLock(rec.Account)
					account.SetFlowActive(rec.Account, true)
					rem := bc.escrowRemainingTxs(rec, s.SM, false)
					bc.Tx_pool.AddGatedTxs(rec.Account, rem)
					account.Account2ShardLock.Lock()
					account.Account2Shard[rec.Account] = owner
					account.AccountInOwnShard[rec.Account] = true
					account.Account2ShardLock.Unlock()
					bc.EscrowOutDebits = append(bc.EscrowOutDebits, debit)
					account.MarkTerminal(rec.MigID)
				}
			}
			st.Update(aAddr, aState.Encode())
		}

		// 3) debit：目的分片接收 spent，激活/删除 A，交付 pending。
		for _, d := range escrow.Debits {
			if d == nil || d.Spent == nil {
				continue
			}
			p := account.GetPendingRecord(d.MigID)
			if p == nil || p.DestShard != selfShardID || p.Delivered {
				continue
			}

			settleAddr := account.SettlementAddr(d.MigID)
			settleEnc := st.Get(settleAddr)
			var settleState *account.AccountState
			if settleEnc == nil {
				settleState = &account.AccountState{Balance: big.NewInt(0), Migrate: -1, Location: selfShardID}
			} else {
				settleState = account.DecodeAccountState(settleEnc)
			}
			settleState.Balance.Add(settleState.Balance, d.Spent)

			aAddr, _ := hex.DecodeString(p.Account)
			if d.Status == params.EscrowSettled {
				aEnc := st.Get(aAddr)
				var aState *account.AccountState
				if aEnc == nil {
					aState = &account.AccountState{Balance: big.NewInt(0), Migrate: -1, Location: selfShardID}
				} else {
					aState = account.DecodeAccountState(aEnc)
				}
				if d.Refund != nil {
					aState.Balance.Add(aState.Balance, d.Refund)
				}
				if d.Flow != nil {
					aState.Balance.Add(aState.Balance, d.Flow)
				}
				aState.Migrate = -1
				aState.Location = selfShardID
				st.Update(aAddr, aState.Encode())

				if commit == 1 {
					account.Account2ShardLock.Lock()
					account.Account2Shard[p.Account] = selfShardID
					account.AccountInOwnShard[p.Account] = true
					account.Account2ShardLock.Unlock()
					account.SetLastNonce(p.Account, maxU64(d.Nu0, d.NuDst))
					account.SetFlowActive(p.Account, true)
					for _, e := range d.Remaining {
						if e != nil && e.Tx != nil {
							account.SetTxNonce(e.Tx.Id, e.Nonce)
						}
					}
					bc.Tx_pool.AddGatedTxs(p.Account, d.Remaining)
				}
			} else {
				// Expired：目的分片的 prepared 副本必须撤销。
				if st.Get(aAddr) != nil {
					st.Delete(aAddr)
				}
			}

			var pending map[string]*big.Int
			var pendingTxIDs map[string][]int
			if commit == 1 {
				pending = account.TakePending(d.MigID)
				pendingTxIDs = account.TakePendingTxIDs(d.MigID)
			} else {
				pending = account.SnapshotPending(d.MigID)
			}
			for recipientHex, value := range pending {
				if value == nil || value.Sign() == 0 {
					continue
				}
				rAddr, _ := hex.DecodeString(recipientHex)
				rEnc := st.Get(rAddr)
				if rEnc != nil {
					rState := account.DecodeAccountState(rEnc)
					if rState.Location == selfShardID {
						rState.Balance.Add(rState.Balance, value)
						st.Update(rAddr, rState.Encode())
						settleState.Balance.Sub(settleState.Balance, value)
						if commit == 1 {
							bc.EscrowDeliverLogs = append(bc.EscrowDeliverLogs, &core.EscrowDeliverLog{
								MigID: d.MigID, Recipient: recipientHex, TxIDs: pendingTxIDs[recipientHex],
								Shard: selfShardID, Height: height,
							})
						}
						continue
					}
				}
				if commit == 1 {
					bc.EscrowOutDeliveries = append(bc.EscrowOutDeliveries, &core.EscrowDelivery{
						MigID: d.MigID, Recipient: rAddr, Value: new(big.Int).Set(value), IdemKey: d.MigID + ":" + recipientHex,
						TxIDs: pendingTxIDs[recipientHex], SettleHeight: height,
					})
				}
				settleState.Balance.Sub(settleState.Balance, value)
			}
			st.Update(settleAddr, settleState.Encode())

			if commit == 1 {
				if settleState.Balance.Sign() != 0 {
					fmt.Printf("[escrow][invariant] settlement residual mig=%s balance=%v\n", d.MigID, settleState.Balance)
				}
				account.FinalizePending(d.MigID, d.Status == params.EscrowExpired)
				account.MarkTerminal(d.MigID)
			}
		}
	}

	st_hash_bytes := st.Hash().Bytes()
	//只是生成区块
	if commit == 0 {
		return st_hash_bytes, outbalance
	}

	//区块上链，要写到磁盘
	// commit the memory trie to the database in the disk
	rt, ns := st.Commit(false)
	//空块
	if ns == nil {
		blocktime := time.Now().UnixMicro() - start_execute
		if commit == 1 && params.Config.NodeID == "N0" {
			s := fmt.Sprintf("%v %v %v %v %v %v %v", height, blocktime, txtime, mig1time, mig2time, anntime, nstime)
			blocktimelog.Write(strings.Split(s, " "))
			blocktimelog.Flush()
		}
		return st_hash_bytes, outbalance
	}
	err = bc.Triedb.Update(trie.NewWithNodeSet(ns))
	if err != nil {
		log.Panic()
	}
	err = bc.Triedb.Commit(rt, false)
	if err != nil {
		log.Panic(err)
	}
	blocktime := time.Now().UnixMicro() - start_execute
	if commit == 1 && params.Config.NodeID == "N0" {
		s := fmt.Sprintf("%v %v %v %v %v %v %v", height, blocktime, txtime, mig1time, mig2time, anntime, nstime)
		blocktimelog.Write(strings.Split(s, " "))
		blocktimelog.Flush()
	}
	return rt.Bytes(), outbalance
}

func (bc *BlockChain) NewGenesisBlock() *core.Block {
	blockHeader := &core.BlockHeader{
		Number: 0,
		Time:   uint64(time.Date(2022, 05, 28, 17, 11, 0, 0, time.Local).Unix()),
	}

	txs := make([]*core.Transaction, 0)
	mig1s := make([]*core.TXmig1, 0)
	mig2s := make([]*core.TXmig2, 0)
	anns := make([]*core.TXann, 0)
	nss := make([]*core.TXns, 0)
	borrows := make([]*core.TXborrow, 0)
	settles := make([]*core.TXsettle, 0)
	caravans := make([]*core.TXcaravan, 0)
	block := core.NewBlock(blockHeader, txs, mig1s, mig2s, anns, nss, borrows, settles, caravans)

	// build a new trie database by db
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.Triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	block.Header.StateRoot = bc.genesisStateTree(statusTrie.Hash().Bytes())
	block.Header.TxHash = GetTxTreeRoot(txs)
	block.Hash = block.GetHash()

	return block
}

func (bc *BlockChain) AddGenesisBlock(block *core.Block) {
	bc.Storage.AddBlock(block)

	// 重新从数据库中获取最新内容
	newestBlockHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curBlock, err := bc.Storage.GetBlock(newestBlockHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curBlock
}

// 创世区块中初始化几个账户
// func genesisStateTree() *trie.Trie {
// 	trie := trie.NewTrie()
// 	for i := 0; i < len(params.Init_addrs); i++ {
// 		address := params.Init_addrs[i]
// 		if utils.Addr2Shard(address) != params.ShardTable[params.Config.ShardID] {
// 			continue
// 		}
// 		value, _ := strconv.ParseFloat(params.Init_balance, 64)
// 		accountState := &account.AccountState{
// 			Balance: value,
// 			Migrate: -1,
// 		}
// 		hex_address, _ := hex.DecodeString(address)
// 		trie.Put(hex_address, accountState.Encode())
// 	}
// 	return trie
// }

// 创世区块中初始化几个账户
func (bc *BlockChain) genesisStateTree(stateroot []byte) []byte {
	// build trie from the triedb (in disk)
	st, err := trie.New(trie.TrieID(common.BytesToHash(stateroot)), bc.Triedb)
	if err != nil {
		log.Panic(err)
	}

	for i := 0; i < len(params.Init_addrs); i++ {
		address := params.Init_addrs[i]
		// if utils.Addr2Shard(address) != params.ShardTable[params.Config.ShardID] {
		// 	continue
		// }
		value := new(big.Int)
		var ok bool
		if bc.ChainConfig.IsPreemptEscrow() && bc.ChainConfig.EscrowInitBalance != nil {
			value.Set(bc.ChainConfig.EscrowInitBalance)
			ok = true
		} else {
			value, ok = value.SetString(params.Init_balance, 10)
		}
		if !ok {
			log.Panic()
		}
		accountState := &account.AccountState{
			Balance:  value,
			Migrate:  -1,
			Location: utils.Addr2Shard(address),
		}
		hex_address, _ := hex.DecodeString(address)
		st.Update(hex_address, accountState.Encode())
	}

	// 银行机制：为本分片初始化一个银行账户（仅当开启银行机制时）
	if bc.ChainConfig.IsPreempt() {
		selfShardID := params.ShardTable[bc.ChainConfig.ShardID]
		bankAddr := account.BankAddrBytes(selfShardID)
		bankState := &account.AccountState{
			Balance:  new(big.Int).Set(bc.ChainConfig.BankInitialBalance),
			Migrate:  -1,
			Location: selfShardID,
		}
		st.Update(bankAddr, bankState.Encode())
	}
	// commit the memory trie to the database in the disk
	rt, ns := st.Commit(false)
	err = bc.Triedb.Update(trie.NewWithNodeSet(ns))
	if err != nil {
		log.Panic()
	}
	err = bc.Triedb.Commit(rt, false)
	if err != nil {
		log.Panic(err)
	}
	return rt.Bytes()
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, tx.Encode())
	}
	return transactionTree.Hash().Bytes()
}

// Get the transaction root, this root can be used to check the transactions
func GetMigTreeRoot(mig1s []*core.TXmig1, mig2s []*core.TXmig2, anns []*core.TXann, nss []*core.TXns, borrows []*core.TXborrow, settles []*core.TXsettle, caravans []*core.TXcaravan) []byte {
	return GetMigTreeRootWithEscrow(mig1s, mig2s, anns, nss, borrows, settles, caravans, nil)
}

func GetMigTreeRootWithEscrow(mig1s []*core.TXmig1, mig2s []*core.TXmig2, anns []*core.TXann, nss []*core.TXns, borrows []*core.TXborrow, settles []*core.TXsettle, caravans []*core.TXcaravan, escrow *core.EscrowBlockData) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range mig1s {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range mig2s {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range anns {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range nss {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range borrows {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range settles {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	for _, tx := range caravans {
		transactionTree.Update(tx.Hash(), tx.Encode())
	}
	if escrow != nil {
		for _, b := range escrow.Batches {
			transactionTree.Update(b.Hash(), b.Encode())
		}
		for _, q := range escrow.Quiesces {
			transactionTree.Update(q.Hash(), q.Encode())
		}
		for _, s := range escrow.Settles {
			transactionTree.Update(s.Hash(), s.Encode())
		}
		for _, d := range escrow.Debits {
			transactionTree.Update(d.Hash(), d.Encode())
		}
	}
	return transactionTree.Hash().Bytes()
}

func (bc *BlockChain) IsBlockValid(block *core.Block) bool {
	// todo

	return true
}

func maxU64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

// escrowRemainingTxs 返回该迁移中尚未被目的分片记录的原始交易：
// MPTP 中未授权的 + 已授权但 seq > SM（未记录）的批次。
// sweepQueued=true 时另外取走普通队列里仍属于该账户的交易：结算后源分片不再拥有
// 该账户，这些交易若留在队列里会被 own-shard 过滤静默丢掉。
// 这些交易保留原 nonce，可用于退款后重放。
func (bc *BlockChain) escrowRemainingTxs(rec *account.EscrowRecord, sm uint64, sweepQueued bool) []*core.EscrowTx {
	out := bc.Tx_pool.TakeMPTP(rec.Account)
	if sweepQueued {
		out = append(out, bc.Tx_pool.TakeQueuedFor(rec.Account)...)
	}
	if m := bc.EscrowBatchArchive[rec.MigID]; m != nil {
		for seq, b := range m {
			if b != nil && seq > sm {
				out = append(out, b.Txs...)
			}
		}
		delete(bc.EscrowBatchArchive, rec.MigID)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Nonce < out[j].Nonce })
	return out
}
