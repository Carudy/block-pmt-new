package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"bytes"
	"encoding/gob"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/trie"
)

// 一个分片一个节点，没有pbft，固定时间出块
func (p *Pbft) propose1() {
	pbftType := "Block"
	block := p.Node.CurChain.GenerateBlock(p.sequenceID)
	encoded_block := block.Encode()
	p.commit1(encoded_block, pbftType)
}

// 一个分片一个节点，没有pbft，固定时间出块
func (p *Pbft) commit1(content []byte, pbftType string) {
	// 如果是区块
	if pbftType == "Block" {
		block := core.DecodeBlock(content)
		pbftend := time.Now().UnixMilli()
		// 本地化存储。修改内存、存储至硬盘
		outbalance := p.Node.CurChain.AddBlock(block)
		fmt.Printf("编号为 %d 的区块已加入本地区块链！", p.sequenceID)
		curBlock := p.Node.CurChain.CurrentBlock
		fmt.Printf("curBlock: \n")
		curBlock.PrintBlock()

		if p.Node.nodeID == "N0" {
			if !params.Config.Stop_When_Migrating && !params.Config.Lock_Acc_When_Migrating {
				account.BalanceBeforeOutLock.Lock()
				for k, v := range outbalance {
					account.BalanceBeforeOut[k] = new(big.Int)
					account.BalanceBeforeOut[k].Set(v)
				}
				account.BalanceBeforeOutLock.Unlock()

				// 银行机制：为刚进入迁移的账户初始化信用额度与贷款
				if params.Config.EnableBankMechanism {
					account.CreditLock.Lock()
					account.LoanLock.Lock()
					for k, v := range outbalance {
						account.Credit[k] = new(big.Int).Set(v)
						account.Loan[k] = big.NewInt(0)
					}
					account.LoanLock.Unlock()
					account.CreditLock.Unlock()
				}
			}

			tx_total := len(block.Transactions)
			// 银行机制：NSs 里每个 TXns 聚合了若干笔原始 payer 交易，这里统计原始笔数
			ns_tx_total := 0
			for _, ns := range block.NSs {
				ns_tx_total += ns.NumTxs
			}
			// ns 统计列：银行机制下统计原始 payer 交易笔数，否则维持“新状态账户数”
			ns_count := len(block.NSs)
			if params.Config.EnableBankMechanism {
				ns_count = ns_tx_total
			}
			relayCount := 0
			//已上链交易集
			commit_txs := []*core.Transaction{}
			//要发送relay交易集合
			relaytxs := make([]*core.Transaction, 0)
			for _, v := range block.Transactions {
				_, toid := account.Addr2Shard(hex.EncodeToString(v.Sender)), account.Addr2Shard(hex.EncodeToString(v.Recipient))
				//若交易接收者属于本分片才加入已上链交易集
				if params.Config.Lock_Acc_When_Migrating {
					account.Lock_Acc_Lock.Lock()
					if params.ShardTable[params.Config.ShardID] == toid {
						if account.Lock_Acc[hex.EncodeToString(v.Recipient)] {
							v.IsRelay = true
							// if v.LockTime > 0 {
							// 	v.LockTime2 = pbftend
							// } else {
							// 	v.LockTime = pbftend
							// }
							// v.RecLock = true
							// if params.Config.RelayLock {
							// 	v.Relay_Lock = true
							// }
							// p.Node.CurChain.Tx_pool.Locking_TX_Pools[hex.EncodeToString(v.Recipient)] = append(p.Node.CurChain.Tx_pool.Locking_TX_Pools[hex.EncodeToString(v.Recipient)], v)
						} else {
							commit_txs = append(commit_txs, v)
							v.CommitTime = pbftend
							var s string
							s = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v", hex.EncodeToString(v.Sender), hex.EncodeToString(v.Recipient), v.Id, block.Header.Number, v.RequestTime-InitTime*1000, v.Second_RequestTime-InitTime*1000, v.CommitTime-v.RequestTime, v.LockTime-InitTime*1000, v.UnlockTime-InitTime*1000, v.LockTime2-InitTime*1000, v.UnlockTime2-InitTime*1000, v.Success, v.SenLock, v.RecLock, v.HalfLock, v.Sen_Suppose_on_chain, v.Rec_Suppose_on_chain, v.Relay_Lock)
							txlog.Write(strings.Split(s, " "))
							// latency := pbftend - v.RequestTime
							// if fromid != toid {
							// 	ctxs := fmt.Sprintf("%v %v", v.Id, latency)
							// 	ctxlog.Write(strings.Split(ctxs, " "))
							// }
						}
					}
					account.Lock_Acc_Lock.Unlock()
				} else {
					if params.ShardTable[params.Config.ShardID] == toid {
						commit_txs = append(commit_txs, v)
						v.CommitTime = pbftend
						var s string
						s = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v %v", hex.EncodeToString(v.Sender), hex.EncodeToString(v.Recipient), v.Id, block.Header.Number, v.RequestTime-InitTime*1000, v.Second_RequestTime-InitTime*1000, v.CommitTime-v.RequestTime, v.LockTime-InitTime*1000, v.UnlockTime-InitTime*1000, v.LockTime2-InitTime*1000, v.UnlockTime2-InitTime*1000, v.Success, v.SenLock, v.RecLock, v.HalfLock, v.Sen_Suppose_on_chain, v.Rec_Suppose_on_chain, v.Relay_Lock)
						txlog.Write(strings.Split(s, " "))
						// latency := pbftend - v.RequestTime
						// if fromid != toid {
						// 	ctxs := fmt.Sprintf("%v %v", v.Id, latency)
						// 	ctxlog.Write(strings.Split(ctxs, " "))
						// }
					}
				}

				// var s string
				// s = fmt.Sprintf("%v %v %v %v %v %v", hex.EncodeToString(v.Sender), hex.EncodeToString(v.Recipient), v.Id, block.Header.Number, v.RequestTime-InitTime, pbftend-v.RequestTime)
				// txlog.Write(strings.Split(s, " "))
				if v.IsRelay {
					relayCount++
				} else if toid != params.ShardTable[params.Config.ShardID] {
					relayCount++
					v.IsRelay = true
					relaytxs = append(relaytxs, v)
				}
			}

			//记录接收账户的时间
			for _, mig := range block.TXmig2s {
				acc := mig.Address
				s := fmt.Sprintf("%v %v", acc, pbftend-InitTime*1000)
				txmig2log.Write(strings.Split(s, " "))
			}

			//记录TXmig1的时间
			if !params.Config.Stop_When_Migrating {
				for _, v := range block.TXmig1s {
					// ([]string{"acc", "source", "target", "blockHeight", "CommitTime"})
					s := fmt.Sprintf("%v %v %v %v %v", v.Address, params.Config.ShardID, v.ToshardID, block.Header.Number, pbftend-InitTime*1000)
					txmig1log.Write(strings.Split(s, " "))
				}
			}

			mig_begin := time.Now().UnixMicro()
			// 若要锁账户，就把账户锁住
			if params.Config.Lock_Acc_When_Migrating {
				account.Lock_Acc_Lock.Lock()
				for _, v := range block.TXmig1s {
					account.Lock_Acc[v.Address] = true
					p.Node.CurChain.Tx_pool.Locking_TX_Pools[v.Address] = make([]*core.Transaction, 0)
				}
				account.Lock_Acc_Lock.Unlock()
			}

			// 不停不锁，要把账户半锁
			if !params.Config.Stop_When_Migrating && !params.Config.Lock_Acc_When_Migrating {
				account.Outing_Acc_Before_Announce_Lock.Lock()
				for _, v := range block.TXmig1s {
					account.Outing_Acc_Before_Announce[v.Address] = true
					p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools[v.Address] = make([]*core.Transaction, 0)
				}
				account.Outing_Acc_Before_Announce_Lock.Unlock()
			}

			// 超时
			if params.Config.Fail && params.Config.Fail_Time == p.sequenceID && params.Config.ShardID == "S0" {
				if params.Config.Lock_Acc_When_Migrating { //锁
					account.Lock_Acc_Lock.Lock()
					account.Lock_Acc["489338d5e8d42e8c923d1f47361d979503d4ad68"] = false
					for _, v := range p.Node.CurChain.Tx_pool.Locking_TX_Pools["489338d5e8d42e8c923d1f47361d979503d4ad68"] {
						v.UnlockTime = pbftend
						v.Success = false
					}
					p.Node.CurChain.Tx_pool.AddTxs(p.Node.CurChain.Tx_pool.Locking_TX_Pools["489338d5e8d42e8c923d1f47361d979503d4ad68"])
					delete(p.Node.CurChain.Tx_pool.Locking_TX_Pools, "489338d5e8d42e8c923d1f47361d979503d4ad68")
					account.Lock_Acc_Lock.Unlock()
				} else { // 不停不锁
					account.Outing_Acc_Before_Announce_Lock.Lock()
					account.Outing_Acc_Before_Announce["489338d5e8d42e8c923d1f47361d979503d4ad68"] = false
					for _, v := range p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools["489338d5e8d42e8c923d1f47361d979503d4ad68"] {
						v.UnlockTime = pbftend
						v.Success = false
					}
					p.Node.CurChain.Tx_pool.AddTxs(p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools["489338d5e8d42e8c923d1f47361d979503d4ad68"])
					delete(p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools, "489338d5e8d42e8c923d1f47361d979503d4ad68")
					account.Outing_Acc_Before_Announce_Lock.Unlock()

					// 银行机制：迁移失败，清理该账户的贷款/信用记录，避免债务被带到目标分片。
					// 注意：完整的 trie 层面轧差（扣账户余额还给银行）留待后续阶段实现。
					if params.Config.EnableBankMechanism {
						account.LoanLock.Lock()
						delete(account.Loan, "489338d5e8d42e8c923d1f47361d979503d4ad68")
						account.LoanLock.Unlock()
						account.CreditLock.Lock()
						delete(account.Credit, "489338d5e8d42e8c923d1f47361d979503d4ad68")
						account.CreditLock.Unlock()
					}
				}
			}

			// build trie from the triedb (in disk)
			st1, err := trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), p.Node.CurChain.Triedb)
			if err != nil {
				log.Panic(err)
			}
			st2, err := trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), p.Node.CurChain.Triedb)
			if err != nil {
				log.Panic(err)
			}
			st3, err := trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), p.Node.CurChain.Triedb)
			if err != nil {
				log.Panic(err)
			}
			st4, err := trie.New(trie.TrieID(common.BytesToHash(block.Header.StateRoot)), p.Node.CurChain.Triedb)
			if err != nil {
				log.Panic(err)
			}

			// use a memory trie database to do this, instead of disk database
			triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
			migTree := trie.NewEmpty(triedb)
			for _, tx := range block.TXmig1s {
				migTree.Update(tx.Hash(), tx.Encode())
			}
			for _, tx := range block.TXmig2s {
				migTree.Update(tx.Hash(), tx.Encode())
			}
			for _, tx := range block.Anns {
				migTree.Update(tx.Hash(), tx.Encode())
			}
			for _, tx := range block.NSs {
				migTree.Update(tx.Hash(), tx.Encode())
			}

			//锁交易(马上锁，交易池中都要锁)
			if !params.Config.Not_Lock_immediately {
				p.Node.CurChain.Tx_pool.LockTX()
			}

			//处理要被彻底迁移出去的账户
			// p.handleAnns(block.Anns, st1, migTree)
			jiaoyichitime, totaltime := p.handleAnns(block.Anns, st1, migTree)

			// 发送relay交易，不再等待
			if len(relaytxs) != 0 {
				go p.TryRelay(relaytxs, block.Transactions, st2, p.sequenceID)
			}

			if !params.Config.Stop_When_Migrating && !params.Config.Fail {
				// 发送迁出账户给对应分片
				if params.Config.Cross_Chain {
					p.TryTXmig1(block.TXmig1s, outbalance, st3, migTree)
				} else if len(block.TXmig1s) != 0 {
					go p.TryTXmig1(block.TXmig1s, outbalance, st3, migTree)
				}
				// 通知各分片，账户已在本分片
				if len(block.TXmig2s) != 0 {
					go p.TryAnnounce(block.TXmig2s, st4, migTree)
				}
			}

			// 银行机制：本分片是结算发送方时，通知接收方分片
			if params.Config.EnableBankMechanism {
				selfShardID := params.ShardTable[params.Config.ShardID]
				for _, settle := range block.TXsettles {
					if settle.FromShardID == selfShardID {
						targetShard := params.ShardTableInt2Str[settle.ToShardID]
						bc, err := json.Marshal(settle)
						if err != nil {
							log.Panic(err)
						}
						message := jointMessage(cSettle, bc)
						go utils.TcpDial(message, params.NodeTable[targetShard]["N0"])
					}
				}
			}
			migend := time.Now().UnixMicro()
			s := fmt.Sprintf("%v %v %v %v %v %v %v", block.Header.Number, tx_total, len(block.TXmig1s)+len(block.TXmig2s)+len(block.Anns)+ns_count, migend-mig_begin, len(block.Anns), jiaoyichitime, totaltime)
			blockexetimelog.Write(strings.Split(s, " "))
			blockexetimelog.Flush()

			locked_cnt := 0
			if params.Config.Lock_Acc_When_Migrating {
				account.Lock_Acc_Lock.Lock()
				for _, locked := range p.Node.CurChain.Tx_pool.Locking_TX_Pools {
					locked_cnt += len(locked)
				}
				account.Lock_Acc_Lock.Unlock()
			} else if !params.Config.Stop_When_Migrating {
				account.Outing_Acc_Before_Announce_Lock.Lock()
				for _, locked := range p.Node.CurChain.Tx_pool.Outing_Before_Announce_TX_Pools {
					locked_cnt += len(locked)
				}
				account.Outing_Acc_Before_Announce_Lock.Unlock()
			}

			txlog.Flush()
			txmig1log.Flush()
			txmig2log.Flush()

			// 银行机制：记录银行余额、活跃贷款数与跨分片结算
			if params.Config.EnableBankMechanism {
				selfShardID := params.ShardTable[params.Config.ShardID]
				bankBalance := big.NewInt(0)
				if enc := st1.Get(account.BankAddrBytes(selfShardID)); enc != nil {
					bs := account.DecodeAccountState(enc)
					bankBalance.Set(bs.Balance)
				}
				account.LoanLock.Lock()
				activeLoans := len(account.Loan)
				account.LoanLock.Unlock()

				settledOut := big.NewInt(0)
				settledIn := big.NewInt(0)
				for _, settle := range block.TXsettles {
					if settle == nil || settle.Amount == nil {
						continue
					}
					if settle.FromShardID == selfShardID {
						settledOut.Add(settledOut, settle.Amount)
					} else if settle.ToShardID == selfShardID {
						settledIn.Add(settledIn, settle.Amount)
					}
				}

				banklog.Write([]string{fmt.Sprintf("%v", block.Header.Number), bankBalance.String(), fmt.Sprintf("%v", activeLoans), settledOut.String(), settledIn.String()})
				banklog.Flush()
			}

			// ctxlog.Flush()
			s = fmt.Sprintf("%v %v %v %v %v %v %v %v %v %v %v %v %v", pbftend-InitTime*1000, block.Header.Number, tx_total, tx_total-relayCount, tx_total-len(commit_txs), relayCount-tx_total+len(commit_txs), len(commit_txs), pbftend-pbftbefore, len(block.TXmig1s), len(block.TXmig2s), len(block.Anns), ns_count, locked_cnt)
			blocklog.Write(strings.Split(s, " "))
			blocklog.Flush()

			commitTX_numofNS := Txs_and_Num_of_New_State{
				Txs:              commit_txs,
				BlockSize:        len(block.TXmig1s) + len(block.TXmig2s) + len(block.Anns) + ns_count + len(block.Transactions),
				Num_of_New_State: len(block.NSs),
			}
			//主节点向客户端发送已确认上链的交易集
			c, err := json.Marshal(commitTX_numofNS)
			if err != nil {
				log.Panic(err)
			}
			m := jointMessage(cReply, c)
			utils.TcpDial(m, params.ClientAddr)

			if p.sequenceID == 1 && (params.Config.Bu_Tong_Bi_Li || params.Config.Bu_Tong_Shi_Jian || (params.Config.Fail && !params.Config.Bu_Tong_Bi_Li_2) || params.Config.Cross_Chain) {
				t := time.Now().UnixMilli()
				for _, tx := range p.Node.CurChain.Tx_pool.Queue {
					tx.RequestTime = t
				}
			}
		}
		p.sequenceID += 1

		if p.Node.nodeID == "N0" {
			p.sequenceLock.Unlock()
			if params.Config.Stop_When_Migrating { //若迁移时要暂停
				p.epochLock.Unlock()
			}
		}
	} else if pbftType == "EpochChange" {
		encoded_new := content
		var new map[string]int
		decoder := gob.NewDecoder(bytes.NewReader(encoded_new))
		err := decoder.Decode(&new)
		if err != nil {
			log.Panic(err)
		}

		// 将account2shard换成新的并得到out账户及映射
		out := make(map[string]int)
		account.Account2ShardLock.Lock()
		for addr, shard := range new {
			account.Account2Shard[addr] = shard
			if account.AccountInOwnShard[addr] && shard != params.ShardTable[params.Config.ShardID] {
				out[addr] = shard
				delete(account.AccountInOwnShard, addr)
			}
		}
		// account.Account2Shard = new
		account.Account2ShardLock.Unlock()

		if p.Node.nodeID == "N0" {
			//整理要发出去的账户的状态和交易，并发到对应分片主节点
			go p.SendOut(out)
		}

		p.msequenceID += 1

		if p.Node.nodeID == "N0" {
			flag := false
			//计算进来的个数
			inacc_count_lock.Lock()
			for k, v := range new {
				account.Account2ShardLock.Lock()
				if v == params.ShardTable[params.Config.ShardID] && !account.AccountInOwnShard[k] {
					inacc_count++
					flag = true
				}
				account.Account2ShardLock.Unlock()
			}

			if flag {
				fmt.Printf("\n有要进来的，此时inacc_count数量为: %v\n\n", inacc_count)
			}

			p.sequenceLock.Unlock()

			if flag && inacc_count == 0 { //有要进来的，且进完了
				inacc_count_lock.Unlock()
				p.spropose(inacc_pool)
			} else {
				inacc_count_lock.Unlock()
				if !flag { //没有要进来的
					go p.SendSure()
				}

			}

		}
	} else { // spropose
		encoded_stateNtx := content
		//解码成用户地址和状态
		in_StateAndTx := []*address_and_balance{}
		decoder := gob.NewDecoder(bytes.NewReader(encoded_stateNtx))
		err := decoder.Decode(&in_StateAndTx)
		if err != nil {
			log.Panic(err)
		}

		// build trie from the triedb (in disk)
		st, err := trie.New(trie.TrieID(common.BytesToHash(p.Node.CurChain.CurrentBlock.Header.StateRoot)), p.Node.CurChain.Triedb)
		if err != nil {
			log.Panic(err)
		}
		for _, v := range in_StateAndTx {
			//修改状态树
			hex_address, _ := hex.DecodeString(v.Address)

			account_state := &account.AccountState{
				Balance: v.Balance,
				Migrate: -1,
			}
			st.Update(hex_address, account_state.Encode())

			//将新账户放入account.AccountInOwnShard中，并设为true
			account.Account2ShardLock.Lock()
			account.AccountInOwnShard[v.Address] = true
			account.Account2ShardLock.Unlock()
		}
		// commit the memory trie to the database in the disk
		rt, ns := st.Commit(false)
		err = p.Node.CurChain.Triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic()
		}
		err = p.Node.CurChain.Triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}

		if p.Node.nodeID == "N0" {
			//将交易放入交易池
			p.Node.CurChain.Tx_pool.AddTxs(intx_pool)
			//发送完成指令给各个主节点
			fmt.Println("准备sendsure！")
			p.SendSure()

			// 将两个pool置空
			inacc_pool = make([]*address_and_balance, 0)
			intx_pool = make([]*core.Transaction, 0)
		}

		p.ssequenceID += 1

		if p.Node.nodeID == "N0" {
			p.sequenceLock.Unlock()
		}
	}
}
