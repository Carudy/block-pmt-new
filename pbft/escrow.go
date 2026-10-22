package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
	"sync/atomic"
	"time"
)

var (
	escrowEpoch     int
	escrowStarted   bool
	escrowTermSent  bool
	escrowStateLock sync.Mutex

	escrowBuffered = struct {
		sync.Mutex
		M map[string]map[uint64]*core.EscrowBatch
	}{M: make(map[string]map[uint64]*core.EscrowBatch)}

	escrowSyntheticID int64
	escrowDelivered   = struct {
		sync.Mutex
		M map[string]bool
	}{M: make(map[string]bool)}
)

func escrowMigIDFor(addr string) string {
	if rec := account.GetEscrowRecordByAccount(addr); rec != nil {
		return rec.MigID
	}
	return ""
}

// handleEscrowStart 由客户端在每轮迁移开始时广播，用于让空闲分片也参与
// terminal 统计。
func (p *Pbft) handleEscrowStart(content []byte) {
	var s EscrowStart
	if err := json.Unmarshal(content, &s); err != nil {
		return
	}
	escrowStateLock.Lock()
	escrowEpoch = s.Epoch
	escrowStarted = true
	escrowTermSent = false
	escrowStateLock.Unlock()
}

func (p *Pbft) handleEscrowTxsFromClient(content []byte) {
	msg := new(EscrowTxsFromClient)
	if err := json.Unmarshal(content, msg); err != nil {
		logEscrow("bad escrow client message: %v", err)
		return
	}
	self := params.ShardTable[params.Config.ShardID]
	forward := make(map[int][]*core.EscrowTx)
	local := make([]*core.Transaction, 0, len(msg.Txs))
	for _, e := range msg.Txs {
		if e == nil || e.Tx == nil {
			continue
		}
		account.SetTxNonce(e.Tx.Id, e.Nonce)
		from := hex.EncodeToString(e.Tx.Sender)
		account.Account2ShardLock.Lock()
		sid, ok := account.Account2Shard[from]
		account.Account2ShardLock.Unlock()
		if !ok {
			sid = account.Addr2Shard(from)
		}
		if sid == self {
			e.Tx.TxHash = e.Tx.Hash()
			local = append(local, e.Tx)
		} else {
			forward[sid] = append(forward[sid], e)
		}
	}
	fwd := int64(0)
	for _, txs := range forward {
		fwd += int64(len(txs))
	}
	if len(local) != 0 || fwd != 0 {
		p.Node.CurChain.Tx_pool.Lock.Lock()
		if len(local) != 0 {
			p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, local...)
		}
		p.Node.CurChain.Tx_pool.Intake.Received += int64(len(local))
		p.Node.CurChain.Tx_pool.Intake.ForwardedOut += fwd
		p.Node.CurChain.Tx_pool.Lock.Unlock()
	}
	for sid, txs := range forward {
		p.sendEscrowClientTxs(sid, txs)
	}
}

func (p *Pbft) sendEscrowClientTxs(shardID int, txs []*core.EscrowTx) {
	name := params.ShardTableInt2Str[shardID]
	if name == "" || name == params.Config.ShardID {
		return
	}
	addr := params.NodeTable[name]["N0"]
	if addr == "" {
		return
	}
	b, _ := json.Marshal(EscrowTxsFromClient{Txs: txs})
	utils.TcpDial(jointMessage(cEscrowClient, b), addr)
}

func (p *Pbft) handleEscrowBatch(content []byte) {
	b := core.DecodeEscrowBatch(content)
	if b == nil {
		return
	}
	applyOrBufferEscrowBatch(b)
}

func applyOrBufferEscrowBatch(b *core.EscrowBatch) {
	if b == nil {
		return
	}
	p := account.GetPendingRecord(b.MigID)
	if p == nil {
		bufferEscrowBatch(b)
		return
	}
	if p.Status == params.EscrowFrozen || p.Status == params.EscrowDelivered || p.Status == params.EscrowAborted {
		return
	}
	if b.SeqStart > p.NextSeq {
		bufferEscrowBatch(b)
		return
	}
	if b.SeqEnd < p.NextSeq {
		return // duplicate
	}
	if b.SeqStart < p.NextSeq {
		return // overlapping / invalid
	}
	applyEscrowBatch(b)
	drainEscrowBuffered(b.MigID)
}

func applyEscrowBatch(b *core.EscrowBatch) {
	bid := hex.EncodeToString(b.BatchHash)
	if !account.MarkMaterialized(b.MigID, bid) {
		return
	}
	pendingTxs := 0
	for _, e := range b.Txs {
		if e == nil || e.Tx == nil || e.Tx.Value == nil {
			continue
		}
		recipient := hex.EncodeToString(e.Tx.Recipient)
		account.AddPending(b.MigID, recipient, e.Tx.Value)
		pendingTxs++
	}
	account.AddPendingTxs(b.MigID, pendingTxs)
	account.AdvancePendingSeq(b.MigID, b.SeqEnd, b.NonceEnd)
}

func bufferEscrowBatch(b *core.EscrowBatch) {
	escrowBuffered.Lock()
	m := escrowBuffered.M[b.MigID]
	if m == nil {
		m = make(map[uint64]*core.EscrowBatch)
		escrowBuffered.M[b.MigID] = m
	}
	m[b.SeqStart] = b
	escrowBuffered.Unlock()
}

func drainEscrowBuffered(migID string) {
	for {
		p := account.GetPendingRecord(migID)
		if p == nil {
			return
		}
		escrowBuffered.Lock()
		m := escrowBuffered.M[migID]
		b := m[p.NextSeq]
		if b == nil {
			escrowBuffered.Unlock()
			return
		}
		delete(m, p.NextSeq)
		escrowBuffered.Unlock()
		if b.SeqEnd < p.NextSeq {
			continue
		}
		applyEscrowBatch(b)
	}
}

func (p *Pbft) handleEscrowQuiesceReq(content []byte) {
	q := new(core.EscrowQuiesceReq)
	if err := json.Unmarshal(content, q); err != nil {
		return
	}
	account.RequestQuiesce(q.MigID)
	p.Node.CurChain.EscrowQuiesceReq_pool.Add(q)
}

func (p *Pbft) handleEscrowQuiesce(content []byte) {
	q := core.DecodeEscrowQuiesce(content)
	if q == nil {
		return
	}
	height := p.Node.CurChain.CurrentBlock.Header.Number
	account.SetPendingSStar(q.MigID, q.SStar, height)
}

func (p *Pbft) handleEscrowSettle(content []byte) {
	s := core.DecodeEscrowSettle(content)
	if s == nil {
		return
	}
	p.Node.CurChain.EscrowSettle_pool.Add(s)
}

func (p *Pbft) handleEscrowDebit(content []byte) {
	d := core.DecodeEscrowDebit(content)
	if d == nil {
		return
	}
	p.Node.CurChain.EscrowDebit_pool.Add(d)
}

func (p *Pbft) handleEscrowActivate(content []byte) {
	a := core.DecodeEscrowActivate(content)
	if a == nil {
		return
	}
	self := params.ShardTable[params.Config.ShardID]
	account.Account2ShardLock.Lock()
	account.Account2Shard[a.Address] = a.DestShard
	if a.DestShard == self {
		account.AccountInOwnShard[a.Address] = true
	} else {
		delete(account.AccountInOwnShard, a.Address)
	}
	account.Account2ShardLock.Unlock()
	account.SetLastNonce(a.Address, a.NuStart)
}

func (p *Pbft) handleEscrowDeliver(content []byte) {
	d := core.DecodeEscrowDelivery(content)
	if d == nil || d.Recipient == nil || d.Value == nil {
		return
	}
	recipient := hex.EncodeToString(d.Recipient)
	account.Account2ShardLock.Lock()
	sid, ok := account.Account2Shard[recipient]
	account.Account2ShardLock.Unlock()
	if !ok {
		sid = account.Addr2Shard(recipient)
	}
	if sid != params.ShardTable[params.Config.ShardID] {
		name := params.ShardTableInt2Str[sid]
		if name != "" && params.NodeTable[name]["N0"] != "" {
			utils.TcpDial(jointMessage(cEscrowDeliver, d.Encode()), params.NodeTable[name]["N0"])
		}
		return
	}
	escrowDelivered.Lock()
	if escrowDelivered.M[d.IdemKey] {
		escrowDelivered.Unlock()
		return
	}
	escrowDelivered.M[d.IdemKey] = true
	escrowDelivered.Unlock()

	id := int(-atomic.AddInt64(&escrowSyntheticID, 1))
	tx := &core.Transaction{
		Sender:    append([]byte(nil), d.Recipient...),
		Recipient: d.Recipient,
		Value:     new(big.Int).Set(d.Value),
		Id:        id,
		IsRelay:   true,
	}
	tx.TxHash = tx.Hash()
	p.Node.CurChain.Tx_pool.Lock.Lock()
	p.Node.CurChain.Tx_pool.Queue = append(p.Node.CurChain.Tx_pool.Queue, tx)
	p.Node.CurChain.Tx_pool.Lock.Unlock()
}

// escrowAfterCommit 在区块 AddBlock 之后调用（仅 N0、仅 preempt_escrow）。
func (p *Pbft) escrowAfterCommit(block *core.Block) {
	if !params.Config.IsPreemptEscrow() || block == nil {
		return
	}
	height := block.Header.Number

	escrowStateLock.Lock()
	if len(block.TXmig1s) != 0 || len(block.TXmig2s) != 0 || (block.Escrow != nil && !block.Escrow.Empty()) {
		escrowStarted = true
	}
	epoch := escrowEpoch
	escrowStateLock.Unlock()

	// 目的分片：对已 prepared 的记录，延迟 QuiesceDelayBlocks 后请求源分片停止授权。
	for _, pr := range account.AllPendingRecords() {
		if account.MarkQuiesceRequested(pr.MigID, height, params.Config.QuiesceDelayBlocks) {
			name := params.ShardTableInt2Str[pr.SourceShard]
			if name != "" {
				if addr := params.NodeTable[name]["N0"]; addr != "" {
					b, _ := json.Marshal(&core.EscrowQuiesceReq{MigID: pr.MigID})
					utils.TcpDial(jointMessage(cEscrowQReq, b), addr)
				}
			}
		}
	}

	// 目的分片：把已 prepared 的 batch 补记录，并尝试 Freeze。
	for _, pr := range account.AllPendingRecords() {
		drainEscrowBuffered(pr.MigID)
		if account.FreezePending(pr.MigID, height, params.Config.SettlementWaitBlocks) {
			snap := account.PendingSnapshot(pr.MigID)
			if snap != nil && account.MarkSettleSent(snap.MigID) {
				s := &core.EscrowSettle{
					MigID:        snap.MigID,
					SStar:        snap.SStar,
					SM:           snap.SM,
					Spent:        new(big.Int).Set(snap.Spent),
					NuDst:        snap.NuDst,
					Status:       params.EscrowFrozen,
					FreezeHeight: snap.FreezeHeight,
				}
				if name := params.ShardTableInt2Str[snap.SourceShard]; name != "" {
					if addr := params.NodeTable[name]["N0"]; addr != "" {
						utils.TcpDial(jointMessage(cEscrowSettle, s.Encode()), addr)
					}
				}
			}
		}
	}

	// 源分片：回复 quiesce。
	if block.Escrow != nil {
		for _, b := range block.Escrow.Batches {
			rec := account.GetEscrowRecord(b.MigID)
			if rec == nil {
				continue
			}
			if name := params.ShardTableInt2Str[rec.DestShard]; name != "" {
				if addr := params.NodeTable[name]["N0"]; addr != "" {
					utils.TcpDial(jointMessage(cEscrowBatch, b.Encode()), addr)
				}
			}
		}
		for _, q := range block.Escrow.Quiesces {
			rec := account.GetEscrowRecord(q.MigID)
			if rec == nil {
				continue
			}
			if name := params.ShardTableInt2Str[rec.DestShard]; name != "" {
				if addr := params.NodeTable[name]["N0"]; addr != "" {
					utils.TcpDial(jointMessage(cEscrowQuies, q.Encode()), addr)
				}
			}
		}
	}

	// 记录实验日志（仅 N0 创建了 writer）。
	for _, m := range block.TXmig1s {
		if rec := account.GetEscrowRecordByAccount(m.Address); rec != nil {
			writeEscrowLog(height, "lock", rec.MigID, rec.Account, rec.Credit, rec.Nu0, rec.Status.String(), fmt.Sprintf("h_lock=%d", rec.HLock))
		}
	}
	for _, m := range block.TXmig2s {
		if m != nil {
			writeEscrowLog(height, "prepare", m.MigID, m.Address, m.Value, 0, params.EscrowPrepared.String(), "")
		}
	}
	if block.Escrow != nil {
		for _, b := range block.Escrow.Batches {
			writeEscrowLog(height, "batch", b.MigID, "", b.TotalValue, b.NonceEnd, "", fmt.Sprintf("seq=%d-%d nonce=%d-%d ntx=%d", b.SeqStart, b.SeqEnd, b.NonceStart, b.NonceEnd, len(b.Txs)))
		}
		for _, q := range block.Escrow.Quiesces {
			// 触发原因：超时路径会把状态置为 Expired，延迟路径保持 Quiescing。
			trigger := "delay"
			if q.Status == params.EscrowExpired {
				trigger = "timeout"
			}
			writeEscrowLog(height, "quiesce", q.MigID, "", nil, q.NuStar, q.Status.String(), fmt.Sprintf("S*=%d trigger=%s", q.SStar, trigger))
		}
		for _, s := range block.Escrow.Settles {
			if rec := account.GetEscrowRecord(s.MigID); rec != nil {
				writeEscrowLog(height, "settle", s.MigID, rec.Account, s.Spent, s.NuDst, rec.Status.String(), fmt.Sprintf("SM=%d", s.SM))
			}
		}
		for _, d := range block.Escrow.Debits {
			refund, flow := big.NewInt(0), big.NewInt(0)
			if d.Refund != nil {
				refund.Set(d.Refund)
			}
			if d.Flow != nil {
				flow.Set(d.Flow)
			}
			writeEscrowLog(height, "debit", d.MigID, "", d.Spent, d.NuDst, d.Status.String(), fmt.Sprintf("refund=%v flow=%v", refund, flow))
		}
	}
	mptp, gated := p.Node.CurChain.Tx_pool.EscrowQueueStats()
	normal := p.Node.CurChain.Tx_pool.QueueLen()
	pendingRecords, pendingCredits := account.CountPending()
	if escrowqueuelog != nil {
		escrowqueuelog.Write([]string{fmt.Sprintf("%d", height), fmt.Sprintf("%d", mptp), fmt.Sprintf("%d", gated), fmt.Sprintf("%d", pendingRecords), fmt.Sprintf("%v", account.AllEscrowTerminal()), fmt.Sprintf("%d", pendingCredits)})
		escrowqueuelog.Flush()
	}

	// 发送 Debit / Activation / Delivery 出站消息。
	for _, d := range p.Node.CurChain.EscrowOutDebits {
		rec := account.GetEscrowRecord(d.MigID)
		if rec == nil {
			continue
		}
		if name := params.ShardTableInt2Str[rec.DestShard]; name != "" {
			if addr := params.NodeTable[name]["N0"]; addr != "" {
				utils.TcpDial(jointMessage(cEscrowDebit, d.Encode()), addr)
			}
		}
	}
	p.Node.CurChain.EscrowOutDebits = nil

	for _, a := range p.Node.CurChain.EscrowOutActivations {
		b := a.Encode()
		for shardName, nodes := range params.NodeTable {
			if nodes["N0"] == "" {
				continue
			}
			utils.TcpDial(jointMessage(cEscrowActive, b), nodes["N0"])
			_ = shardName
		}
		utils.TcpDial(jointMessage(cEscrowActive, b), params.ClientAddr)
	}
	p.Node.CurChain.EscrowOutActivations = nil

	for _, d := range p.Node.CurChain.EscrowOutDeliveries {
		p.handleEscrowDeliver(d.Encode())
	}
	p.Node.CurChain.EscrowOutDeliveries = nil

	// 所有 shard（包括空闲 shard）在收到 escrowStart 后都可上报 terminal。
	escrowStateLock.Lock()
	busy := escrowStarted && (!account.AllEscrowTerminal() || mptp != 0 || gated != 0 || normal != 0)
	if busy {
		escrowTermSent = false
	}
	shouldTerm := escrowStarted && !escrowTermSent && !busy
	if shouldTerm {
		escrowTermSent = true
	}
	escrowStateLock.Unlock()
	if busy {
		b, _ := json.Marshal(EscrowBusy{ShardID: params.Config.ShardID, Epoch: epoch})
		utils.TcpDial(jointMessage(cEscrowBusy, b), params.ClientAddr)
	}
	if shouldTerm {
		logEscrow("terminal shard=%s h=%d epoch=%d", params.Config.ShardID, height, epoch)
		b, _ := json.Marshal(EscrowTerm{ShardID: params.Config.ShardID, Epoch: epoch})
		utils.TcpDial(jointMessage(cEscrowTerm, b), params.ClientAddr)
	}
}

func logEscrow(format string, args ...interface{}) {
	fmt.Printf("[escrow] "+format+"\n", args...)
}

func writeEscrowLog(height int, event, migID, account string, value *big.Int, nu uint64, status, note string) {
	if escrowlog == nil {
		return
	}
	v := "0"
	if value != nil {
		v = value.String()
	}
	escrowlog.Write([]string{fmt.Sprintf("%d", time.Now().UnixMilli()), fmt.Sprintf("%d", height), event, migID, account, v, fmt.Sprintf("%d", nu), status, note})
	escrowlog.Flush()
}
