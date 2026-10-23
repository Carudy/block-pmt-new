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
	escrowTxLogLock   sync.Mutex

	// 注入消息去重：Seq -> 已处理（重传幂等）。
	escrowInjectSeen     map[uint64]bool
	escrowInjectSeenLock sync.Mutex
	escrowDelivered      = struct {
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
	if msg.Seq != 0 {
		escrowInjectSeenLock.Lock()
		if escrowInjectSeen == nil {
			escrowInjectSeen = make(map[uint64]bool)
		}
		if escrowInjectSeen[msg.Seq] {
			escrowInjectSeenLock.Unlock()
			sendEscrowAck(msg.Seq, msg.AckAddr) // 重传：已经处理过，直接再确认一次
			return
		}
		escrowInjectSeen[msg.Seq] = true
		escrowInjectSeenLock.Unlock()
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
		// 注意：目的分片在 Debit 之前还没把 A 的 Account2Shard 改成自己，
		// 若只按 Account2Shard 判定，交易会在源/目的分片之间来回转发（每次一跳都有丢包风险）。
		// InLocalEscrowFlow 为 true 说明本分片已用 escrow 状态跟踪该账户，直接本地入队。
		if sid == self || account.InLocalEscrowFlow(from) {
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

	// 入池之后才确认，保证客户端停止重传时交易确实已经在本分片。
	if msg.Seq != 0 {
		sendEscrowAck(msg.Seq, msg.AckAddr)
	}
}

// sendEscrowAck 确认一条注入/转发消息。ackAddr 为空时发给客户端（客户端注入），
// 否则发给转发方分片。
func sendEscrowAck(seq uint64, ackAddr string) {
	if seq == 0 {
		return
	}
	b, err := json.Marshal(EscrowClientAck{ShardID: params.ShardTable[params.Config.ShardID], Seq: seq})
	if err != nil {
		return
	}
	target := params.ClientAddr
	if ackAddr != "" {
		target = ackAddr
	}
	utils.TcpDial(jointMessage(cEscrowAck, b), target)
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
	// 转发也走可靠投递：丢一条转发消息就等于丢一批用户交易。
	seq := nextReliableSeq()
	self := params.NodeTable[params.Config.ShardID]["N0"]
	b, err := json.Marshal(EscrowTxsFromClient{Seq: seq, Txs: txs, AckAddr: self})
	if err != nil {
		return
	}
	ids := make([]int, 0, len(txs))
	for _, e := range txs {
		if e != nil && e.Tx != nil {
			ids = append(ids, e.Tx.Id)
		}
	}
	registerReliable(seq, cEscrowClient, b, addr, "fwd", ids)
}

func (p *Pbft) handleEscrowBatch(content []byte) {
	b := core.DecodeEscrowBatch(content)
	if b == nil {
		return
	}
	applyOrBufferEscrowBatch(b, p.Node.CurChain.CurrentBlock.Header.Number)
}

func applyOrBufferEscrowBatch(b *core.EscrowBatch, height int) {
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
	applyEscrowBatch(b, height)
	drainEscrowBuffered(b.MigID, height)
}

func applyEscrowBatch(b *core.EscrowBatch, height int) {
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
		account.AddPendingTxID(b.MigID, recipient, e.Tx.Id)
		pendingTxs++
	}
	account.AddPendingTxs(b.MigID, pendingTxs)
	account.AdvancePendingSeq(b.MigID, b.SeqEnd, b.NonceEnd)
	writeEscrowTxRecord(b, height)
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

func drainEscrowBuffered(migID string, height int) {
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
		applyEscrowBatch(b, height)
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
	writeEscrowTxDeliver(d.MigID, d.TxIDs, d.SettleHeight, p.Node.CurChain.CurrentBlock.Header.Number, params.ShardTable[params.Config.ShardID])
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
		drainEscrowBuffered(pr.MigID, height)
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

	// 可靠投递重试耗尽：这些 relay 里的交易算作丢弃（relay_lost），账本仍然闭合。
	if lost := TakeReliableLost(); len(lost) != 0 {
		for _, id := range lost {
			if escrowdroplog != nil {
				escrowdroplog.Write([]string{fmt.Sprintf("%d", id), "", "0", "0", "relay_lost", fmt.Sprintf("%d", height)})
			}
		}
		if escrowdroplog != nil {
			escrowdroplog.Flush()
		}
	}

	// 缺口回查：目的分片向源分片要缺失的 nonce（源分片保留着 Debit.Remaining）。
	for _, need := range p.Node.CurChain.Tx_pool.TakeGapNeeds() {
		if rec := account.GetPendingRecordByAccount(need.Account); rec != nil {
			p.requestGap(rec.MigID, need.Nonce)
		}
	}

	// 记录实验日志（仅 N0 创建了 writer）。
	// 丢弃事件：strict gap / duplicate / no_nonce / not_own / relay。
	if evs := p.Node.CurChain.Tx_pool.TakeDropEvents(); len(evs) != 0 {
		for _, e := range evs {
			if escrowdroplog != nil {
				escrowdroplog.Write([]string{
					fmt.Sprintf("%d", e.TxID), e.Sender, fmt.Sprintf("%d", e.Nonce),
					fmt.Sprintf("%d", e.Nu0), e.Reason, fmt.Sprintf("%d", e.Height),
				})
			}
			if e.Reason == "gap" {
				fmt.Printf("[escrow][gap] shard=%s height=%d account=%s missing_nonce=%d (strict: 不跳过，等待定位)\n",
					params.Config.ShardID, e.Height, e.Sender, e.Nonce)
			}
		}
		if escrowdroplog != nil {
			escrowdroplog.Flush()
		}
	}

	// 交接窗口：账户已经结算离开，但交易刚好处在“队列已取、映射已改”的窄缝里。
	// 把它们转发到账户当前所在的分片，而不是当成丢给别的分片的交易丢掉。
	if fwd := p.Node.CurChain.Tx_pool.TakeEscrowForward(); len(fwd) != 0 {
		byShard := make(map[int][]*core.EscrowTx)
		for _, e := range fwd {
			if e == nil || e.Tx == nil {
				continue
			}
			addr := hex.EncodeToString(e.Tx.Sender)
			account.Account2ShardLock.Lock()
			sid, ok := account.Account2Shard[addr]
			account.Account2ShardLock.Unlock()
			if !ok || sid == params.ShardTable[params.Config.ShardID] {
				continue
			}
			byShard[sid] = append(byShard[sid], e)
		}
		for sid, txs := range byShard {
			p.sendEscrowClientTxs(sid, txs)
		}
	}

	// 收款方已迁走的 relay：按当前映射重新发送（借助 TryRelay2 的可靠投递）。
	if stranded := p.Node.CurChain.Tx_pool.TakeEscrowForwardRelay(); len(stranded) != 0 {
		wrapped := make([]*core.TXrelay, 0, len(stranded))
		for _, tx := range stranded {
			if tx != nil {
				wrapped = append(wrapped, &core.TXrelay{Txcs: tx})
			}
		}
		if len(wrapped) != 0 {
			p.TryRelay2(wrapped)
		}
	}

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
			writeEscrowTxAuth(b, height)
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
	// 交付事件：逐笔写 deliver_height / deliver_time / deliver_shard。
	for _, dl := range p.Node.CurChain.EscrowDeliverLogs {
		if dl == nil {
			continue
		}
		writeEscrowTxDeliver(dl.MigID, dl.TxIDs, dl.Height, dl.Height, dl.Shard)
	}
	p.Node.CurChain.EscrowDeliverLogs = nil

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
	busy := escrowStarted && (!account.AllEscrowTerminal() || mptp != 0 || gated != 0 || normal != 0 || reliablePendingCount() != 0)
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

// writeEscrowTxAuth 在源分片 batch 提交时写逐笔授权记录。
//
// 注意：S*_escrow_tx.csv 会被两个 goroutine 写 —— 区块提交后的 escrowAfterCommit
// （auth/deliver）和 relay 消息处理协程（record）。csv.Writer 不是并发安全的，
// 一旦内部 error 被置位，后续 Write 会静默丢弃，所以必须加锁。
func writeEscrowTxAuth(b *core.EscrowBatch, height int) {
	if escapetxlog == nil || b == nil {
		return
	}
	escrowTxLogLock.Lock()
	defer escrowTxLogLock.Unlock()
	now := time.Now().UnixMilli()
	for _, e := range b.Txs {
		if e == nil || e.Tx == nil {
			continue
		}
		v := "0"
		if e.Tx.Value != nil {
			v = e.Tx.Value.String()
		}
		escapetxlog.Write([]string{
			fmt.Sprintf("%d", e.Tx.Id),
			hex.EncodeToString(e.Tx.Sender),
			hex.EncodeToString(e.Tx.Recipient),
			v,
			fmt.Sprintf("%d", e.Nonce),
			b.MigID,
			fmt.Sprintf("%d", b.SeqStart),
			fmt.Sprintf("%d", e.Tx.RequestTime-InitTime*1000),
			fmt.Sprintf("%d", height),
			fmt.Sprintf("%d", now),
			"", "", "", "", "", "",
		})
	}
	escapetxlog.Flush()
}

// writeEscrowTxRecord 在目的分片接受 batch 时写记录高度/时间。
func writeEscrowTxRecord(b *core.EscrowBatch, height int) {
	if escapetxlog == nil || b == nil {
		return
	}
	escrowTxLogLock.Lock()
	defer escrowTxLogLock.Unlock()
	now := time.Now().UnixMilli()
	for _, e := range b.Txs {
		if e == nil || e.Tx == nil {
			continue
		}
		escapetxlog.Write([]string{
			fmt.Sprintf("%d", e.Tx.Id),
			"", "", "", "",
			b.MigID,
			fmt.Sprintf("%d", b.SeqStart),
			"", "", "",
			fmt.Sprintf("%d", height),
			fmt.Sprintf("%d", now),
			"", "", "", "",
		})
	}
	escapetxlog.Flush()
}

// writeEscrowTxDeliver 在收款方实际入账时写交付高度/时间。
func writeEscrowTxDeliver(migID string, txIDs []int, settleHeight, height, shard int) {
	if escapetxlog == nil || len(txIDs) == 0 {
		return
	}
	escrowTxLogLock.Lock()
	defer escrowTxLogLock.Unlock()
	now := time.Now().UnixMilli()
	for _, id := range txIDs {
		escapetxlog.Write([]string{
			fmt.Sprintf("%d", id),
			"", "", "", "",
			migID,
			"", "", "", "", "", "",
			fmt.Sprintf("%d", settleHeight),
			fmt.Sprintf("%d", height),
			fmt.Sprintf("%d", now),
			fmt.Sprintf("%d", shard),
		})
	}
	escapetxlog.Flush()
}

// escrowBatchCols 返回 S*_block.csv 末尾的 escrow_batch / escrow_tx 两列。
// 旧 schema 返回 nil，保证 lb/ftl/caravan/preempt 的日志列集完全不变。
func escrowBatchCols(escrowSchema bool, batchNum, txNum int) []string {
	if !escrowSchema {
		return nil
	}
	return []string{fmt.Sprintf("%d", batchNum), fmt.Sprintf("%d", txNum)}
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
