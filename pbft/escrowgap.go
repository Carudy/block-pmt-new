package pbft

import (
	"blockEmulator/account"
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// 缺口恢复（仅 preempt_escrow）：
// 结算时源分片发出的 Debit.Remaining 是“迁移后应重放”的权威集合。它可能因为
// 交接时序或消息丢失而没有全部到达目的分片的 gated 队列，表现为 nonce 缺一格。
//
// 做法：源分片在结算后把 Remaining 留在 core 的归档里；目的分片发现缺口并持续
// GapReqBlocks 个块不推进时，按 (mig_id, nonce) 向源分片要那一笔。拿到就插入
// gated 继续推进；拿不到说明该交易确实从未到达源分片（真丢失），记入
// S*_escrow_drop.csv 的 reason=lost，而不是靠 gap repair 蒙混过去。

type EscrowGapReq struct {
	MigID string
	Nonce uint64
}

type EscrowGapResp struct {
	MigID string
	Nonce uint64
	Txs   []*core.EscrowTx
}

var (
	gapReqLock sync.Mutex
	gapReqSent map[string]int64 // "migID:nonce" -> unix ms，避免重复请求
)

// requestGap 由目的分片调用：缺口持续未推进时向源分片要那一笔。
func (p *Pbft) requestGap(migID string, nonce uint64) {
	rec := account.GetPendingRecord(migID)
	if rec == nil {
		return
	}
	key := fmt.Sprintf("%s:%d", migID, nonce)
	now := time.Now().UnixMilli()
	gapReqLock.Lock()
	if gapReqSent == nil {
		gapReqSent = make(map[string]int64)
	}
	if last, ok := gapReqSent[key]; ok && now-last < 5000 {
		gapReqLock.Unlock()
		return
	}
	gapReqSent[key] = now
	gapReqLock.Unlock()

	name := params.ShardTableInt2Str[rec.SourceShard]
	if name == "" {
		return
	}
	addr := params.NodeTable[name]["N0"]
	if addr == "" {
		return
	}
	b, err := json.Marshal(EscrowGapReq{MigID: migID, Nonce: nonce})
	if err != nil {
		return
	}
	utils.TcpDial(jointMessage(cEscrowGapReq, b), addr)
}

func (p *Pbft) handleEscrowGapReq(content []byte) {
	q := new(EscrowGapReq)
	if err := json.Unmarshal(content, q); err != nil {
		return
	}
	resp := EscrowGapResp{MigID: q.MigID, Nonce: q.Nonce}
	if tx := core.GetEscrowRemaining(q.MigID, q.Nonce); tx != nil {
		resp.Txs = []*core.EscrowTx{tx}
	}
	b, err := json.Marshal(resp)
	if err != nil {
		return
	}
	// 回复给请求方（目的分片）。源分片只有 EscrowRecord，没有 PendingRecord。
	dest := -1
	if rec := account.GetEscrowRecord(q.MigID); rec != nil {
		dest = rec.DestShard
	} else if pr := account.GetPendingRecord(q.MigID); pr != nil {
		dest = pr.DestShard
	}
	if dest < 0 {
		return
	}
	name := params.ShardTableInt2Str[dest]
	if name == "" {
		return
	}
	if addr := params.NodeTable[name]["N0"]; addr != "" {
		utils.TcpDial(jointMessage(cEscrowGapResp, b), addr)
	}
}

func (p *Pbft) handleEscrowGapResp(content []byte) {
	r := new(EscrowGapResp)
	if err := json.Unmarshal(content, r); err != nil {
		return
	}
	rec := account.GetPendingRecord(r.MigID)
	if rec == nil {
		return
	}
	height := p.Node.CurChain.CurrentBlock.Header.Number
	if len(r.Txs) == 0 {
		// 源分片也没有：真丢失。写进 drop 日志，严格模式下仍会停，但原因不再是谜。
		fmt.Printf("[escrow][lost] mig=%s account=%s nonce=%d (源分片无此交易)\n", r.MigID, rec.Account, r.Nonce)
		p.Node.CurChain.Tx_pool.RecordLost(rec.Account, r.Nonce, height)
		if params.Config.EscrowSkipConfirmedLost {
			// 已由源分片确认不可恢复：显式跳过，让批次能跑完（该笔已在 drop 里记账）。
			if cur := account.GetLastNonce(rec.Account); cur < r.Nonce {
				account.SetLastNonce(rec.Account, r.Nonce)
			}
			fmt.Printf("[escrow][lost-skip] account=%s nonce=%d (EscrowSkipConfirmedLost)\n", rec.Account, r.Nonce)
		}
		return
	}
	for _, e := range r.Txs {
		if e != nil && e.Tx != nil {
			account.SetTxNonce(e.Tx.Id, e.Nonce)
		}
	}
	p.Node.CurChain.Tx_pool.AddGatedTxs(rec.Account, r.Txs)
	fmt.Printf("[escrow][gap-recovered] mig=%s account=%s nonce=%d\n", r.MigID, rec.Account, r.Nonce)
}
