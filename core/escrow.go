package core

import (
	"blockEmulator/params"
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"math/big"
)

// EscrowTx 是 preempt_escrow 专用的交易包装：保留原始 Transaction，同时携带
// 发送方 nonce。这样无需修改 core.Transaction，避免影响 lb/ftl/caravan/preempt 的
// gob 编码与 TxHash。
type EscrowTx struct {
	Tx    *Transaction
	Nonce uint64
}

func (e *EscrowTx) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(e); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowTx(to_decode []byte) *EscrowTx {
	var e EscrowTx
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&e); err != nil {
		log.Panic(err)
	}
	return &e
}

// EscrowBatch 表示源分片一次 nonce 连续前缀授权。
type EscrowBatch struct {
	MigID        string
	SeqStart     uint64
	SeqEnd       uint64
	NonceStart   uint64
	NonceEnd     uint64
	Txs          []*EscrowTx
	TotalValue   *big.Int
	BatchHash    []byte
	SrcHeight    int
	SrcBlockHash []byte
}

func (b *EscrowBatch) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(b); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowBatch(to_decode []byte) *EscrowBatch {
	var b EscrowBatch
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&b); err != nil {
		log.Panic(err)
	}
	return &b
}

func (b *EscrowBatch) Hash() []byte {
	h := sha256.Sum256(b.Encode())
	return h[:]
}

// EscrowQuiesceReq 是目的分片 → 源分片的“停止授权”请求。
type EscrowQuiesceReq struct {
	MigID string
}

func (q *EscrowQuiesceReq) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(q); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowQuiesceReq(to_decode []byte) *EscrowQuiesceReq {
	var q EscrowQuiesceReq
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&q); err != nil {
		log.Panic(err)
	}
	return &q
}

// EscrowQuiesce 是源分片 → 目的分片的 quiesce 结果。
// Remaining 仅当 status == Settled/Quiescing 且 S_own == S_dst 时存在；
// Expired 时源分片保留这些交易并自行重放。
type EscrowQuiesce struct {
	MigID     string
	SStar     uint64
	NuStar    uint64
	Status    params.EscrowStatus
	Remaining []*EscrowTx
}

func (q *EscrowQuiesce) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(q); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowQuiesce(to_decode []byte) *EscrowQuiesce {
	var q EscrowQuiesce
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&q); err != nil {
		log.Panic(err)
	}
	return &q
}

func (q *EscrowQuiesce) Hash() []byte {
	h := sha256.Sum256(q.Encode())
	return h[:]
}

// EscrowSettle 是目的分片 → 源分片的冻结/结算请求。
type EscrowSettle struct {
	MigID        string
	SStar        uint64
	SM           uint64
	Spent        *big.Int
	NuDst        uint64
	Status       params.EscrowStatus
	FreezeHeight int
}

func (s *EscrowSettle) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(s); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowSettle(to_decode []byte) *EscrowSettle {
	var s EscrowSettle
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&s); err != nil {
		log.Panic(err)
	}
	return &s
}

func (s *EscrowSettle) Hash() []byte {
	h := sha256.Sum256(s.Encode())
	return h[:]
}

// EscrowDebit 是源分片 → 目的分片的扣款/退款/flow 结果。
// Status == Settled 时，Refund/Flow 应记入目的分片的 A 账户；
// Status == Expired 时，Refund/Flow 留在源分片，目的分片仅接收 Spent。
type EscrowDebit struct {
	MigID     string
	Spent     *big.Int
	Refund    *big.Int
	Flow      *big.Int
	Nu0       uint64
	NuDst     uint64
	Status    params.EscrowStatus
	SStar     uint64
	SM        uint64
	Remaining []*EscrowTx
}

func (d *EscrowDebit) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(d); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowDebit(to_decode []byte) *EscrowDebit {
	var d EscrowDebit
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&d); err != nil {
		log.Panic(err)
	}
	return &d
}

func (d *EscrowDebit) Hash() []byte {
	h := sha256.Sum256(d.Encode())
	return h[:]
}

// EscrowActivate 在 Debit 成功后广播新的账户归属；客户端也用同一消息更新路由。
type EscrowActivate struct {
	MigID       string
	Address     string
	SourceShard int
	DestShard   int
	NuStart     uint64
}

func (a *EscrowActivate) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(a); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowActivate(to_decode []byte) *EscrowActivate {
	var a EscrowActivate
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&a); err != nil {
		log.Panic(err)
	}
	return &a
}

// EscrowDelivery 是目的分片结算完成后，发给远端收款方所在分片的系统交付。
type EscrowDelivery struct {
	MigID     string
	Recipient []byte
	Value     *big.Int
	IdemKey   string
}

func (d *EscrowDelivery) Encode() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(d); err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeEscrowDelivery(to_decode []byte) *EscrowDelivery {
	var d EscrowDelivery
	if err := gob.NewDecoder(bytes.NewReader(to_decode)).Decode(&d); err != nil {
		log.Panic(err)
	}
	return &d
}

// EscrowBlockData 是 preempt_escrow 在区块中携带的系统操作集合。
// 旧 schema 下 Block.Escrow == nil，行为不变。
type EscrowBlockData struct {
	Batches  []*EscrowBatch
	Quiesces []*EscrowQuiesce
	Settles  []*EscrowSettle
	Debits   []*EscrowDebit
}

func (d *EscrowBlockData) Empty() bool {
	return d == nil || (len(d.Batches) == 0 && len(d.Quiesces) == 0 && len(d.Settles) == 0 && len(d.Debits) == 0)
}

// Hash 用于迁移树/调试。
func (d *EscrowBlockData) Hash() []byte {
	var buff bytes.Buffer
	if err := gob.NewEncoder(&buff).Encode(d); err != nil {
		log.Panic(err)
	}
	h := sha256.Sum256(buff.Bytes())
	return h[:]
}
