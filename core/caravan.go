package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"math/big"
)

// TXcaravan 表示目的分片在打包前，把同一个迁移账户的多笔 payer 交易聚合成的
// “单账户 → 多输出”交易。执行时对 Address 做一次合并扣减，并对每个收款人各入账一次，
// 只占用 1 个区块槽位（而非原始交易笔数）。
type TXcaravan struct {
	Address    string         // 迁移账户地址（发送方）
	TotalValue *big.Int       // 合并扣减总额 = Σ Outputs[i].Value
	NumTxs     int            // 聚合的原始 payer 交易笔数（用于 real_tx 计数）
	Outputs    []*CreditEntry // 各收款人及其入账金额
}

func (tx *TXcaravan) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeTXcaravan(to_decode []byte) *TXcaravan {
	var tx TXcaravan
	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}
	return &tx
}

func (tx *TXcaravan) Hash() []byte {
	hash := sha256.Sum256(tx.Encode())
	return hash[:]
}
