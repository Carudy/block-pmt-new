package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"math/big"
)

// CreditEntry 是聚合后的 payer 交易里的一个收款人记录。
type CreditEntry struct {
	Recipient []byte
	Value     *big.Int
}

// TXborrow 表示源分片在迁移窗口内，把某个迁出账户的所有 payer 交易聚合成的“借款”条目。
// 它不再占用普通交易（Transaction）槽位，而是作为迁移条目打包进区块，
// 从而让区块腾出更多槽位给 payee/普通交易。
type TXborrow struct {
	Address    string         // 迁出账户地址
	TotalValue *big.Int       // 银行代付总额（所有聚合 payer 交易的 value 之和）
	NumTxs     int            // 聚合的原始 payer 交易笔数（用于 real_tx 计数）
	Credits    []*CreditEntry // 收款人列表（给收款人入账）
}

func (tx *TXborrow) Encode() []byte {
	var buff bytes.Buffer
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}
	return buff.Bytes()
}

func DecodeTXborrow(to_decode []byte) *TXborrow {
	var tx TXborrow
	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}
	return &tx
}

func (tx *TXborrow) Hash() []byte {
	hash := sha256.Sum256(tx.Encode())
	return hash[:]
}
