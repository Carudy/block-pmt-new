package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/gob"
	"log"
	"math/big"
)

// TXsettle 表示一次跨分片的银行资金结算：
// 从 FromShardID 的银行向 ToShardID 的银行转移 Amount。
// 它由还款方（target 分片）发起，既在本分片记账（扣款），也通过 cSettle 消息通知收款方（source 分片）。
type TXsettle struct {
	FromShardID int      `json:"fromshardID"`
	ToShardID   int      `json:"toshardID"`
	Amount      *big.Int `json:"amount"`
	ID          int
}

func (tx *TXsettle) Encode() []byte {
	var buff bytes.Buffer

	enc := gob.NewEncoder(&buff)
	err := enc.Encode(tx)
	if err != nil {
		log.Panic(err)
	}

	return buff.Bytes()
}

func DecodeTXsettle(to_decode []byte) *TXsettle {
	var tx TXsettle

	decoder := gob.NewDecoder(bytes.NewReader(to_decode))
	err := decoder.Decode(&tx)
	if err != nil {
		log.Panic(err)
	}

	return &tx
}

func (tx *TXsettle) Hash() []byte {
	hash := sha256.Sum256(tx.Encode())
	return hash[:]
}
