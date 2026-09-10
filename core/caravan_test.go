package core

import (
	"blockEmulator/account"
	"blockEmulator/params"
	"encoding/hex"
	"math/big"
	"testing"
)

// 构造一笔交易（from 固定为同一迁移账户）。
func mkCaravanTx(from, to string, val int64, fromMigration bool) *Transaction {
	sb, _ := hex.DecodeString(from)
	tb, _ := hex.DecodeString(to)
	return &Transaction{
		Sender:        sb,
		Recipient:     tb,
		Value:         big.NewInt(val),
		FromMigration: fromMigration,
	}
}

func withSchema(t *testing.T, schema params.Schema) {
	t.Helper()
	oldSchema := params.Config.Schema
	oldStop := params.Config.Stop_When_Migrating
	oldLock := params.Config.Lock_Acc_When_Migrating
	oldNotLock := params.Config.Not_Lock_immediately
	params.Config.Schema = schema
	params.Config.Stop_When_Migrating = false
	params.Config.Lock_Acc_When_Migrating = false
	params.Config.Not_Lock_immediately = true
	t.Cleanup(func() {
		params.Config.Schema = oldSchema
		params.Config.Stop_When_Migrating = oldStop
		params.Config.Lock_Acc_When_Migrating = oldLock
		params.Config.Not_Lock_immediately = oldNotLock
	})
}

func resetAccountGlobals() {
	account.AccountInOwnShard = make(map[string]bool)
	account.Outing_Acc_Before_Announce = make(map[string]bool)
	account.Credit = make(map[string]*big.Int)
	account.Loan = make(map[string]*big.Int)
}

// Caravan：同一迁移账户的 marked payer 交易被聚合成 1 笔 TXcaravan，不占普通槽位。
func TestCaravanAggregatesMarkedPayerTxs(t *testing.T) {
	withSchema(t, params.SchemaCaravan)
	resetAccountGlobals()

	sender := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	c := "cccccccccccccccccccccccccccccccccccccccc"
	d := "dddddddddddddddddddddddddddddddddddddddd"
	account.AccountInOwnShard[sender] = true

	pool := NewTxPool()
	pool.Queue = append(pool.Queue,
		mkCaravanTx(sender, b, 100, true),
		mkCaravanTx(sender, c, 50, true),
		mkCaravanTx(sender, d, 50, true),
		mkCaravanTx(sender, c, 7, false), // 非迁移注入的普通交易，不应被聚合
	)

	txs, queueLen := pool.FetchTxs2Pack(2000, 1)

	if len(txs) != 1 {
		t.Fatalf("expected 1 normal tx packed, got %d", len(txs))
	}
	if queueLen != 0 {
		t.Fatalf("expected queueLen 0, got %d", queueLen)
	}

	caravans := pool.FetchCaravans2Pack()
	if len(caravans) != 1 {
		t.Fatalf("expected 1 caravan, got %d", len(caravans))
	}
	cu := caravans[0]
	if cu.Address != sender {
		t.Fatalf("caravan address mismatch: %s", cu.Address)
	}
	if cu.NumTxs != 3 {
		t.Fatalf("expected NumTxs 3, got %d", cu.NumTxs)
	}
	if cu.TotalValue.Cmp(big.NewInt(200)) != 0 {
		t.Fatalf("expected TotalValue 200, got %s", cu.TotalValue)
	}
	if len(cu.Outputs) != 3 {
		t.Fatalf("expected 3 outputs, got %d", len(cu.Outputs))
	}

	// 再取一次应为空（已被取走）
	if again := pool.FetchCaravans2Pack(); len(again) != 0 {
		t.Fatalf("expected Caravan_Pool drained, got %d", len(again))
	}
}

// FTL 方案下：marked 交易不做聚合，按普通交易打包。
func TestFTLDoesNotAggregate(t *testing.T) {
	withSchema(t, params.SchemaFTL)
	resetAccountGlobals()

	sender := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	b := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	c := "cccccccccccccccccccccccccccccccccccccccc"
	account.AccountInOwnShard[sender] = true

	pool := NewTxPool()
	pool.Queue = append(pool.Queue,
		mkCaravanTx(sender, b, 100, true),
		mkCaravanTx(sender, c, 50, true),
	)

	txs, _ := pool.FetchTxs2Pack(2000, 1)

	if len(txs) != 2 {
		t.Fatalf("expected 2 normal txs packed under ftl, got %d", len(txs))
	}
	if caravans := pool.FetchCaravans2Pack(); len(caravans) != 0 {
		t.Fatalf("expected no caravan under ftl, got %d", len(caravans))
	}
}

// TXcaravan 编解码往返一致性。
func TestCaravanEncodeDecode(t *testing.T) {
	out := []*CreditEntry{
		{Recipient: []byte{0x01, 0x02}, Value: big.NewInt(11)},
		{Recipient: []byte{0x03}, Value: big.NewInt(22)},
	}
	cu := &TXcaravan{Address: "abc", TotalValue: big.NewInt(33), NumTxs: 2, Outputs: out}
	dec := DecodeTXcaravan(cu.Encode())
	if dec.Address != cu.Address || dec.NumTxs != cu.NumTxs || dec.TotalValue.Cmp(cu.TotalValue) != 0 {
		t.Fatalf("caravan header mismatch after round-trip")
	}
	if len(dec.Outputs) != 2 || dec.Outputs[0].Value.Cmp(big.NewInt(11)) != 0 {
		t.Fatalf("caravan outputs mismatch after round-trip")
	}
}
