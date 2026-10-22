package core

import (
	"blockEmulator/account"
	"blockEmulator/params"
	"math/big"
	"testing"
)

func escrowTestSetup(t *testing.T) {
	t.Helper()
	oldSchema := params.Config.Schema
	oldShards := params.Config.Shard_num
	params.Config.Schema = params.SchemaPreemptEscrow
	params.Config.Shard_num = 1
	account.InitEscrowState()
	t.Cleanup(func() {
		params.Config.Schema = oldSchema
		params.Config.Shard_num = oldShards
		account.InitEscrowState()
	})
}

func addLockedAccount(addr string, credit int64) *account.EscrowRecord {
	rec := &account.EscrowRecord{
		MigID:       "mig-" + addr,
		Account:     addr,
		SourceShard: 0,
		DestShard:   1,
		Credit:      big.NewInt(credit),
		TotalCons:   big.NewInt(0),
		SeqCons:     0,
		Nu0:         0,
		NuCons:      0,
		HLock:       1,
		Status:      params.EscrowLocked,
		SOwn:        0,
		Spent:       big.NewInt(0),
	}
	account.PutEscrowRecord(rec)
	account.AccountInOwnShard[addr] = true
	return rec
}

func escTx(sender, recipient string, val int64, nonce uint64) *EscrowTx {
	return &EscrowTx{
		Tx: &Transaction{
			Sender:    []byte(sender),
			Recipient: []byte(recipient),
			Value:     big.NewInt(val),
			Id:        int(nonce),
		},
		Nonce: nonce,
	}
}

func TestBuildEscrowBatchesPrefixAndCredit(t *testing.T) {
	escrowTestSetup(t)
	addr := "aaaaaaaaaaaaaaaaaaaaaaaa"
	addLockedAccount(addr, 100)

	pool := NewTxPool()
	pool.MPTP[addr] = []*EscrowTx{
		escTx(addr, "r1", 60, 1),
		escTx(addr, "r2", 50, 2),
		escTx(addr, "r3", 1, 3),
	}

	batches := pool.BuildEscrowBatches(10)
	if len(batches) != 1 {
		t.Fatalf("expected 1 batch, got %d", len(batches))
	}
	b := batches[0]
	if b.SeqStart != 1 || b.SeqEnd != 1 || b.NonceStart != 1 || b.NonceEnd != 1 {
		t.Fatalf("unexpected batch range: %+v", b)
	}
	if b.TotalValue.Cmp(big.NewInt(60)) != 0 {
		t.Fatalf("expected batch value 60, got %v", b.TotalValue)
	}
	if len(pool.MPTP[addr]) != 2 {
		t.Fatalf("expected 2 txs left in MPTP, got %d", len(pool.MPTP[addr]))
	}

	// 模拟 batch 已提交：total_cons=60, seq/nonce=1。
	rec := account.GetEscrowRecordByAccount(addr)
	rec.TotalCons = big.NewInt(60)
	rec.SeqCons = 1
	rec.NuCons = 1
	// 下一笔 50 会超额，且不能跳过它去取 nonce=3。
	if got := pool.BuildEscrowBatches(11); len(got) != 0 {
		t.Fatalf("expected no batch after credit exhausted, got %d", len(got))
	}
}

func TestBuildEscrowBatchesStopsAtNonceGap(t *testing.T) {
	escrowTestSetup(t)
	addr := "bbbbbbbbbbbbbbbbbbbbbb"
	addLockedAccount(addr, 1000)
	pool := NewTxPool()
	pool.MPTP[addr] = []*EscrowTx{
		escTx(addr, "r1", 10, 1),
		escTx(addr, "r3", 10, 3),
	}
	batches := pool.BuildEscrowBatches(1)
	if len(batches) != 1 || batches[0].NonceEnd != 1 {
		t.Fatalf("must stop at nonce gap, got %+v", batches)
	}
	if len(pool.MPTP[addr]) != 1 {
		t.Fatalf("nonce-gap tx must stay in MPTP")
	}
}

func TestBuildEscrowBatchesNoSkipOverLimit(t *testing.T) {
	escrowTestSetup(t)
	addr := "cccccccccccccccccccccc"
	addLockedAccount(addr, 100)
	pool := NewTxPool()
	pool.MPTP[addr] = []*EscrowTx{
		escTx(addr, "r1", 60, 1),
		escTx(addr, "r2", 60, 2),
		escTx(addr, "r3", 1, 3),
	}
	if got := pool.BuildEscrowBatches(1); len(got) != 1 || got[0].NonceEnd != 1 {
		t.Fatalf("expected only nonce 1, got %+v", got)
	}
}

func TestEscrowGapRepairUnblocksFlow(t *testing.T) {
	escrowTestSetup(t)
	oldRepair := params.Config.EscrowGapRepairBlocks
	params.Config.EscrowGapRepairBlocks = 2
	t.Cleanup(func() { params.Config.EscrowGapRepairBlocks = oldRepair })

	addr := "dddddddddddddddddddddd"
	account.AccountInOwnShard[addr] = true
	account.SetFlowActive(addr, true)
	account.SetLastNonce(addr, 1)

	pool := NewTxPool()
	pool.EscrowGated[addr] = []*EscrowTx{
		escTx(addr, "r3", 10, 3),
		escTx(addr, "r4", 10, 4),
	}
	pool.EscrowGatedSince[addr] = 1

	txs, _ := pool.FetchTxs2Pack(100, 10)
	if len(txs) != 2 {
		t.Fatalf("gap repair should release nonce 3 and 4, got %d", len(txs))
	}
	if len(pool.EscrowGated[addr]) != 0 {
		t.Fatalf("gated queue should be empty after repair")
	}
	if got := account.GetLastNonce(addr); got != 2 {
		// Fetch 只把交易放入区块；LastNonce 在 AddBlock 执行阶段才推进。
		t.Fatalf("gap repair should set last nonce to 2, got %d", got)
	}
}

func TestEscrowBatchAndDeliveryCodec(t *testing.T) {
	b := &EscrowBatch{
		MigID:      "m1",
		SeqStart:   1,
		SeqEnd:     2,
		NonceStart: 5,
		NonceEnd:   6,
		Txs: []*EscrowTx{
			{Nonce: 5, Tx: &Transaction{Value: big.NewInt(1)}},
		},
		TotalValue: big.NewInt(1),
	}
	b.BatchHash = b.Hash()
	dec := DecodeEscrowBatch(b.Encode())
	if dec.MigID != b.MigID || dec.NonceEnd != b.NonceEnd || dec.TotalValue.Cmp(b.TotalValue) != 0 {
		t.Fatalf("batch codec mismatch")
	}

	d := &EscrowDelivery{MigID: "m1", Recipient: []byte{1, 2, 3}, Value: big.NewInt(9), IdemKey: "m1:1"}
	dd := DecodeEscrowDelivery(d.Encode())
	if dd.Value.Cmp(d.Value) != 0 || dd.IdemKey != d.IdemKey {
		t.Fatalf("delivery codec mismatch")
	}
}
