package account

import (
	"blockEmulator/params"
	"math/big"
	"testing"
)

func escrowAccountSetup(t *testing.T) {
	t.Helper()
	InitEscrowState()
	t.Cleanup(func() { InitEscrowState() })
}

func TestEscrowAddrDeterministicAndUnique(t *testing.T) {
	a1 := EscrowAddr("m1")
	a2 := EscrowAddr("m1")
	b := SettlementAddr("m1")
	if string(a1) != string(a2) {
		t.Fatal("escrow address is not deterministic")
	}
	if len(a1) != 20 || len(b) != 20 {
		t.Fatalf("address must be 20 bytes")
	}
	if string(a1) == string(b) {
		t.Fatal("source escrow and settlement address must differ")
	}
}

func TestEscrowNonceBook(t *testing.T) {
	escrowAccountSetup(t)
	SetTxNonce(7, 3)
	if n, ok := GetTxNonce(7); !ok || n != 3 {
		t.Fatalf("tx nonce mismatch: %d %v", n, ok)
	}
	BumpLastNonce("a", 3)
	BumpLastNonce("a", 2)
	if got := GetLastNonce("a"); got != 3 {
		t.Fatalf("last nonce must be monotonic, got %d", got)
	}
}

func TestEscrowPendingSequenceAndFreeze(t *testing.T) {
	escrowAccountSetup(t)
	p := &PendingRecord{
		MigID:        "m1",
		Account:      "a",
		SourceShard:  0,
		DestShard:    1,
		NextSeq:      1,
		Status:       params.EscrowPrepared,
		Pending:      make(map[string]*big.Int),
		Materialized: make(map[string]bool),
		Spent:        big.NewInt(0),
	}
	PutPendingRecord(p)

	AddPending("m1", "r1", big.NewInt(10))
	MarkMaterialized("m1", "b1")
	AdvancePendingSeq("m1", 1, 1)

	if PendingNextSeq("m1") != 2 || PendingNuDst("m1") != 1 {
		t.Fatalf("pending cursor mismatch")
	}
	if got := PendingSnapshot("m1").Spent; got.Cmp(big.NewInt(10)) != 0 {
		t.Fatalf("pending spent mismatch: %v", got)
	}

	SetPendingSStar("m1", 2, 5)
	if FreezePending("m1", 6, 10) {
		t.Fatal("must wait for S*=2")
	}
	AddPending("m1", "r2", big.NewInt(5))
	AdvancePendingSeq("m1", 2, 2)
	if !FreezePending("m1", 7, 10) {
		t.Fatal("should freeze once contiguous prefix reaches S*")
	}
	snap := PendingSnapshot("m1")
	if snap.SM != 2 || snap.Spent.Cmp(big.NewInt(15)) != 0 {
		t.Fatalf("freeze snapshot mismatch: SM=%d spent=%v", snap.SM, snap.Spent)
	}
}

func TestEscrowTerminalAndFlowGate(t *testing.T) {
	escrowAccountSetup(t)
	PutPendingRecord(&PendingRecord{
		MigID: "m1", Account: "a", SourceShard: 0, DestShard: 1,
		NextSeq: 1, Status: params.EscrowPrepared,
		Pending: make(map[string]*big.Int), Materialized: make(map[string]bool), Spent: big.NewInt(0),
	})
	if !IsFlowBlocked("a") {
		t.Fatal("prepared account must be flow-blocked")
	}
	if AllEscrowTerminal() {
		t.Fatal("undelivered pending must not be terminal")
	}
	FinalizePending("m1", false)
	if !AllEscrowTerminal() {
		t.Fatal("delivered pending with no remaining records must be terminal")
	}
	SetFlowActive("a", true)
	if IsFlowBlocked("a") || !IsFlowActive("a") {
		t.Fatal("flow activation must clear blocked state")
	}
}
