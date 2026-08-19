package core

import (
	"blockEmulator/account"
	"encoding/json"
	"math/big"
	"testing"
)

func TestCanBorrow(t *testing.T) {
	account.Credit = map[string]*big.Int{"a": big.NewInt(100)}
	account.Loan = map[string]*big.Int{"a": big.NewInt(30)}

	if !canBorrow("a", big.NewInt(50), nil) {
		t.Fatal("should allow borrow when loan+value <= credit")
	}
	if canBorrow("a", big.NewInt(71), nil) {
		t.Fatal("should reject borrow when loan+value > credit")
	}

	// reserved tracks intra-block reservations
	reserved := map[string]*big.Int{"a": big.NewInt(40)}
	if !canBorrow("a", big.NewInt(30), reserved) {
		t.Fatal("30+30+40 == 100 should pass")
	}
	if canBorrow("a", big.NewInt(31), reserved) {
		t.Fatal("30+31+40 > 100 should fail")
	}

	// unknown account has no credit line
	if canBorrow("b", big.NewInt(1), nil) {
		t.Fatal("unknown account should fail")
	}
}

func TestTXsettleRoundTrip(t *testing.T) {
	s := &TXsettle{FromShardID: 1, ToShardID: 0, Amount: big.NewInt(12345)}
	dec := DecodeTXsettle(s.Encode())
	if dec.FromShardID != 1 || dec.ToShardID != 0 || dec.Amount.Cmp(big.NewInt(12345)) != 0 {
		t.Fatalf("TXsettle gob round-trip mismatch: %+v", dec)
	}
}

func TestTXnsDebtRoundTrip(t *testing.T) {
	ns := &TXns{Address: "abc", Change: big.NewInt(50), Debt: big.NewInt(20), NumTxs: 7}
	dec := DecodeTXns(ns.Encode())
	if dec.Change.Cmp(big.NewInt(50)) != 0 || dec.Debt.Cmp(big.NewInt(20)) != 0 || dec.NumTxs != 7 {
		t.Fatalf("TXns gob round-trip mismatch: change=%v debt=%v numtxs=%v", dec.Change, dec.Debt, dec.NumTxs)
	}
}

func TestTXnsDebtJSONRoundTrip(t *testing.T) {
	ns := &TXns{Address: "abc", Change: big.NewInt(50), Debt: big.NewInt(20), NumTxs: 7}
	b, err := json.Marshal(ns)
	if err != nil {
		t.Fatal(err)
	}
	var ns2 TXns
	if err := json.Unmarshal(b, &ns2); err != nil {
		t.Fatal(err)
	}
	if ns2.Debt == nil || ns2.Debt.Cmp(big.NewInt(20)) != 0 {
		t.Fatalf("TXns debt JSON round-trip failed: %v", ns2.Debt)
	}
	if ns2.Change == nil || ns2.Change.Cmp(big.NewInt(50)) != 0 {
		t.Fatalf("TXns change JSON round-trip failed: %v", ns2.Change)
	}
	if ns2.NumTxs != 7 {
		t.Fatalf("TXns numtxs JSON round-trip failed: %v", ns2.NumTxs)
	}
}
