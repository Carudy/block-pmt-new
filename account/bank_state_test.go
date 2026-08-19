package account

import "testing"

func TestBankAddr(t *testing.T) {
	a0 := BankAddrString(0)
	a1 := BankAddrString(1)

	if len(a0) != 40 || len(a1) != 40 {
		t.Fatalf("bank address must be 40 hex chars, got %d and %d", len(a0), len(a1))
	}
	if a0 == a1 {
		t.Fatalf("bank addresses must be unique per shard")
	}
	if len(BankAddrBytes(0)) != 20 {
		t.Fatalf("bank address bytes must be 20 bytes, got %d", len(BankAddrBytes(0)))
	}
}
