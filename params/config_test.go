package params

import (
	"strconv"
	"testing"
)

// Schema 是唯一控制点：lb -> 全锁；ftl/caravan/preempt -> 半锁。
func TestNormalizeLockFlag(t *testing.T) {
	cases := []struct {
		schema Schema
		lock   bool
	}{
		{SchemaLB, true},
		{SchemaFTL, false},
		{SchemaCaravan, false},
		{SchemaPreempt, false},
	}
	for _, tc := range cases {
		// 故意把初值设为相反值，验证 Normalize 会覆盖它。
		c := &ChainConfig{Schema: tc.schema, Lock_Acc_When_Migrating: !tc.lock}
		c.Normalize()
		if c.Lock_Acc_When_Migrating != tc.lock {
			t.Fatalf("schema %q: expected Lock_Acc_When_Migrating=%v, got %v",
				tc.schema, tc.lock, c.Lock_Acc_When_Migrating)
		}
	}
}

func TestPortLayoutCompact(t *testing.T) {
	// 紧凑端口分配：client=StartPort；节点(s,n)=StartPort+1+s*num_nodes+n
	const shards, nodes = 3, 4
	RenewShardTable(shards, nodes)
	defer RenewShardTable(2, 1)

	want := StartPort + 1
	for s := 0; s < shards; s++ {
		for n := 0; n < nodes; n++ {
			got := NodeTable["S"+strconv.Itoa(s)]["N"+strconv.Itoa(n)]
			expect := BaseIP + ":" + strconv.Itoa(want)
			if got != expect {
				t.Fatalf("node S%d_N%d: got %s, want %s", s, n, got, expect)
			}
			want++
		}
	}
	if want-1 != StartPort+shards*nodes {
		t.Fatalf("highest node port = %d, want %d", want-1, StartPort+shards*nodes)
	}
}

func TestSchemaPredicates(t *testing.T) {
	checks := []struct {
		schema  Schema
		lb      bool
		ftl     bool
		caravan bool
		preempt bool
	}{
		{SchemaLB, true, false, false, false},
		{SchemaFTL, false, true, false, false},
		{SchemaCaravan, false, false, true, false},
		{SchemaPreempt, false, false, false, true},
	}
	for _, c := range checks {
		cfg := &ChainConfig{Schema: c.schema}
		if cfg.IsLB() != c.lb || cfg.IsFTL() != c.ftl || cfg.IsCaravan() != c.caravan || cfg.IsPreempt() != c.preempt {
			t.Fatalf("predicates mismatch for schema %q", c.schema)
		}
	}
}
