package account

import (
	"blockEmulator/params"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"sync"
)

// 本文件中的状态仅在 SchemaPreemptEscrow 下使用。
// 旧 schema 不读写这些 map，因此不会影响 lb/ftl/caravan/preempt。

type EscrowRecord struct {
	MigID        string
	Account      string
	SourceShard  int
	DestShard    int
	Credit       *big.Int // 锁定时的真实余额 n
	SeqCons      uint64   // 已授权交易笔数
	TotalCons    *big.Int // 已授权金额 total_cons
	Nu0          uint64   // 锁定时账户 nonce
	NuCons       uint64   // 已授权的最后一个 nonce
	HLock        int      // 锁定块高
	Status       params.EscrowStatus
	SOwn         int // Settled -> dest; Expired -> source
	NuDst        uint64
	Spent        *big.Int
	FreezeHeight int
}

type PendingRecord struct {
	MigID            string
	Account          string
	SourceShard      int
	DestShard        int
	NextSeq          uint64 // 下一个期望的连续 seq（从 1 开始）
	NuDst            uint64
	SStar            uint64
	SM               uint64
	Status           params.EscrowStatus
	PreparedHeight   int
	QuiesceRequested bool
	QuiesceReceived  bool
	QuiesceHeight    int
	FreezeHeight     int
	SettleSent       bool
	Pending          map[string]*big.Int // recipient(hex) -> amount
	Materialized     map[string]bool     // batch hash(hex) -> recorded
	Spent            *big.Int            // 连续前缀批次金额之和
	PendingTxs       int                 // 已记录但尚未交付的委托交易笔数
	Delivered        bool
}

var (
	EscrowLock        sync.Mutex
	EscrowRecords     map[string]*EscrowRecord // migID -> record
	EscrowLocked      map[string]string        // account -> migID
	EscrowQuiesceReqs map[string]bool          // migID -> request pending
	EscrowTerminal    map[string]bool          // migID -> terminal

	PendingLock    sync.Mutex
	PendingRecords map[string]*PendingRecord // migID -> pending
	FlowBlocked    map[string]bool           // account -> outgoing blocked
	FlowActive     map[string]bool           // account -> post-cutover nonce gate active

	NonceBookLock sync.Mutex
	NonceByTxID   map[int]uint64 // tx.Id -> sender nonce
	LastNonce     map[string]uint64
)

// InitEscrowState 在每个节点启动时调用。旧 schema 也会初始化，但不被使用。
func InitEscrowState() {
	EscrowLock.Lock()
	EscrowRecords = make(map[string]*EscrowRecord)
	EscrowLocked = make(map[string]string)
	EscrowQuiesceReqs = make(map[string]bool)
	EscrowTerminal = make(map[string]bool)
	EscrowLock.Unlock()

	PendingLock.Lock()
	PendingRecords = make(map[string]*PendingRecord)
	FlowBlocked = make(map[string]bool)
	FlowActive = make(map[string]bool)
	PendingLock.Unlock()

	NonceBookLock.Lock()
	NonceByTxID = make(map[int]uint64)
	LastNonce = make(map[string]uint64)
	NonceBookLock.Unlock()
}

// EscrowAddr 返回某个 migID 的托管账户地址（20 字节，trie key）。
func EscrowAddr(migID string) []byte {
	h := sha256.Sum256([]byte("blockEmulator-escrow-src:" + migID))
	return h[:20]
}

// SettlementAddr 返回某个 migID 在目的分片的结算账户地址（20 字节）。
func SettlementAddr(migID string) []byte {
	h := sha256.Sum256([]byte("blockEmulator-escrow-dst:" + migID))
	return h[:20]
}

// EscrowMigID 生成确定性的迁移实例 ID。attempt 用于未来重试/新实例；当前为 0。
func EscrowMigID(account string, source, dest, hlock, attempt int) string {
	return fmt.Sprintf("%s:%d:%d:%d:%d", account, source, dest, hlock, attempt)
}

// ---------- Nonce ----------

func SetTxNonce(txID int, nonce uint64) {
	NonceBookLock.Lock()
	NonceByTxID[txID] = nonce
	NonceBookLock.Unlock()
}

func GetTxNonce(txID int) (uint64, bool) {
	NonceBookLock.Lock()
	defer NonceBookLock.Unlock()
	n, ok := NonceByTxID[txID]
	return n, ok
}

func SetLastNonce(addr string, nonce uint64) {
	NonceBookLock.Lock()
	LastNonce[addr] = nonce
	NonceBookLock.Unlock()
}

func GetLastNonce(addr string) uint64 {
	NonceBookLock.Lock()
	defer NonceBookLock.Unlock()
	return LastNonce[addr]
}

func BumpLastNonce(addr string, nonce uint64) {
	NonceBookLock.Lock()
	if nonce > LastNonce[addr] {
		LastNonce[addr] = nonce
	}
	NonceBookLock.Unlock()
}

// ---------- Escrow record ----------

func PutEscrowRecord(r *EscrowRecord) {
	EscrowLock.Lock()
	EscrowRecords[r.MigID] = r
	if r.Status == params.EscrowLocked || r.Status == params.EscrowQuiescing {
		EscrowLocked[r.Account] = r.MigID
	}
	EscrowLock.Unlock()
}

func GetEscrowRecord(migID string) *EscrowRecord {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	return EscrowRecords[migID]
}

func GetEscrowRecordByAccount(addr string) *EscrowRecord {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	id := EscrowLocked[addr]
	if id == "" {
		return nil
	}
	return EscrowRecords[id]
}

func LockedMigID(addr string) string {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	return EscrowLocked[addr]
}

func DeleteEscrowLock(addr string) {
	EscrowLock.Lock()
	delete(EscrowLocked, addr)
	EscrowLock.Unlock()
}

func SetEscrowStatus(migID string, status params.EscrowStatus) {
	EscrowLock.Lock()
	if r := EscrowRecords[migID]; r != nil {
		r.Status = status
		if status == params.EscrowSettled || status == params.EscrowAborted || status == params.EscrowDelivered {
			delete(EscrowLocked, r.Account)
		}
	}
	EscrowLock.Unlock()
}

func RequestQuiesce(migID string) {
	EscrowLock.Lock()
	EscrowQuiesceReqs[migID] = true
	if r := EscrowRecords[migID]; r != nil && r.Status == params.EscrowLocked {
		r.Status = params.EscrowQuiescing
	}
	EscrowLock.Unlock()
}

func TakeQuiesceRequests() []string {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	out := make([]string, 0, len(EscrowQuiesceReqs))
	for id := range EscrowQuiesceReqs {
		out = append(out, id)
	}
	EscrowQuiesceReqs = make(map[string]bool)
	return out
}

func MarkTerminal(migID string) {
	EscrowLock.Lock()
	EscrowTerminal[migID] = true
	EscrowLock.Unlock()
}

// AllEscrowRecords 返回当前记录快照（指针只读；调用方不要直接修改并假定并发安全）。
func AllEscrowRecords() []*EscrowRecord {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	out := make([]*EscrowRecord, 0, len(EscrowRecords))
	for _, r := range EscrowRecords {
		out = append(out, r)
	}
	return out
}

// AllEscrowTerminal 判断本分片是否已无未完成迁移。旧 schema 不使用。
func AllEscrowTerminal() bool {
	EscrowLock.Lock()
	defer EscrowLock.Unlock()
	for _, r := range EscrowRecords {
		if r.Status != params.EscrowSettled && r.Status != params.EscrowExpired {
			return false
		}
	}
	PendingLock.Lock()
	defer PendingLock.Unlock()
	for _, p := range PendingRecords {
		if !p.Delivered {
			return false
		}
	}
	return true
}

// ---------- Pending / flow ----------

func PutPendingRecord(p *PendingRecord) {
	PendingLock.Lock()
	PendingRecords[p.MigID] = p
	FlowBlocked[p.Account] = true
	PendingLock.Unlock()
}

func GetPendingRecord(migID string) *PendingRecord {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	return PendingRecords[migID]
}

func AllPendingRecords() []*PendingRecord {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	out := make([]*PendingRecord, 0, len(PendingRecords))
	for _, p := range PendingRecords {
		out = append(out, p)
	}
	return out
}

// CountPending 返回尚未交付（!Delivered）的迁移实例数，
// 以及这些实例中已记录但尚未交付的 pending credit 笔数。
// 已交付的实例不再计入，避免队列监控列被历史实例数污染。
func CountPending() (records int, credits int) {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	for _, p := range PendingRecords {
		if p.Delivered {
			continue
		}
		records++
		credits += p.PendingTxs
	}
	return records, credits
}

func GetPendingRecordByAccount(addr string) *PendingRecord {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	for _, p := range PendingRecords {
		if p.Account == addr {
			return p
		}
	}
	return nil
}

func IsFlowBlocked(addr string) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	return FlowBlocked[addr]
}

func IsFlowActive(addr string) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	return FlowActive[addr]
}

// InLocalEscrowFlow 报告 addr 是否正被本分片的 escrow 状态跟踪：
// 源分片上的锁定账户，或目的分片上已 prepare 但尚未 Debit 的账户。
// 用于在交接窗口内保留交易，避免注入路径把它们当成“别人的交易”丢掉。
// 两步串行加锁，不嵌套两把锁，避免与其它路径形成锁序问题。
func InLocalEscrowFlow(addr string) bool {
	if LockedMigID(addr) != "" {
		return true
	}
	PendingLock.Lock()
	defer PendingLock.Unlock()
	return FlowBlocked[addr] || FlowActive[addr]
}

func SetFlowActive(addr string, active bool) {
	PendingLock.Lock()
	if active {
		FlowActive[addr] = true
		delete(FlowBlocked, addr)
	} else {
		delete(FlowActive, addr)
	}
	PendingLock.Unlock()
}

func AddPending(migID, recipient string, value *big.Int) {
	PendingLock.Lock()
	p := PendingRecords[migID]
	if p != nil {
		if p.Pending[recipient] == nil {
			p.Pending[recipient] = big.NewInt(0)
		}
		p.Pending[recipient].Add(p.Pending[recipient], value)
		if p.Spent == nil {
			p.Spent = big.NewInt(0)
		}
		p.Spent.Add(p.Spent, value)
	}
	PendingLock.Unlock()
}

// AddPendingTxs 记录一个批次中实际写入 pending 账本的交易笔数。
func AddPendingTxs(migID string, n int) {
	if n <= 0 {
		return
	}
	PendingLock.Lock()
	if p := PendingRecords[migID]; p != nil {
		p.PendingTxs += n
	}
	PendingLock.Unlock()
}

func MarkMaterialized(migID, batchID string) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil || p.Materialized[batchID] {
		return false
	}
	p.Materialized[batchID] = true
	return true
}

func AdvancePendingSeq(migID string, seqEnd, nuEnd uint64) {
	PendingLock.Lock()
	if p := PendingRecords[migID]; p != nil {
		p.NextSeq = seqEnd + 1
		if nuEnd > p.NuDst {
			p.NuDst = nuEnd
		}
	}
	PendingLock.Unlock()
}

func MarkPendingDelivered(migID string) {
	PendingLock.Lock()
	if p := PendingRecords[migID]; p != nil {
		p.Delivered = true
	}
	PendingLock.Unlock()
}

func FinalizePending(migID string, aborted bool) {
	PendingLock.Lock()
	if p := PendingRecords[migID]; p != nil {
		p.Delivered = true
		if aborted {
			p.Status = params.EscrowAborted
		} else {
			p.Status = params.EscrowDelivered
		}
	}
	PendingLock.Unlock()
}

func TakePending(migID string) map[string]*big.Int {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil {
		return nil
	}
	out := p.Pending
	p.Pending = make(map[string]*big.Int)
	return out
}

// SnapshotPending 返回 pending 的副本，不清空（供 commit==0 生成状态根）。
func SnapshotPending(migID string) map[string]*big.Int {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil {
		return nil
	}
	out := make(map[string]*big.Int, len(p.Pending))
	for k, v := range p.Pending {
		out[k] = new(big.Int).Set(v)
	}
	return out
}

func PendingNuDst(migID string) uint64 {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	if p := PendingRecords[migID]; p != nil {
		return p.NuDst
	}
	return 0
}

func PendingNextSeq(migID string) uint64 {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	if p := PendingRecords[migID]; p != nil {
		return p.NextSeq
	}
	return 1
}

func SetPendingSStar(migID string, sstar uint64, height int) {
	PendingLock.Lock()
	if p := PendingRecords[migID]; p != nil {
		p.SStar = sstar
		p.QuiesceReceived = true
		p.QuiesceHeight = height
	}
	PendingLock.Unlock()
}

func MarkQuiesceRequested(migID string, height, delay int) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil || p.QuiesceRequested {
		return false
	}
	if height < p.PreparedHeight+delay {
		return false
	}
	p.QuiesceRequested = true
	return true
}

func MarkSettleSent(migID string) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil || p.SettleSent {
		return false
	}
	p.SettleSent = true
	return true
}

// PendingSnapshot 返回一个只读副本，供结算消息构造使用。
func PendingSnapshot(migID string) *PendingRecord {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil {
		return nil
	}
	cp := *p
	cp.Pending = make(map[string]*big.Int, len(p.Pending))
	for k, v := range p.Pending {
		cp.Pending[k] = new(big.Int).Set(v)
	}
	cp.Materialized = make(map[string]bool, len(p.Materialized))
	for k, v := range p.Materialized {
		cp.Materialized[k] = v
	}
	if p.Spent != nil {
		cp.Spent = new(big.Int).Set(p.Spent)
	}
	return &cp
}

// FreezePending 返回是否已经可以冻结（连续前缀到 S* 或等待超过 wait）。
func FreezePending(migID string, height, wait int) bool {
	PendingLock.Lock()
	defer PendingLock.Unlock()
	p := PendingRecords[migID]
	if p == nil || !p.QuiesceReceived || p.Status == params.EscrowFrozen {
		return false
	}
	recorded := uint64(0)
	if p.NextSeq > 0 {
		recorded = p.NextSeq - 1
	}
	if recorded >= p.SStar || (p.QuiesceHeight > 0 && height >= p.QuiesceHeight+wait) {
		p.SM = recorded
		p.Status = params.EscrowFrozen
		p.FreezeHeight = height
		return true
	}
	return false
}

func HexAddr(b []byte) string { return hex.EncodeToString(b) }
