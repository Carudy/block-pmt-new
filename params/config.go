package params

import (
	"log"
	"math/big"
	"os"
	"strconv"
)

// Schema 标识迁移期间的方案。4 种方案的迁移流程均为“不停”模式，
// 其中 lb 为全锁（Lock_Acc_When_Migrating=true），其余三种为半锁（false）。
// Normalize() 会据 Schema 派生 Lock_Acc_When_Migrating，Schema 是唯一控制点。
type Schema string

const (
	SchemaLB            Schema = "lb"             // LB-chain：迁移时全锁账户（payer+payee 都锁），其余同 FTL
	SchemaFTL           Schema = "ftl"            // Fine-tuned Lock：迁移中锁 payer、放行 payee；迁移完成注入目的分片后正常打包
	SchemaCaravan       Schema = "caravan"        // FTL + 目的分片打包前把同账户多笔 payer 交易聚合成“单账户→多输出”交易
	SchemaPreempt       Schema = "preempt"        // 银行代付：迁移中源分片聚合 payer 为借款，银行直接付给 payee
	SchemaPreemptEscrow Schema = "preempt_escrow" // 托管本金 + nonce 前缀授权 + pending credit + 结算（新协议，独立于银行版 preempt）
)

// IsLB 判断当前是否 LB-chain（全锁）方案。
func (c *ChainConfig) IsLB() bool { return c.Schema == SchemaLB }

// IsFTL 判断当前是否 FTL 方案（caravan 的迁移/锁定/注入流程与 ftl 相同，但打包阶段多一步聚合）。
func (c *ChainConfig) IsFTL() bool { return c.Schema == SchemaFTL }

// IsCaravan 判断当前是否 Caravan 方案。
func (c *ChainConfig) IsCaravan() bool { return c.Schema == SchemaCaravan }

// IsPreempt 判断当前是否 Preempt（银行代付）方案。
// 注意：新协议 preempt_escrow 使用独立 predicate，绝不能让旧银行路径被新协议触发。
func (c *ChainConfig) IsPreempt() bool { return c.Schema == SchemaPreempt }

// IsPreemptEscrow 判断当前是否为托管本金版 Preempt（新协议）。
func (c *ChainConfig) IsPreemptEscrow() bool { return c.Schema == SchemaPreemptEscrow }

// EscrowStatus 是 preempt_escrow 迁移记录的状态。放在 params 包中以避免 core/account 循环依赖。
type EscrowStatus uint8

const (
	EscrowPrepared EscrowStatus = iota
	EscrowLocked
	EscrowQuiescing
	EscrowFrozen
	EscrowSettled
	EscrowExpired
	EscrowDelivered
	EscrowAborted
)

// String 便于日志输出。
func (s EscrowStatus) String() string {
	switch s {
	case EscrowPrepared:
		return "Prepared"
	case EscrowLocked:
		return "Locked"
	case EscrowQuiescing:
		return "Quiescing"
	case EscrowFrozen:
		return "Frozen"
	case EscrowSettled:
		return "Settled"
	case EscrowExpired:
		return "Expired"
	case EscrowDelivered:
		return "Delivered"
	case EscrowAborted:
		return "Aborted"
	default:
		return "Unknown"
	}
}

// Normalize 以 Schema 为唯一控制点派生迁移开关：
//
//	lb                      -> Lock_Acc_When_Migrating = true
//	ftl / caravan / preempt -> Lock_Acc_When_Migrating = false
func (c *ChainConfig) Normalize() {
	switch c.Schema {
	case SchemaLB:
		c.Lock_Acc_When_Migrating = true
	case SchemaFTL, SchemaCaravan, SchemaPreempt, SchemaPreemptEscrow:
		c.Lock_Acc_When_Migrating = false
	}
}

type ChainConfig struct {
	ChainID                  int
	NodeID                   string
	ShardID                  string
	Shard_num                int
	Malicious_num            int    // per shard
	Path                     string // input file path
	Block_interval           int    // second
	MaxBlockSize             int
	MaxMigSize               int
	MaxMig2Size              int
	MaxMig1Size              int
	MaxAnnSize               int
	MaxCapSize               int
	Relay_interval           int
	MaxRelayBlockSize        int
	MinRelayBlockSize        int
	Inject_speed             int // tx count per second
	Max_Commit               int // 确认多少个交易后就迁移
	Max_Commit_Block         int // 确认多少个区块后就迁移，要×分片数量
	ClientSendTX             bool
	Stop_When_Migrating      bool //  迁移时是停止还是继续运行
	Lock_Acc_When_Migrating  bool //迁移时相关账户是否要锁住
	Bu_Tong_Bi_Li            bool //进行相关交易不同占比的实验, 地址：489338d5e8d42e8c923d1f47361d979503d4ad68
	Bu_Tong_Bi_Li_2          bool //进行相关交易不同占比的实验, 地址有10个
	Bu_Tong_Shi_Jian         bool //进行迁移请求不同时间被打包的实验, 地址：489338d5e8d42e8c923d1f47361d979503d4ad68
	Bu_Tong_Shi_Jian_Jian_Ge int  //进行迁移请求不同时间被打包的实验, 设置被打包的时间
	Fail                     bool //进行迁移迁移失败的实验, 地址：489338d5e8d42e8c923d1f47361d979503d4ad68
	Fail_Time                int  //进行迁移迁移失败的实验, 设置失败的时间
	Cross_Chain              bool //进行跨链迁移的实验, 地址：489338d5e8d42e8c923d1f47361d979503d4ad68
	Algorithm                bool
	Pressure                 bool   //压力测试，迁移多个账户
	PorC                     string //PageRank or CLPA
	MigrateBeforeInject      bool   //一开始就迁移
	OnlyOnce                 int    //1只迁移1次，否则设为1000000
	Not_Lock_immediately     bool
	RelayLock                bool

	// 协议方案：lb / ftl / caravan / preempt / preempt_escrow。
	Schema Schema
	// 仅 preempt 使用：本分片银行账户的初始余额（视为“无限”）。
	BankInitialBalance *big.Int

	// 仅 preempt_escrow 使用：创世时每个账户的真实初始余额。
	// 不要使用 10^60 之类的“无限余额”，否则 total_cons <= n 永远不会触发。
	EscrowInitBalance *big.Int
	// preempt_escrow：迁移锁定的源分片超时块数（h_lock + MigrationTimeoutBlocks）。
	MigrationTimeoutBlocks int
	// preempt_escrow：目的分片收到 TXmig2 后等待多少块再发起 Quiesce，
	// 给源分片留出 nonce 前缀授权窗口。
	QuiesceDelayBlocks int
	// preempt_escrow：目的分片等待缺失批次的最长块数。
	SettlementWaitBlocks int
	// preempt_escrow：单个 EscrowBatch 最多包含的原始交易笔数。
	MaxEscrowBatchTxs int
	// preempt_escrow：gated nonce 缺口超过该块数后跳过缺失 nonce，避免实验永久卡住。
	// 0 表示禁用（严格 nonce 语义）。可信原型推荐保留一个有限值。
	EscrowGapRepairBlocks int
}

// envInt 读取环境变量为 int，失败则返回默认值。
func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

var (
	BaseIP = "127.0.0.1"
	// StartPort 为端口基址：client 用 StartPort；节点(s,n) 用 StartPort+1+s*num_nodes+n（紧凑连续分配）。
	// 可由环境变量 PORT_BASE 覆盖（run.sh 会自动挑选空闲端口段并导出）。
	StartPort  = envInt("PORT_BASE", 32000)
	ClientAddr = "127.0.0.1:" + strconv.Itoa(StartPort)

	// Configuration for auto-generation
	N_SHARDS = 2
	N_NODES  = 1

	// Auto-generated NodeTable
	NodeTable = func() map[string]map[string]string {
		nodeTable := make(map[string]map[string]string)
		for s := 0; s < N_SHARDS; s++ {
			shardName := "S" + strconv.Itoa(s)
			nodeTable[shardName] = make(map[string]string)
			for n := 0; n < N_NODES; n++ {
				nodeName := "N" + strconv.Itoa(n)
				port := StartPort + 1 + s*N_NODES + n
				nodeTable[shardName][nodeName] = BaseIP + ":" + strconv.Itoa(port)
			}
		}
		return nodeTable
	}()

	// Auto-generated ShardTable
	ShardTable = func() map[string]int {
		shardTable := make(map[string]int)
		for s := 0; s < N_SHARDS; s++ {
			shardName := "S" + strconv.Itoa(s)
			shardTable[shardName] = s
		}
		return shardTable
	}()

	// Auto-generated ShardTableInt2Str
	ShardTableInt2Str = func() map[int]string {
		shardTableInt2Str := make(map[int]string)
		for s := 0; s < N_SHARDS; s++ {
			shardName := "S" + strconv.Itoa(s)
			shardTableInt2Str[s] = shardName
		}
		return shardTableInt2Str
	}()

	Config = &ChainConfig{
		ChainID:           77,
		Block_interval:    6,
		MaxBlockSize:      2000,
		MaxMigSize:        1000,
		MaxMig2Size:       500,
		MaxMig1Size:       500,
		MaxAnnSize:        500,
		MaxCapSize:        500,
		MaxRelayBlockSize: 10,
		MinRelayBlockSize: 1,
		Inject_speed:      2000,
		// Inject_speed:     2000,
		Relay_interval:   1000,
		Max_Commit:       100000,
		Max_Commit_Block: 10,
		// Max_Commit_Block:         30,
		ClientSendTX:             true,
		Stop_When_Migrating:      false,
		Lock_Acc_When_Migrating:  false,
		Bu_Tong_Bi_Li:            false,
		Bu_Tong_Bi_Li_2:          true,
		Bu_Tong_Shi_Jian:         false,
		Bu_Tong_Shi_Jian_Jian_Ge: 3,
		Fail:                     false,
		Fail_Time:                8,
		Cross_Chain:              false,
		Algorithm:                false,
		Pressure:                 false,
		PorC:                     "CLPA",
		MigrateBeforeInject:      false,
		OnlyOnce:                 100,
		Not_Lock_immediately:     true,
		RelayLock:                true,

		// Schema: SchemaLB,
		// Schema: SchemaFTL,
		// Schema: SchemaCaravan,
		// Schema: SchemaPreemptEscrow,
		Schema: SchemaPreempt,
		BankInitialBalance: new(big.Int).Exp(big.NewInt(10), big.NewInt(60), nil), // 10^60, 视为“无限”

		// preempt_escrow 默认值。
		// 20W.csv 中单账户总流出最大约 5.98e24；这里用 1e26 保证旧实验默认不会因普通余额不足卡死。
		// 要验证 total_cons <= n 的论文语义，请通过 set_config/配置将该值调小或使用校准后的数据集。
		EscrowInitBalance:      new(big.Int).Exp(big.NewInt(10), big.NewInt(26), nil),
		MigrationTimeoutBlocks: 10,
		QuiesceDelayBlocks:     3,
		SettlementWaitBlocks:   2,
		MaxEscrowBatchTxs:      200,
		EscrowGapRepairBlocks:  30,
	}

	Init_addrs          = []string{}
	Init_balance string = "1000000000000000000000000000000000000000000000000000000000000" // 10^60，视为“无限”
)

func RenewShardTable(num_shards, num_nodes_per_shard int) {
	N_SHARDS = num_shards
	N_NODES = num_nodes_per_shard

	log.Printf("Renewing ShardTable and NodeTable with %d shards and %d nodes per shard", num_shards, num_nodes_per_shard)

	NodeTable = make(map[string]map[string]string)
	for s := 0; s < num_shards; s++ {
		shardName := "S" + strconv.Itoa(s)
		NodeTable[shardName] = make(map[string]string)
		for n := 0; n < num_nodes_per_shard; n++ {
			nodeName := "N" + strconv.Itoa(n)
			port := StartPort + 1 + s*num_nodes_per_shard + n
			NodeTable[shardName][nodeName] = BaseIP + ":" + strconv.Itoa(port)
		}
	}

	ShardTable = func() map[string]int {
		shardTable := make(map[string]int)
		for s := 0; s < N_SHARDS; s++ {
			shardName := "S" + strconv.Itoa(s)
			shardTable[shardName] = s
		}
		return shardTable
	}()

	ShardTableInt2Str = func() map[int]string {
		shardTableInt2Str := make(map[int]string)
		for s := 0; s < N_SHARDS; s++ {
			shardName := "S" + strconv.Itoa(s)
			shardTableInt2Str[s] = shardName
		}
		return shardTableInt2Str
	}()
}

// init 在包加载完成后立即据 Schema 归一化迁移开关（Schema 是唯一控制点）。
func init() {
	Config.Normalize()
}
