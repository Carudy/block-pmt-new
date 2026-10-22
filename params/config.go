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
	SchemaLB      Schema = "lb"      // LB-chain：迁移时全锁账户（payer+payee 都锁），其余同 FTL
	SchemaFTL     Schema = "ftl"     // Fine-tuned Lock：迁移中锁 payer、放行 payee；迁移完成注入目的分片后正常打包
	SchemaCaravan Schema = "caravan" // FTL + 目的分片打包前把同账户多笔 payer 交易聚合成“单账户→多输出”交易
	SchemaPreempt Schema = "preempt" // 银行代付：迁移中源分片聚合 payer 为借款，银行直接付给 payee
)

// IsLB 判断当前是否 LB-chain（全锁）方案。
func (c *ChainConfig) IsLB() bool { return c.Schema == SchemaLB }

// IsFTL 判断当前是否 FTL 方案（caravan 的迁移/锁定/注入流程与 ftl 相同，但打包阶段多一步聚合）。
func (c *ChainConfig) IsFTL() bool { return c.Schema == SchemaFTL }

// IsCaravan 判断当前是否 Caravan 方案。
func (c *ChainConfig) IsCaravan() bool { return c.Schema == SchemaCaravan }

// IsPreempt 判断当前是否 Preempt（银行代付）方案。
func (c *ChainConfig) IsPreempt() bool { return c.Schema == SchemaPreempt }

// Normalize 以 Schema 为唯一控制点派生迁移开关：
//
//	lb                      -> Lock_Acc_When_Migrating = true
//	ftl / caravan / preempt -> Lock_Acc_When_Migrating = false
func (c *ChainConfig) Normalize() {
	switch c.Schema {
	case SchemaLB:
		c.Lock_Acc_When_Migrating = true
	case SchemaFTL, SchemaCaravan, SchemaPreempt:
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

	// 协议方案：lb / ftl / caravan / preempt。取代原来的 EnableBankMechanism 开关。
	Schema Schema
	// 仅 preempt 使用：本分片银行账户的初始余额（视为“无限”）。
	BankInitialBalance *big.Int
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
		Schema: SchemaPreempt,
		BankInitialBalance: new(big.Int).Exp(big.NewInt(10), big.NewInt(60), nil), // 10^60, 视为“无限”
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
