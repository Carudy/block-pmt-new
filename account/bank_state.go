package account

import (
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"sync"
)

// 以下两个 map 用于银行机制（borrow-during-migration）。
// 只对正在迁出的账户维护，账户完成迁移（Announce）后即删除。

var CreditLock sync.Mutex

// 正在迁出账户的可用信用额度 = 冻结余额快照 + 迁移窗口内收到的入账。
var Credit map[string]*big.Int

var LoanLock sync.Mutex

// 正在迁出账户从银行借出的金额 = 迁移窗口内由银行代为支付的出账总和。
var Loan map[string]*big.Int

var PayerTxCountLock sync.Mutex

// 正在迁出账户在迁移窗口内由银行代付的 payer 交易数量（用于统计原始交易笔数）。
var PayerTxCount map[string]int

// BankAddrString 返回指定分片银行账户的地址字符串（40 个十六进制字符，20 字节）。
// 前缀固定为 36 个 '0' + "ba"，与普通账户地址（来自数据集）不冲突。
func BankAddrString(shardID int) string {
	return strings.Repeat("0", 36) + "ba" + fmt.Sprintf("%02x", shardID)
}

// BankAddrBytes 返回银行账户的 20 字节地址（用于 trie key）。
func BankAddrBytes(shardID int) []byte {
	b, _ := hex.DecodeString(BankAddrString(shardID))
	return b
}
