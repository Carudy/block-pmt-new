package pbft

import (
	"blockEmulator/params"
	"blockEmulator/utils"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// 可靠投递（仅 preempt_escrow 的 relay 使用）：
// 发送方按 Seq 登记，接收方去重后回 RelayAck；未确认的消息定期重发，
// 超过重试上限后记为 relay_lost，保证“注入 = 提交 + 委托 + 丢弃 + 残留”仍然闭合。

const (
	reliableRetryMS  = 2500
	reliableMaxTries = 8
)

type reliableMsg struct {
	Cmd     command
	Payload []byte
	Addr    string
	Kind    string
	TxIDs   []int
	Sent    int64
	Tries   int
}

var (
	reliableLock    sync.Mutex
	reliableSeq     uint64
	reliablePending map[uint64]*reliableMsg
	reliableStarted bool

	reliableSeen     map[string]bool
	reliableSeenLock sync.Mutex

	reliableLost     []int
	reliableLostLock sync.Mutex
)

func nextReliableSeq() uint64 {
	reliableLock.Lock()
	reliableSeq++
	s := reliableSeq
	reliableLock.Unlock()
	return s
}

// registerReliable 登记一条待确认消息并立即发送一次。
func registerReliable(seq uint64, cmd command, payload []byte, addr, kind string, txIDs []int) {
	if addr == "" {
		return
	}
	reliableLock.Lock()
	if reliablePending == nil {
		reliablePending = make(map[uint64]*reliableMsg)
	}
	reliablePending[seq] = &reliableMsg{
		Cmd: cmd, Payload: payload, Addr: addr, Kind: kind, TxIDs: txIDs,
		Sent: time.Now().UnixMilli(),
	}
	start := !reliableStarted
	reliableStarted = true
	reliableLock.Unlock()

	utils.TcpDial(jointMessage(cmd, payload), addr)
	if start {
		go reliableRetryLoop()
	}
}

func ackReliable(seq uint64) {
	reliableLock.Lock()
	delete(reliablePending, seq)
	reliableLock.Unlock()
}

// reliablePendingCount 返回尚未被确认的可靠消息数（停机前必须为 0）。
func reliablePendingCount() int {
	reliableLock.Lock()
	defer reliableLock.Unlock()
	return len(reliablePending)
}

// seenReliable 返回 true 表示这条消息已经处理过（重传）。
func seenReliable(key string) bool {
	reliableSeenLock.Lock()
	defer reliableSeenLock.Unlock()
	if reliableSeen == nil {
		reliableSeen = make(map[string]bool)
	}
	if reliableSeen[key] {
		return true
	}
	reliableSeen[key] = true
	return false
}

// TakeReliableLost 取走重试耗尽、被判定为丢失的 relay 交易 id。
func TakeReliableLost() []int {
	reliableLostLock.Lock()
	defer reliableLostLock.Unlock()
	if len(reliableLost) == 0 {
		return nil
	}
	out := reliableLost
	reliableLost = nil
	return out
}

func reliableRetryLoop() {
	for {
		time.Sleep(reliableRetryMS * time.Millisecond)
		now := time.Now().UnixMilli()

		type resendItem struct {
			cmd     command
			payload []byte
			addr    string
		}
		var resend []resendItem
		var failed []*reliableMsg

		reliableLock.Lock()
		for seq, m := range reliablePending {
			if now-m.Sent < reliableRetryMS {
				continue
			}
			if m.Tries >= reliableMaxTries {
				failed = append(failed, m)
				delete(reliablePending, seq)
				continue
			}
			m.Sent = now
			m.Tries++
			resend = append(resend, resendItem{cmd: m.Cmd, payload: m.Payload, addr: m.Addr})
		}
		reliableLock.Unlock()

		for _, r := range resend {
			go utils.TcpDial(jointMessage(r.cmd, r.payload), r.addr)
		}
		for _, m := range failed {
			fmt.Printf("[escrow][reliable-lost] kind=%s addr=%s txs=%d\n", m.Kind, m.Addr, len(m.TxIDs))
			if len(m.TxIDs) != 0 {
				reliableLostLock.Lock()
				reliableLost = append(reliableLost, m.TxIDs...)
				reliableLostLock.Unlock()
			}
		}
	}
}

// sendRelayAck 告诉源分片某批 relay 已被接收（仅 escrow）。
func sendRelayAck(fromShard string, seq uint64) {
	if fromShard == "" || seq == 0 {
		return
	}
	b, err := json.Marshal(RelayAck{Seq: seq})
	if err != nil {
		return
	}
	if addr := params.NodeTable[fromShard]["N0"]; addr != "" {
		utils.TcpDial(jointMessage(cRelayAck, b), addr)
	}
}
