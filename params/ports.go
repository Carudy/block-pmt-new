package params

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
)

// FindFreePortBase 寻找一个可用端口基址，满足：
//  1. client 端口(base) 与所有节点端口(base+1+s*numNodes+n) 都能被真正 bind（不被任何进程占用）；
//  2. 整个端口段避开内核的临时端口(ephemeral)范围，避免与任意进程出站连接所用的临时端口冲突。
//
// 判定用“真正尝试 bind”，因此端口被遗留节点、其他服务、其他用户占用都能被测出。
func FindFreePortBase(numShards, numNodes, firstBase int) (int, error) {
	span := numShards*numNodes + 1 // 需要的端口跨度（base 本身 + 所有节点端口）

	elo, ehi, ok := ephemeralPortRange()
	if !ok {
		elo, ehi = 32768, 60999 // 保守默认
	}

	// 候选起始点：优先沿用 firstBase（若其端口段完整落在 ephemeral 之外），
	// 否则退到 ephemeral 之下的常用区间，最后退到 ephemeral 之上。
	var ordered []int
	seen := map[int]bool{}
	add := func(lo int) {
		if lo < 1024 {
			lo = 1024
		}
		if seen[lo] {
			return
		}
		if lo+span-1 < elo || lo > ehi { // 端口段必须完整位于 ephemeral 之外
			seen[lo] = true
			ordered = append(ordered, lo)
		}
	}
	add(firstBase)
	for _, lo := range []int{20000, 16000, 12000, 8000, 5000, 1024} {
		add(lo)
	}
	add(ehi + 1)

	for _, lo := range ordered {
		hi := 65535
		if lo < elo {
			hi = elo - 1
		}
		for base := lo; base+span-1 <= hi; base += 100 {
			if portsBindable(numShards, numNodes, base) {
				return base, nil
			}
		}
	}
	return 0, fmt.Errorf("找不到可用端口段（需求跨度 %d，临时端口范围 %d-%d）；请清理占用端口的进程或减少分片/节点数", span, elo, ehi)
}

// portsBindable 依次尝试绑定所需全部端口；全部成功则返回 true（随后释放）。
func portsBindable(numShards, numNodes, base int) bool {
	var listeners []net.Listener
	defer func() {
		for _, l := range listeners {
			_ = l.Close()
		}
	}()

	tryBind := func(port int) bool {
		l, err := net.Listen("tcp", fmt.Sprintf("%s:%d", BaseIP, port))
		if err != nil {
			return false
		}
		listeners = append(listeners, l)
		return true
	}

	if !tryBind(base) {
		return false
	}
	for s := 0; s < numShards; s++ {
		for n := 0; n < numNodes; n++ {
			if !tryBind(base + 1 + s*numNodes + n) {
				return false
			}
		}
	}
	return true
}

// ephemeralPortRange 读取内核临时端口范围（Linux：/proc；读取失败返回 ok=false）。
func ephemeralPortRange() (int, int, bool) {
	data, err := os.ReadFile("/proc/sys/net/ipv4/ip_local_port_range")
	if err != nil {
		return 0, 0, false
	}
	f := strings.Fields(string(data))
	if len(f) != 2 {
		return 0, 0, false
	}
	lo, err1 := strconv.Atoi(f[0])
	hi, err2 := strconv.Atoi(f[1])
	if err1 != nil || err2 != nil || lo <= 0 || hi <= lo {
		return 0, 0, false
	}
	return lo, hi, true
}
