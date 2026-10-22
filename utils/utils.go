package utils

import (
	"blockEmulator/params"
	"encoding/binary"
	"fmt"
	"log"
	"math/rand"
	"net"
	"strconv"
	"sync"
)

var (
	connMaplock sync.Mutex
	connMap     map[string]net.Conn
	// writeLocks 按对端地址给写入加锁，避免多 goroutine 并发写同一连接导致长度前缀粘包错乱。
	writeLocks map[string]*sync.Mutex
)

func Addr2Shard(senderAddr string) int {
	// 只取地址后五位已绝对够用
	senderAddr = senderAddr[len(senderAddr)-5:]
	num, err := strconv.ParseInt(senderAddr, 16, 32)
	// num, err := strconv.ParseInt(senderAddr, 10, 32)
	if err != nil {
		log.Panic()
	}
	return int(num) % params.Config.Shard_num
}

func Min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// // 使用tcp发送消息, \n 作为结束符
// func TcpDial(context []byte, addr string) {
// 	// 给map加锁
// 	connMaplock.Lock()
// 	if connMap == nil {
// 		connMap = make(map[string]net.Conn)
// 	}
// 	// 没有连接则先建立连接
// 	if _, ok := connMap[addr]; !ok {
// 		conn, err := net.Dial("tcp", addr)
// 		if err != nil {
// 			log.Println("connect error", err)
// 			return
// 		}
// 		connMap[addr] = conn
// 	}

// 	conn := connMap[addr]
// 	// 给map解锁
// 	connMaplock.Unlock()

// 	// 结束字符，读数据时读到 \n 为止
// 	_, err := conn.Write(append(context, '\n'))
// 	if err != nil {
// 		log.Fatal(err)

// 	}

// }

// 使用tcp发送消息，利用长度前缀防止粘包
//
// 发送失败不再直接 log.Fatal 终止进程：缓存的连接可能因对端重启而失效，
// 此时丢弃该连接并用新连接重试一次；仍失败只记日志（保持原有“丢消息”的降级行为）。
func TcpDial(context []byte, addr string) {
	// 创建一个字节切片，用于存储消息长度前缀和消息内容
	data := make([]byte, 4+len(context))

	// 将消息长度写入字节切片（使用大端序）
	binary.BigEndian.PutUint32(data[:4], uint32(len(context)))

	// 将消息内容写入字节切片
	copy(data[4:], context)

	for attempt := 0; attempt < 2; attempt++ {
		conn, err := getConn(addr)
		if err != nil {
			log.Println("connect error", err)
			return
		}

		// 同一连接上的写入必须串行，否则长度前缀与消息体会交错。
		mu := writeLockFor(addr)
		mu.Lock()
		_, err = conn.Write(data)
		mu.Unlock()
		if err == nil {
			return
		}

		log.Println("send error, 丢弃连接后重试:", err)
		dropConn(addr, conn)
	}
}

// getConn 返回缓存的连接，没有缓存则新建。
func getConn(addr string) (net.Conn, error) {
	connMaplock.Lock()
	defer connMaplock.Unlock()
	if connMap == nil {
		connMap = make(map[string]net.Conn)
	}
	if conn, ok := connMap[addr]; ok {
		return conn, nil
	}
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}
	connMap[addr] = conn
	return conn, nil
}

// dropConn 关闭并移除已失效的连接，下次发送时会重新建立。
func dropConn(addr string, conn net.Conn) {
	connMaplock.Lock()
	if cur, ok := connMap[addr]; ok && cur == conn {
		delete(connMap, addr)
	}
	connMaplock.Unlock()
	_ = conn.Close()
}

// writeLockFor 返回 addr 对应的写锁（懒创建）。
func writeLockFor(addr string) *sync.Mutex {
	connMaplock.Lock()
	defer connMaplock.Unlock()
	if writeLocks == nil {
		writeLocks = make(map[string]*sync.Mutex)
	}
	mu, ok := writeLocks[addr]
	if !ok {
		mu = new(sync.Mutex)
		writeLocks[addr] = mu
	}
	return mu
}

// 使用tcp（短链接）发送消息
// func TcpDial(context []byte, addr string) {
// 	// 先建立连接
// 	conn, err := net.Dial("tcp", addr)
// 	if err != nil {
// 		log.Println("connect error", err)
// 		return
// 	}
// 	_, err = conn.Write(context)
// 	if err != nil {
// 		log.Fatal(err)
// 	}

// 	conn.Close()
// }

func Int2hexString(j int) string {
	str := strconv.Itoa(j)
	distInt64, _ := strconv.ParseInt(str, 10, 64)
	dist16Str := fmt.Sprintf("%x", distInt64)

	zero := make([]byte, 0)
	for i := 0; i < 40-len(dist16Str); i++ {
		zero = append(zero, 48)
	}
	zz := string(zero)
	ans := zz + dist16Str
	return ans
}

// 生成 [0, 3] 之间的随机数
func RandInt0To3(seed int64) int {
	rand.Seed(seed)
	res := rand.Intn(4)
	return res
}

// var (
// 	path1 = []string{"./log/S0_transaction.csv", "./log/S1_transaction.csv", "./log/S2_transaction.csv", "./log/S3_transaction.csv"}
// )

// type TxDelay struct {
// 	//交易ID
// 	txid 					int
// 	//交易延迟
// 	// delay 					int
// 	first_queueing_time		int
// 	second_queueing_time	int
// }

// func order(txs []TxDelay) {
// 	//升序排序
// 	sort.Slice(txs, func(i, j int) bool {
// 		return txs[i].txid < txs[j].txid
// 	})
// }

// func TxDelayCsv() {
// 	var sum float64
// 	// ntx, temp_first, second := readdelay2()
// 	ntx, temp_first, second := readdelay3()
// 	order(temp_first)
// 	order(second)
// 	order(ntx)

// 	first := make([]TxDelay, 0)
// 	j:=0
// 	for i := 0; i<len(second); i++ {

// 		for temp_first[j].txid!=second[i].txid {
// 			j++
// 		}
// 		first = append(first, temp_first[j])
// 	}

// 	if len(first) != len(second) {
// 		fmt.Println("不对！")
// 		fmt.Printf("len1: %v, len2: %v\n", len(first), len(second))
// 		return
// 	}

// 	//首行
// 	// var titles string
// 	// titles := "txid,sender_delay,delay,time_diff,1stOnChainTo2ndEntry\n"
// 	titles := "txid,1st_queueing_time,2nd_queueing_time,queueing_time\n"

// 	var stringBuilder strings.Builder
// 	stringBuilder.WriteString(titles)

// 	for i := 0; i < len(first); i++ {
// 		// dataString := fmt.Sprintf("%v,%v,%v,%v,%v\n", first[i].txid, first[i].delay, second[i].delay, second[i].delay-first[i].delay, second[i].second_request_time - first[i].first_on_chain_time)
// 		dataString := fmt.Sprintf("%v,%v,%v,%v\n", first[i].txid, first[i].first_queueing_time, second[i].second_queueing_time, second[i].second_queueing_time+first[i].first_queueing_time)
// 		stringBuilder.WriteString(dataString)
// 	}
// 	filename := "./log/跨分片交易延迟及时间差.csv"
// 	file, _ := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModeAppend|os.ModePerm)
// 	dataString := stringBuilder.String()
// 	file.WriteString(dataString)
// 	file.Close()

// 	//首行
// 	// var titles string
// 	titles = "txid,queueing_time\n"

// 	var stringBuilder2 strings.Builder
// 	stringBuilder2.WriteString(titles)

// 	for i := 0; i < len(ntx); i++ {
// 		dataString := fmt.Sprintf("%v,%v\n", ntx[i].txid, ntx[i].first_queueing_time)
// 		stringBuilder2.WriteString(dataString)
// 	}
// 	filename = "./log/片内交易延迟.csv"
// 	file, _ = os.OpenFile(filename, os.O_RDWR|os.O_CREATE, os.ModeAppend|os.ModePerm)
// 	dataString = stringBuilder2.String()
// 	file.WriteString(dataString)
// 	file.Close()

// 	fmt.Println(sum)
// }

// func readdelay3() (ntx, ctx1, ctx2 []TxDelay) {

// 	for i := 0; i < len(path1); i++ {
// 		file, err := os.Open(path1[i])
// 		if err != nil {
// 			log.Panic()
// 		}
// 		fmt.Println(i)
// 		r := csv.NewReader(file)
// 		_, err = r.Read()
// 		if err != nil {
// 			log.Panic()
// 		}
// 		for {
// 			row, err := r.Read()
// 			// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
// 			if err != nil && err != io.EOF {
// 				log.Panic(err)
// 			}
// 			if err == io.EOF {
// 				break
// 			}

// 			senderid, _ := strconv.Atoi(row[7])
// 			receiverid,_ := strconv.Atoi(row[8])
// 			id, _ := strconv.Atoi(row[0])
// 			first_request_time, _ := strconv.Atoi(row[2])
// 			first_fetch_time, _ := strconv.Atoi(row[3])
// 			second_request_time, _ := strconv.ParseInt(row[4], 10, 64)
// 			second_fetch_time, _ := strconv.ParseInt(row[5], 10, 64)
// 			tx := TxDelay{txid:id, first_queueing_time: first_fetch_time-first_request_time, second_queueing_time: int(second_fetch_time-second_request_time)}
// 			//片内交易不管
// 			if senderid == receiverid {
// 				ntx = append(ntx, tx)
// 			}else if  senderid == i {
// 				ctx1 = append(ctx1, tx)
// 			} else {
// 				ctx2 = append(ctx2, tx)
// 			}
// 		}
// 		file.Close()
// 	}
// 	fmt.Printf("ntx,ctx1,ctx2: %v %v %v\n",len(ntx), len(ctx1), len(ctx2))
// 	return ntx, ctx1, ctx2
// }

// func readdelay2() (ntx, ctx1, ctx2 []TxDelay) {

// 	for i := 0; i < len(path1); i++ {
// 		file, err := os.Open(path1[i])
// 		if err != nil {
// 			log.Panic()
// 		}
// 		fmt.Println(i)
// 		r := csv.NewReader(file)
// 		_, err = r.Read()
// 		if err != nil {
// 			log.Panic()
// 		}
// 		for {
// 			row, err := r.Read()
// 			// fmt.Printf("%v %v %v\n", row[0][2:], row[1][2:], row[2])
// 			if err != nil && err != io.EOF {
// 				log.Panic(err)
// 			}
// 			if err == io.EOF {
// 				break
// 			}

// 			senderid, _ := strconv.Atoi(row[8])
// 			receiverid,_ := strconv.Atoi(row[9])
// 			id, _ := strconv.Atoi(row[0])
// 			delay, _ := strconv.Atoi(row[5])
// 			firsttime, _ := strconv.Atoi(row[4])
// 			secondetime, _ := strconv.ParseInt(row[3], 10, 64)
// 			tx := TxDelay{txid:id, delay:delay, first_on_chain_time: int64(firsttime), second_request_time: secondetime}
// 			//片内交易不管
// 			if senderid == receiverid {
// 				ntx = append(ntx, tx)
// 			}else if  senderid == i {
// 				ctx1 = append(ctx1, tx)
// 			} else {
// 				ctx2 = append(ctx2, tx)
// 			}
// 		}
// 		file.Close()
// 	}
// 	fmt.Printf("ntx,ctx1,ctx2: %v %v %v\n",len(ntx), len(ctx1), len(ctx2))
// 	return ntx, ctx1, ctx2
// }
