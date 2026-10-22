package pbft

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"
)

// //客户端使用的tcp监听
// func clientTcpListen() {
// 	listen, err := net.Listen("tcp", clientAddr)
// 	if err != nil {
// 		log.Panic(err)
// 	}
// 	defer listen.Close()

// 	for {
// 		conn, err := listen.Accept()
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 		b, err := ioutil.ReadAll(conn)
// 		if err != nil {
// 			log.Panic(err)
// 		}
// 		fmt.Println(string(b))
// 	}

// }

// 节点使用的tcp监听
func (p *Pbft) TcpListen() {
	var listen net.Listener
	var err error
	// 端口可能被刚被 kill 的遗留进程短暂占用，先重试一段时间再报错。
	const maxAttempts = 15
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		listen, err = net.Listen("tcp", p.Node.addr)
		if err == nil {
			break
		}
		if attempt == 1 {
			fmt.Printf("警告：节点 %s 监听失败（%v），将重试最多 %d 秒...\n", p.Node.addr, err, maxAttempts)
		}
		time.Sleep(1 * time.Second)
	}
	if err != nil {
		log.Panicf("节点 %s 监听失败：%v。该端口似被占用：请先运行 ./kill_block_exe.sh 清理遗留进程，"+
			"或用 PORT_BASE 指定其它端口段（run.sh 会自动挑选空闲端口段）。", p.Node.addr, err)
	}
	fmt.Printf("节点开启监听，地址：%s\n", p.Node.addr)
	defer listen.Close()

	for {
		conn, err := listen.Accept()
		if err != nil {
			log.Panic(err)
		}
		go p.handleConn(conn)
		// time.Sleep(10 * time.Millisecond)
		// b, err := ioutil.ReadAll(conn)
		// if err != nil {
		// 	log.Println(err)
		// 	continue
		// }
		// conn.Close()
		// p.handleRequest(b)
	}
}

// // 读取连接数据, \n 做结束符
// func (p *Pbft) handleConn(conn net.Conn) {
// 	defer conn.Close()
// 	reader := bufio.NewReader(conn)
// 	for {
// 		b, err := reader.ReadBytes('\n')
// 		switch err {
// 		case nil:
// 			p.handleRequest(b)
// 		case io.EOF:
// 			log.Println("client closed the connection by terminating the process")
// 			return
// 		default:
// 			log.Printf("error: %v\n", err)
// 			return
// 		}
// 	}
// }

// 读取连接数据，利用长度前缀防粘包
func (p *Pbft) handleConn(conn net.Conn) {
	defer conn.Close()
	for {
		// 创建一个字节切片来存储消息长度前缀
		lengthPrefix := make([]byte, 4)

		// 读取消息长度前缀（ReadFull：长度前缀必须读满 4 字节，否则长度解析会有偏差）
		if _, err := io.ReadFull(conn, lengthPrefix); err != nil {
			// 对端关闭 / 连接被重置属于正常断连（如节点退出、连接被丢弃重建），
			// 不能因此 log.Fatal 终止整个节点进程。
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				log.Println("Error reading message length prefix:", err.Error())
			}
			return
		}

		// 将消息长度前缀解析为一个无符号整数
		length := binary.BigEndian.Uint32(lengthPrefix)

		// 创建一个字节切片来存储消息内容
		message := make([]byte, length)

		// 读取消息内容
		if _, err := io.ReadFull(conn, message); err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) {
				log.Println("Error reading message:", err.Error())
			}
			return
		}

		p.handleRequest(message)
	}
}
