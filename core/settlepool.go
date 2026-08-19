package core

import "sync"

type TXsettle_pool struct {
	// 待打包的银行结算队列（settle-in，收到其他分片发来的 cSettle 后进入这里）
	Queue []*TXsettle
	lock  sync.Mutex
}

func NewTXsettlePool() *TXsettle_pool {
	return &TXsettle_pool{
		Queue: make([]*TXsettle, 0),
	}
}

func (pool *TXsettle_pool) AddTXsettle(settle *TXsettle) {
	pool.lock.Lock()
	pool.Queue = append(pool.Queue, settle)
	pool.lock.Unlock()
}

func (pool *TXsettle_pool) AddTXsettles(settles []*TXsettle) {
	for _, s := range settles {
		pool.AddTXsettle(s)
	}
}

func (pool *TXsettle_pool) FetchTXsettles2Pack() (settles []*TXsettle) {
	pool.lock.Lock()
	settles = make([]*TXsettle, 0)
	settles = append(settles, pool.Queue...)
	pool.Queue = []*TXsettle{}
	pool.lock.Unlock()
	return
}
