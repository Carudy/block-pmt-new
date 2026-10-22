package core

import "sync"

// 以下 pool 仅 preempt_escrow 使用，由 BlockChain 持有并在 GenerateBlock 中取走。

type EscrowQuiesceReqPool struct {
	Queue []*EscrowQuiesceReq
	lock  sync.Mutex
}

func NewEscrowQuiesceReqPool() *EscrowQuiesceReqPool {
	return &EscrowQuiesceReqPool{Queue: make([]*EscrowQuiesceReq, 0)}
}

func (p *EscrowQuiesceReqPool) Add(q *EscrowQuiesceReq) {
	p.lock.Lock()
	p.Queue = append(p.Queue, q)
	p.lock.Unlock()
}

func (p *EscrowQuiesceReqPool) Fetch() []*EscrowQuiesceReq {
	p.lock.Lock()
	out := p.Queue
	p.Queue = make([]*EscrowQuiesceReq, 0)
	p.lock.Unlock()
	return out
}

type EscrowQuiescePool struct {
	Queue []*EscrowQuiesce
	lock  sync.Mutex
}

func NewEscrowQuiescePool() *EscrowQuiescePool {
	return &EscrowQuiescePool{Queue: make([]*EscrowQuiesce, 0)}
}

func (p *EscrowQuiescePool) Add(q *EscrowQuiesce) {
	p.lock.Lock()
	p.Queue = append(p.Queue, q)
	p.lock.Unlock()
}

func (p *EscrowQuiescePool) Fetch() []*EscrowQuiesce {
	p.lock.Lock()
	out := p.Queue
	p.Queue = make([]*EscrowQuiesce, 0)
	p.lock.Unlock()
	return out
}

type EscrowSettlePool struct {
	Queue []*EscrowSettle
	lock  sync.Mutex
}

func NewEscrowSettlePool() *EscrowSettlePool {
	return &EscrowSettlePool{Queue: make([]*EscrowSettle, 0)}
}

func (p *EscrowSettlePool) Add(s *EscrowSettle) {
	p.lock.Lock()
	p.Queue = append(p.Queue, s)
	p.lock.Unlock()
}

func (p *EscrowSettlePool) Fetch() []*EscrowSettle {
	p.lock.Lock()
	out := p.Queue
	p.Queue = make([]*EscrowSettle, 0)
	p.lock.Unlock()
	return out
}

type EscrowDebitPool struct {
	Queue []*EscrowDebit
	lock  sync.Mutex
}

func NewEscrowDebitPool() *EscrowDebitPool {
	return &EscrowDebitPool{Queue: make([]*EscrowDebit, 0)}
}

func (p *EscrowDebitPool) Add(d *EscrowDebit) {
	p.lock.Lock()
	p.Queue = append(p.Queue, d)
	p.lock.Unlock()
}

func (p *EscrowDebitPool) Fetch() []*EscrowDebit {
	p.lock.Lock()
	out := p.Queue
	p.Queue = make([]*EscrowDebit, 0)
	p.lock.Unlock()
	return out
}
