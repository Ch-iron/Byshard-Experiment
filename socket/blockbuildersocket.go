package socket

import (
	"math/rand"
	"strconv"
	"sync"
	"time"

	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/transport"
	"paperexperiment/types"
	"paperexperiment/utils"
)

// Socket integrates all networking interface and fault injections
type BBSocket interface {

	// Send put message to outbound queue
	Send(to identity.NodeID, m interface{})

	// MulticastQuorum sends msg to random number of nodes
	MulticastQuorum(quorum int, m interface{})

	// Broadcast send to all peers
	Broadcast(m interface{})

	// Broadcast send to some peers
	BroadcastToSome(some []identity.NodeID, m interface{})

	// Recv receives a message
	Recv() interface{}

	Close()

	// Fault injection
	Drop(id identity.NodeID, t int)             // drops every message send to NodeID last for t seconds
	Slow(id identity.NodeID, d int, t int)      // delays every message send to NodeID for d ms and last for t seconds
	Flaky(id identity.NodeID, p float64, t int) // drop message by chance p for t seconds
	Crash(t int)                                // node crash for t seconds
}

type bbsocket struct {
	ip        string
	shard     types.Shard
	transport transport.Transport
	addresses map[identity.NodeID]string
	nodes     map[identity.NodeID]transport.Transport

	crash bool
	drop  map[identity.NodeID]bool
	slow  map[identity.NodeID]int
	flaky map[identity.NodeID]float64

	lock sync.RWMutex // locking map nodes
}

// NewSocket return Socket interface instance given self NodeID, node list, transport and codec name
func NewBBSocket(ip string, shard types.Shard, addrs map[types.Shard]map[identity.NodeID]string) BBSocket {
	bbs := new(bbsocket)
	bbs.ip = ip
	bbs.shard = shard
	bbs.transport = transport.NewTransport(ip)
	tmpaddrs := make(map[types.Shard]map[identity.NodeID]string)
	tmpaddrs[shard] = make(map[identity.NodeID]string)
	for nodeshard, address := range addrs {
		if nodeshard == shard {
			for id, addr := range address {
				if id != identity.NewNodeID(0) {
					port := strconv.Itoa(3999 + int(shard)*100 + id.Node())
					addr = addr + port
					tmpaddrs[shard][id] = addr
				}
			}
		}
	}
	bbs.addresses = tmpaddrs[shard]
	bbs.nodes = make(map[identity.NodeID]transport.Transport)
	bbs.crash = false
	bbs.drop = make(map[identity.NodeID]bool)
	bbs.slow = make(map[identity.NodeID]int)
	bbs.flaky = make(map[identity.NodeID]float64)

	bbs.transport.Listen()

	return bbs
}

func (s *bbsocket) Send(to identity.NodeID, m interface{}) {
	// log.Debugf("shard %v send message %v to %v", s.shard, m, to)

	if s.crash {
		return
	}

	if s.drop[to] {
		return
	}

	if p, ok := s.flaky[to]; ok && p > 0 {
		if rand.Float64() < p {
			return
		}
	}

	s.lock.RLock()
	t, exists := s.nodes[to]
	s.lock.RUnlock()
	if !exists {
		s.lock.RLock()
		address, ok := s.addresses[to]
		s.lock.RUnlock()
		if !ok {
			log.Errorf("socket does not have address of node %s", to)
			return
		}
		t = transport.NewTransport(address)
		err := utils.Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
		if err != nil {
			panic(err)
		}
		s.lock.Lock()
		s.nodes[to] = t
		s.lock.Unlock()
	}

	if delay, ok := s.slow[to]; ok && delay > 0 {
		timer := time.NewTimer(time.Duration(delay) * time.Millisecond)
		go func() {
			<-timer.C
			t.Send(m)
		}()
		return
	}

	t.Send(m)
	// log.Debugf("shard %v send message done %v to %v", s.shard, m, to)
}

func (s *bbsocket) Recv() interface{} {
	s.lock.RLock()
	t := s.transport
	s.lock.RUnlock()
	for {
		m := t.Recv()
		if !s.crash {
			return m
		}
	}
}

func (s *bbsocket) MulticastQuorum(quorum int, m interface{}) {
	//log.Debugf("node %s multicasting message %+v for %d nodes", s.id, m, quorum)
	sent := map[int]struct{}{}
	for i := 0; i < quorum; i++ {
		r := rand.Intn(len(s.addresses)) + 1
		_, exists := sent[r]
		if exists {
			continue
		}
		s.Send(identity.NewNodeID(r), m)
		sent[r] = struct{}{}
	}
}

func (s *bbsocket) Broadcast(m interface{}) {
	// log.Debugf("shard %v broadcasting message %v", s.shard, m)
	latencytimer := utils.GetBetweenShardTimer(s.shard, s.shard)
	<-latencytimer.C
	for id := range s.addresses {
		s.Send(id, m)
	}
	// log.Debugf("shard %v done broadcasting message %v", s.shard, m)
}

func (s *bbsocket) BroadcastToSome(some []identity.NodeID, m interface{}) {
	//log.Debugf("node %s broadcasting message %+v", s.id, m)
	for _, id := range some {
		if _, exist := s.addresses[id]; !exist {
			continue
		}
		s.Send(id, m)
	}
	// log.Errorf("node %s done  broadcasting message %+v", s.id, m)
}

func (s *bbsocket) Close() {
	for _, t := range s.nodes {
		t.Close()
	}
}

func (s *bbsocket) Drop(id identity.NodeID, t int) {
	s.drop[id] = true
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.drop[id] = false
	}()
}

func (s *bbsocket) Slow(id identity.NodeID, delay int, t int) {
	s.slow[id] = delay
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.slow[id] = 0
	}()
}

func (s *bbsocket) Flaky(id identity.NodeID, p float64, t int) {
	s.flaky[id] = p
	timer := time.NewTimer(time.Duration(t) * time.Second)
	go func() {
		<-timer.C
		s.flaky[id] = 0
	}()
}

func (s *bbsocket) Crash(t int) {
	s.crash = true
	if t > 0 {
		timer := time.NewTimer(time.Duration(t) * time.Second)
		go func() {
			<-timer.C
			s.crash = false
		}()
	}
}
