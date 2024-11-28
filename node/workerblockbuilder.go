package node

import (
	"net/http"
	"reflect"
	"sync"

	"paperexperiment/config"
	"paperexperiment/log"
	"paperexperiment/mempool"
	"paperexperiment/message"
	"paperexperiment/socket"
	"paperexperiment/types"
	"paperexperiment/utils"

	"github.com/ethereum/go-ethereum/common"
)

// Node is the primary access point for every replica
// it includes networking, state machine and RESTful API server
type BlockBuilder interface {
	socket.BBSocket
	//Database
	GetIP() string
	GetShard() types.Shard
	IsRootShard(tx *message.Transaction) bool
	AddGlobalSequence(ct *message.Transaction)
	GetGlobalSequence() []*message.Transaction
	AddProcessingTransaction(tx *message.Transaction, length int)
	SetAbortTransaction(tx *message.Transaction)
	DeleteProcessingTransaction(tx *message.Transaction)
	AddCommittedTransaction(tx *message.Transaction)
	GetCommittedTransaction() []*message.Transaction
	// CreateWorkerBlockData() *blockchain.WorkerBlockData
	GetBlockHeight() types.BlockHeight
	SetBlockHeight(blockheight types.BlockHeight)
	Run()
	Retry(r message.Transaction)
	Register(m interface{}, f interface{})
}

// node implements Node interface
type blockbuilder struct {
	ip    string
	shard types.Shard

	socket.BBSocket
	server *http.Server

	//Database
	handles     map[string]reflect.Value
	MessageChan chan interface{}
	TxChan      chan interface{}
	BlockChan   chan interface{}

	sync.RWMutex

	pt                             *mempool.ManageTransactions
	g_sequence                     []*message.Transaction
	current_blockheight            types.BlockHeight
	latest_coordinator_blockheight types.BlockHeight
	processingTransaction          map[common.Hash]int // How many need shard associate transaction
	decidingTransaction            map[common.Hash]int // Increase when receive vote transaction from other shard
	committedTransaction           *mempool.Producer
	txcnt                          int
}

// NewNode creates a new Node object from configuration
func NewWorkerBlockBuilder(ip string, shard types.Shard) BlockBuilder {
	bb := new(blockbuilder)
	bb.ip = ip
	bb.shard = shard
	bb.BBSocket = socket.NewBBSocket(ip, shard, config.Configuration.Addrs)
	bb.handles = make(map[string]reflect.Value)
	bb.MessageChan = make(chan interface{}, config.Configuration.ChanBufferSize)
	bb.pt = mempool.NewManageTransactions()
	bb.g_sequence = make([]*message.Transaction, 0)
	bb.current_blockheight = 0
	bb.latest_coordinator_blockheight = 0
	bb.processingTransaction = make(map[common.Hash]int, 0)
	bb.decidingTransaction = make(map[common.Hash]int, 0)
	bb.committedTransaction = mempool.NewProducer()
	bb.txcnt = 0

	return bb
}

/* Function */
func (bb *blockbuilder) GetIP() string {
	return bb.ip
}

func (bb *blockbuilder) GetShard() types.Shard {
	return bb.shard
}

func (bb *blockbuilder) IsRootShard(tx *message.Transaction) bool {
	var address []common.Address
	if tx.TXType == types.TRANSFER {
		address = append(address, tx.From)
		if utils.CalculateShardToSend(address)[0] == bb.shard {
			return true
		} else {
			return false
		}
	} else if tx.TXType == types.SMARTCONTRACT {
		address = append(address, tx.To)
		if utils.CalculateShardToSend(address)[0] == bb.shard {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (bb *blockbuilder) AddGlobalSequence(ct *message.Transaction) {
	bb.g_sequence = append(bb.g_sequence, ct)
}

func (bb *blockbuilder) GetGlobalSequence() []*message.Transaction {
	return bb.g_sequence
}

func (bb *blockbuilder) AddProcessingTransaction(tx *message.Transaction, length int) {
	bb.processingTransaction[tx.Hash] = length
}

func (bb *blockbuilder) SetAbortTransaction(tx *message.Transaction) {
	bb.decidingTransaction[tx.Hash] = bb.processingTransaction[tx.Hash] + 1
}

func (bb *blockbuilder) DeleteProcessingTransaction(tx *message.Transaction) {
	delete(bb.processingTransaction, tx.Hash)
}

func (bb *blockbuilder) AddCommittedTransaction(tx *message.Transaction) {
	bb.committedTransaction.AddTxn(tx)
}

func (bb *blockbuilder) GetCommittedTransaction() []*message.Transaction {
	return bb.committedTransaction.GeneratePayload(bb.pt)
}

func (bb *blockbuilder) GetBlockHeight() types.BlockHeight {
	return bb.current_blockheight
}

func (bb *blockbuilder) SetBlockHeight(blockheight types.BlockHeight) {
	bb.current_blockheight = blockheight
}

func (bb *blockbuilder) Retry(r message.Transaction) {
	log.Debugf("blockbuilder %v retry reqeust %v", bb.shard, r)
	bb.TxChan <- r
}

// Register a handle function for each message type
func (bb *blockbuilder) Register(m interface{}, f interface{}) {
	t := reflect.TypeOf(m)
	fn := reflect.ValueOf(f)

	if fn.Kind() != reflect.Func {
		panic("handle function is not func")
	}

	if fn.Type().In(0) != t {
		panic("func type is not t")
	}

	if fn.Kind() != reflect.Func || fn.Type().NumIn() != 1 || fn.Type().In(0) != t {
		panic("register handle function error")
	}

	bb.handles[t.String()] = fn
}

// Run start and run the node
func (bb *blockbuilder) Run() {
	log.Infof("BlockBuilder %v, %v start running", bb.ip, bb.shard)

	go bb.handle()
	go bb.recv()
	// bb.http()
}

// handle receives messages from message channel and calls handle function using refection
func (bb *blockbuilder) handle() {
	for {
		msg := <-bb.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := bb.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		go f.Call([]reflect.Value{v})
	}
}

// recv receives messages from socket and pass to message channel
func (bb *blockbuilder) recv() {
	for {
		m := bb.Recv()
		bb.MessageChan <- m
	}
}
