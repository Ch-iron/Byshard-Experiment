package node

import (
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
type Communicator interface {
	socket.CommSocket
	//Database
	GetIP() string
	GetShard() types.Shard
	IsRootShard(tx *message.Transaction) bool
	AddProcessingTransaction(tx *message.Transaction, length int)
	SetAbortTransaction(tx *message.Transaction)
	DeleteProcessingTransaction(tx *message.Transaction)
	AddCommittedTransaction(tx *message.Transaction)
	GetCommittedTransaction() []*message.Transaction
	// CreateShardBlockData() *blockchain.ShardBlockData
	GetBlockHeight() types.BlockHeight
	SetBlockHeight(blockheight types.BlockHeight)
	Run()
	Retry(r message.Transaction)
	Register(m interface{}, f interface{})
}

// node implements Node interface
type communicator struct {
	ip    string
	shard types.Shard

	socket.CommSocket

	//Database
	handles     map[string]reflect.Value
	MessageChan chan interface{}
	TxChan      chan interface{}
	BlockChan   chan interface{}

	sync.RWMutex

	pt                             *mempool.ManageTransactions
	current_blockheight            types.BlockHeight
	latest_coordinator_blockheight types.BlockHeight
	processingTransaction          map[common.Hash]int // How many need shard associate transaction
	decidingTransaction            map[common.Hash]int // Increase when receive vote transaction from other shard
	committedTransaction           *mempool.Producer
	txcnt                          int
}

// NewNode creates a new Node object from configuration
func NewCommunicator(ip string, shard types.Shard) Communicator {
	comm := new(communicator)
	comm.ip = ip
	comm.shard = shard
	comm.CommSocket = socket.NewCommunicatorSocket(ip, shard, config.Configuration.Addrs)
	comm.handles = make(map[string]reflect.Value)
	comm.MessageChan = make(chan interface{}, config.Configuration.ChanBufferSize)
	comm.pt = mempool.NewManageTransactions()
	comm.current_blockheight = 0
	comm.latest_coordinator_blockheight = 0
	comm.processingTransaction = make(map[common.Hash]int, 0)
	comm.decidingTransaction = make(map[common.Hash]int, 0)
	comm.committedTransaction = mempool.NewProducer()
	comm.txcnt = 0

	return comm
}

/* Function */
func (comm *communicator) GetIP() string {
	return comm.ip
}

func (comm *communicator) GetShard() types.Shard {
	return comm.shard
}

func (comm *communicator) IsRootShard(tx *message.Transaction) bool {
	var address []common.Address
	if tx.TXType == types.TRANSFER {
		address = append(address, tx.From)
		if utils.CalculateShardToSend(address)[0] == comm.shard {
			return true
		} else {
			return false
		}
	} else if tx.TXType == types.SMARTCONTRACT {
		address = append(address, tx.To)
		if utils.CalculateShardToSend(address)[0] == comm.shard {
			return true
		} else {
			return false
		}
	} else {
		return false
	}
}

func (comm *communicator) AddProcessingTransaction(tx *message.Transaction, length int) {
	comm.processingTransaction[tx.Hash] = length
}

func (comm *communicator) SetAbortTransaction(tx *message.Transaction) {
	comm.decidingTransaction[tx.Hash] = comm.processingTransaction[tx.Hash] + 1
}

func (comm *communicator) DeleteProcessingTransaction(tx *message.Transaction) {
	delete(comm.processingTransaction, tx.Hash)
}

func (comm *communicator) AddCommittedTransaction(tx *message.Transaction) {
	comm.committedTransaction.AddTxn(tx)
}

func (comm *communicator) GetCommittedTransaction() []*message.Transaction {
	return comm.committedTransaction.GeneratePayload(comm.pt)
}

func (comm *communicator) GetBlockHeight() types.BlockHeight {
	return comm.current_blockheight
}

func (comm *communicator) SetBlockHeight(blockheight types.BlockHeight) {
	comm.current_blockheight = blockheight
}

func (comm *communicator) Retry(r message.Transaction) {
	log.Debugf("communicator %v retry reqeust %v", comm.shard, r)
	comm.TxChan <- r
}

// Register a handle function for each message type
func (comm *communicator) Register(m interface{}, f interface{}) {
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

	comm.handles[t.String()] = fn
}

// Run start and run the node
func (comm *communicator) Run() {
	log.Infof("communicator %v, %v start running", comm.ip, comm.shard)

	go comm.handle()
	go comm.recv()
	// comm.http()
}

// handle receives messages from message channel and calls handle function using refection
func (comm *communicator) handle() {
	for {
		msg := <-comm.MessageChan
		v := reflect.ValueOf(msg)
		name := v.Type().String()
		f, exists := comm.handles[name]
		if !exists {
			log.Fatalf("no registered handle function for message type %v", name)
		}
		go f.Call([]reflect.Value{v})
	}
}

// recv receives messages from socket and pass to message channel
func (comm *communicator) recv() {
	for {
		m := comm.Recv()
		comm.MessageChan <- m
	}
}
