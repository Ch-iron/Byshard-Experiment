package communicator

import (
	"encoding/gob"
	"time"

	"paperexperiment/blockchain"
	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/mempool"
	"paperexperiment/message"
	"paperexperiment/node"
	"paperexperiment/transport"
	"paperexperiment/types"
	"paperexperiment/utils"

	"github.com/ethereum/go-ethereum/common"
)

type (
	CommunicatorReplica struct {
		node.Communicator
		gatewaynodeTransport transport.Transport
		// This channel pass pointer of message struct. Topic name is decided by who is receiver
		CommunicatorTopic         chan interface{}
		RootShardVotedTransaction chan interface{}
		CommittedTransaction      chan interface{}
		GatewayTopic              chan interface{}
		ConsensusNodeTopic        chan interface{}
		txcnt                     int
		txMempool                 *mempool.Producer
		pt                        *mempool.ManageTransactions
	}
)

func NewCommunicator(ip string, shard types.Shard) *CommunicatorReplica {
	r := new(CommunicatorReplica)
	addrs := config.Configuration.Addrs
	r.gatewaynodeTransport = transport.NewTransport(addrs[types.Shard(0)][identity.NewNodeID(0)] + "2999")
	r.Communicator = node.NewCommunicator(ip, shard)
	r.CommunicatorTopic = make(chan interface{}, 16384)
	r.RootShardVotedTransaction = make(chan interface{}, 10240)
	r.CommittedTransaction = make(chan interface{}, 10240)
	r.GatewayTopic = make(chan interface{}, 128)
	r.ConsensusNodeTopic = make(chan interface{}, 128)
	r.txMempool = mempool.NewProducer()
	r.pt = mempool.NewManageTransactions()
	r.txcnt = 0

	/* Register to gob en/decoder */
	gob.Register(message.CommunicatorRegister{})
	gob.Register(message.CommunicatorListResponse{})
	gob.Register(message.ShardSharedVariableRegisterRequest{})
	gob.Register(message.ShardSharedVariableRegisterResponse{})
	gob.Register(message.ShardList{})
	gob.Register(message.ConsensusNodeRegister{})
	gob.Register(message.Transaction{})
	gob.Register(message.RootShardVotedTransaction{})
	gob.Register(message.VotedTransaction{})
	gob.Register(message.CommittedTransaction{})
	gob.Register(message.CompleteTransaction{})
	gob.Register(blockchain.ShardBlock{})
	gob.Register(message.NodeStartMessage{})
	gob.Register(message.NodeStartAck{})
	gob.Register(message.ClientStart{})
	gob.Register(message.CommunicatorSignedTransaction{})
	gob.Register(message.VoteCommunicatorSignedTransaction{})
	gob.Register(message.VotedTransaction{})
	gob.Register(message.CommittedTransaction{})
	gob.Register(message.Experiment{})

	/* Register message handler */
	r.Register(message.ShardSharedVariableRegisterRequest{}, r.handleShardVariableRegisterRequest)
	r.Register(message.ShardList{}, r.handleShardList)
	r.Register(message.ConsensusNodeRegister{}, r.handleConsensusRegister)
	r.Register(message.NodeStartAck{}, r.handleNodeStartAck)
	r.Register(message.Transaction{}, r.handleTransaction)
	r.Register(message.RootShardVotedTransaction{}, r.handleRootShardVotedTransaction)
	r.Register(message.VotedTransaction{}, r.handleVotedTransaction)
	r.Register(message.CommittedTransaction{}, r.handleCommittedTransaction)
	r.Register(message.CompleteTransaction{}, r.handleConsensusCompleteTransaction)
	r.Register(blockchain.ShardBlock{}, r.handleShardBlock)
	r.Register(message.Experiment{}, r.handleExperiment)

	return r
}

func (r *CommunicatorReplica) Start() {
	go r.Run()
	err := utils.Retry(r.gatewaynodeTransport.Dial, 100, time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
}

func (r *CommunicatorReplica) IsExist(address common.Address) bool {
	var addressList []common.Address
	addressList = append(addressList, address)
	if utils.CalculateShardToSend(addressList)[0] == r.GetShard() {
		return true
	} else {
		return false
	}
}

/* Node Start */
func (r *CommunicatorReplica) SendNodeStartMessage() {
	nodeStartMessage := message.NodeStartMessage{
		Message: "Start",
	}
	r.Broadcast(nodeStartMessage)
	log.Debugf("[Communicator %v] (SendNodeStartMessage) Send To Consensus Node Start Block Create", r.GetShard())
}

/* Message Handler */
func (r *CommunicatorReplica) handleShardList(msg message.ShardList) {
	r.CommunicatorTopic <- msg
}

func (r *CommunicatorReplica) handleShardVariableRegisterRequest(msg message.ShardSharedVariableRegisterRequest) {
	// G -> W
	r.CommunicatorTopic <- msg
}

func (r *CommunicatorReplica) handleConsensusRegister(msg message.ConsensusNodeRegister) {
	r.ConsensusNodeTopic <- msg
}

func (r *CommunicatorReplica) handleNodeStartAck(msg message.NodeStartAck) {
	r.ConsensusNodeTopic <- msg
}

/* paperexperiment Transaction Consensus */
func (r *CommunicatorReplica) handleTransaction(tx message.Transaction) {
	log.Debugf("[Communicator %v] Receive Transaction Hash: %v, Nonce: %v", r.GetShard(), tx.Hash, tx.Nonce)
	if tx.AbortAndRetryLatencyDissection.BlockWaitingTime == 0 {
		tx.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
	}
	if !tx.IsCrossShardTx {
		r.txcnt++
		// log.Debugf("[Communicator %v] %vth Receive Transaction!!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), r.txcnt, tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
		// log.Debugf("[Communicator %v] Start Local paperexperiment Protocol!!!", r.GetShard())

		// Builder Sign Transaction
		builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
		signed_transaction := message.CommunicatorSignedTransaction{
			Transaction: tx,
		}
		signed_transaction.Committee_sig = append(signed_transaction.Committee_sig, builder_signature)
		r.Broadcast(signed_transaction) // Send transaction to Consensus Nodes
	} else {

		if r.IsRootShard(&tx) {
			r.txcnt++
			tx.StartTime = time.Now().UnixMilli()
			// Start paperexperiment protocol
			// log.Debugf("[Communicator %v] %vth Receive Transaction!!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), r.txcnt, tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
			// log.Debugf("[Communicator %v] Start paperexperiment Protocol!!!", r.GetShard())
			// Builder Sign Transaction
			builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
			signed_transaction := message.CommunicatorSignedTransaction{
				Transaction: tx,
			}
			signed_transaction.Committee_sig = append(signed_transaction.Committee_sig, builder_signature)
			r.Broadcast(signed_transaction) // Send transaction to Consensus Nodes
			// Process Consensus Process
		} else { // not root shard case, process vote step
			var root_shard []types.Shard
			root_address := []common.Address{}
			if tx.TXType == types.TRANSFER {
				root_address = append(root_address, tx.From)
			} else if tx.TXType == types.SMARTCONTRACT {
				root_address = append(root_address, tx.To)
			}
			root_shard = utils.CalculateShardToSend(root_address)
			latencyTimer := utils.GetBetweenShardTimer(r.GetShard(), root_shard[0])
			<-latencyTimer.C
			tx.AbortAndRetryLatencyDissection.Network1 = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.Network1
			tx.LatencyDissection.Network1 = tx.LatencyDissection.Network1 + tx.AbortAndRetryLatencyDissection.Network1
			tx.AbortAndRetryLatencyDissection.VoteConsensusTime = time.Now().UnixMilli()
			// log.Debugf("[Communicator %v] Receive Transaction From Root Shard!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
			// log.Debugf("[Communicator %v] Start Vote Process!!!", r.GetShard())
			// Builder Sign Transaction
			builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
			signed_transaction := message.VoteCommunicatorSignedTransaction{
				Transaction: tx,
			}
			signed_transaction.Committee_sig = append(signed_transaction.Committee_sig, builder_signature)
			r.Broadcast(signed_transaction) // Send transaction to Consensus Nodes
			// Process Consensus Process
		}
	}
}

// Receive Transaction completed consensus from consensus nodes in root shard
func (r *CommunicatorReplica) handleRootShardVotedTransaction(tx message.RootShardVotedTransaction) {
	log.Infof("[Communicator %v] Receive Consensus Complete Transaction From Consensus Node TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	if r.IsRootShard(&tx.Transaction) {
		if tx.IsCommit {
			r.RootShardVotedTransaction <- tx
		}
	} else {
		r.RootShardVotedTransaction <- tx
	}
}

func (r *CommunicatorReplica) handleVotedTransaction(tx message.VotedTransaction) {
	log.Infof("[Communicator %v] Receive Voted Transaction From Other Shard TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	tx.AbortAndRetryLatencyDissection.Network2 = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.Network2
	tx.LatencyDissection.Network2 = tx.LatencyDissection.Network2 + tx.AbortAndRetryLatencyDissection.Network2
	tx.Transaction.AbortAndRetryLatencyDissection.DecideConsensusTime = time.Now().UnixMilli()

	builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
	tx.Committee_sig = append(tx.Committee_sig, builder_signature)
	r.Broadcast(tx)
}

func (r *CommunicatorReplica) handleCommittedTransaction(tx message.CommittedTransaction) {
	if r.IsRootShard(&tx.Transaction) {
		tx.AbortAndRetryLatencyDissection.DecideConsensusTime = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.DecideConsensusTime
		tx.LatencyDissection.DecideConsensusTime = tx.LatencyDissection.DecideConsensusTime + tx.AbortAndRetryLatencyDissection.DecideConsensusTime
		tx.AbortAndRetryLatencyDissection.Network3 = time.Now().UnixMilli()
		log.DecideDebugf("[Communicator %v] Receive Committed Transaction TxHash: %v", r.GetShard(), tx.Transaction.Hash)
		r.CommittedTransaction <- tx
	} else {
		var root_shard []types.Shard
		root_address := []common.Address{}
		if tx.TXType == types.TRANSFER {
			root_address = append(root_address, tx.From)
		} else if tx.TXType == types.SMARTCONTRACT {
			root_address = append(root_address, tx.To)
		}
		root_shard = utils.CalculateShardToSend(root_address)
		latencyTimer := utils.GetBetweenShardTimer(r.GetShard(), root_shard[0])
		<-latencyTimer.C
		log.DecideDebugf("[Communicator %v] Receive Committed Transaction from Root shard TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	}
	tx.AbortAndRetryLatencyDissection.Network3 = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.Network3
	tx.LatencyDissection.Network3 = tx.LatencyDissection.Network3 + tx.AbortAndRetryLatencyDissection.Network3
	tx.Transaction.AbortAndRetryLatencyDissection.CommitConsensusTime = time.Now().UnixMilli()
	builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
	tx.Committee_sig = append(tx.Committee_sig, builder_signature)
	r.Broadcast(tx)
}

func (r *CommunicatorReplica) handleConsensusCompleteTransaction(tx message.CompleteTransaction) {
	log.CommitDebugf("[Communicator %v] Complete Transaction TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	r.AddCommittedTransaction(&tx.Transaction)
}

// Receive ShardBlock completed consensus
func (r *CommunicatorReplica) handleShardBlock(msg blockchain.ShardBlock) {
	log.Infof("[Communicator %v] Receive ShardBlock From ID: %v Blockhash: %v", r.GetShard(), msg.Proposer, msg.Block_hash)
	r.gatewaynodeTransport.Send(msg)
	r.SetBlockHeight(r.GetBlockHeight() + 1)
	// only logging
	// log.Errorf("[Communicator %v] State Commit Start BlockHeight %v", r.GetShard(), msg.Block_header.Block_height)
	cross := 0
	for _, tx := range msg.Transaction {
		if tx.IsCrossShardTx {
			cross++
		}
	}
	log.PerformanceInfof("[Communicator %v] Final Block 블록번호: %v, 크로스: %v, 로컬: %v, 전체: %v", r.GetShard(), msg.Block_header.Block_height, cross, len(msg.Transaction)-cross, len(msg.Transaction))
	// only logging
	// for _, lt := range msg.Transaction {
	// 	log.PerformanceInfof("[Communicator %v] Final Committed Transaction Hash: %v", r.GetShard(), lt.Hash)
	// }
	// only logging
	// log.PerformanceInfof("[Communicator %v] 남은 글로벌 시퀀스 수: %v, 남은 글로벌 스냅샷 수: %v", r.GetShard(), len(r.GetGlobalSequence()), len(r.GetGlobalSnapshot()))
}

func (r *CommunicatorReplica) handleExperiment(msg message.Experiment) {
	r.CommunicatorTopic <- msg
}
