package blockbuilder

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
	/*
		1. (worker builder) connect to coordination shard with hard-coded address
		2. construct shard network
		3. construct block builder network
	*/
	WorkerBlockBuilderReplica struct {
		node.BlockBuilder
		gatewaynodeTransport transport.Transport
		// This channel pass pointer of message struct. Topic name is decided by who is receiver
		WorkerBuilderTopic        chan interface{}
		RootShardVotedTransaction chan interface{}
		CommittedTransaction      chan interface{}
		GatewayTopic              chan interface{}
		ConsensusNodeTopic        chan interface{}
		txcnt                     int
		txMempool                 *mempool.Producer
		pt                        *mempool.ManageTransactions
	}
)

func NewWorkerBlockBuilder(ip string, shard types.Shard) *WorkerBlockBuilderReplica {
	r := new(WorkerBlockBuilderReplica)
	addrs := config.Configuration.Addrs
	r.gatewaynodeTransport = transport.NewTransport(addrs[types.Shard(0)][identity.NewNodeID(0)] + "2999")
	r.BlockBuilder = node.NewWorkerBlockBuilder(ip, shard)
	r.WorkerBuilderTopic = make(chan interface{}, 16384)
	r.RootShardVotedTransaction = make(chan interface{}, 10240)
	r.CommittedTransaction = make(chan interface{}, 10240)
	r.GatewayTopic = make(chan interface{}, 128)
	r.ConsensusNodeTopic = make(chan interface{}, 128)
	r.txMempool = mempool.NewProducer()
	r.pt = mempool.NewManageTransactions()
	r.txcnt = 0

	/* Register to gob en/decoder */
	gob.Register(message.WorkerBuilderRegister{})
	gob.Register(message.WorkerBuilderListResponse{})
	gob.Register(message.WorkerSharedVariableRegisterRequest{})
	gob.Register(message.WorkerSharedVariableRegisterResponse{})
	gob.Register(message.WorkerList{})
	gob.Register(message.ConsensusNodeRegister{})
	gob.Register(message.Transaction{})
	gob.Register(message.RootShardVotedTransaction{})
	gob.Register(message.VotedTransaction{})
	gob.Register(message.CommittedTransaction{})
	gob.Register(message.CompleteTransaction{})
	gob.Register(blockchain.WorkerBlock{})
	gob.Register(message.NodeStartMessage{})
	gob.Register(message.NodeStartAck{})
	gob.Register(message.ClientStart{})
	gob.Register(message.BuilderSignedTransaction{})
	gob.Register(message.VoteBuilderSignedTransaction{})
	gob.Register(message.VotedTransaction{})
	gob.Register(message.CommittedTransaction{})
	gob.Register(message.Experiment{})

	/* Register message handler */
	r.Register(message.WorkerSharedVariableRegisterRequest{}, r.handleWorkerShardVariableRegisterRequest)
	r.Register(message.WorkerList{}, r.handleWorkerList)
	r.Register(message.ConsensusNodeRegister{}, r.handleConsensusRegister)
	r.Register(message.NodeStartAck{}, r.handleNodeStartAck)
	r.Register(message.Transaction{}, r.handleTransaction)
	r.Register(message.RootShardVotedTransaction{}, r.handleRootShardVotedTransaction)
	r.Register(message.VotedTransaction{}, r.handleVotedTransaction)
	r.Register(message.CommittedTransaction{}, r.handleCommittedTransaction)
	r.Register(message.CompleteTransaction{}, r.handleConsensusCompleteTransaction)
	r.Register(blockchain.WorkerBlock{}, r.handleWorkerBlock)
	r.Register(message.Experiment{}, r.handleExperiment)

	return r
}

func (r *WorkerBlockBuilderReplica) Start() {
	go r.Run()
	err := utils.Retry(r.gatewaynodeTransport.Dial, 100, time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
}

func (r *WorkerBlockBuilderReplica) IsExist(address common.Address) bool {
	var addressList []common.Address
	addressList = append(addressList, address)
	if utils.CalculateShardToSend(addressList)[0] == r.GetShard() {
		return true
	} else {
		return false
	}
}

/* Node Start */
func (r *WorkerBlockBuilderReplica) SendNodeStartMessage() {
	nodeStartMessage := message.NodeStartMessage{
		Message: "Start",
	}
	r.Broadcast(nodeStartMessage)
	log.Debugf("[Worker BlockBuilder %v] (SendNodeStartMessage) Send To Consensus Node Start Block Create", r.GetShard())
}

/* Message Handler */
func (r *WorkerBlockBuilderReplica) handleWorkerList(msg message.WorkerList) {
	r.WorkerBuilderTopic <- msg
}

func (r *WorkerBlockBuilderReplica) handleWorkerShardVariableRegisterRequest(msg message.WorkerSharedVariableRegisterRequest) {
	// G -> W
	r.WorkerBuilderTopic <- msg
}

func (r *WorkerBlockBuilderReplica) handleConsensusRegister(msg message.ConsensusNodeRegister) {
	r.ConsensusNodeTopic <- msg
}

func (r *WorkerBlockBuilderReplica) handleNodeStartAck(msg message.NodeStartAck) {
	r.ConsensusNodeTopic <- msg
}

/* paperexperiment Transaction Consensus */
func (r *WorkerBlockBuilderReplica) handleTransaction(tx message.Transaction) {
	log.Debugf("[WorkerBlockBuilder %v] Receive Transaction Hash: %v, Nonce: %v", r.GetShard(), tx.Hash, tx.Nonce)
	if tx.AbortAndRetryLatencyDissection.BlockWaitingTime == 0 {
		tx.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
	}
	if !tx.IsCrossShardTx {
		r.txcnt++
		// log.Debugf("[WorkerBlockBuilder %v] %vth Receive Transaction!!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), r.txcnt, tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
		// log.Debugf("[WorkerBlockBuilder %v] Start Local paperexperiment Protocol!!!", r.GetShard())

		// Builder Sign Transaction
		builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
		signed_transaction := message.BuilderSignedTransaction{
			Transaction: tx,
		}
		signed_transaction.Committee_sig = append(signed_transaction.Committee_sig, builder_signature)
		r.Broadcast(signed_transaction) // Send transaction to Consensus Nodes
	} else {

		if r.IsRootShard(&tx) {
			r.txcnt++
			tx.StartTime = time.Now().UnixMilli()
			// Start paperexperiment protocol
			// log.Debugf("[WorkerBlockBuilder %v] %vth Receive Transaction!!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), r.txcnt, tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
			// log.Debugf("[WorkerBlockBuilder %v] Start paperexperiment Protocol!!!", r.GetShard())
			// Builder Sign Transaction
			builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
			signed_transaction := message.BuilderSignedTransaction{
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
			// log.Debugf("[WorkerBlockBuilder %v] Receive Transaction From Root Shard!!! Tx: Hash: %v IsCross: %v, TXType: %v, From %v, To: %v, RwSet: %v", r.GetShard(), tx.Hash, tx.IsCrossShardTx, tx.TXType, tx.From, tx.To, tx.RwSet)
			// log.Debugf("[WorkerBlockBuilder %v] Start Vote Process!!!", r.GetShard())
			// Builder Sign Transaction
			builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
			signed_transaction := message.VoteBuilderSignedTransaction{
				Transaction: tx,
			}
			signed_transaction.Committee_sig = append(signed_transaction.Committee_sig, builder_signature)
			r.Broadcast(signed_transaction) // Send transaction to Consensus Nodes
			// Process Consensus Process
		}
	}
}

// Receive Transaction completed consensus from consensus nodes in root shard
func (r *WorkerBlockBuilderReplica) handleRootShardVotedTransaction(tx message.RootShardVotedTransaction) {
	log.Infof("[Worker BlockBuilder %v] Receive Consensus Complete Transaction From Consensus Node TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	if r.IsRootShard(&tx.Transaction) {
		if tx.IsCommit {
			r.RootShardVotedTransaction <- tx
		}
	} else {
		r.RootShardVotedTransaction <- tx
	}
}

func (r *WorkerBlockBuilderReplica) handleVotedTransaction(tx message.VotedTransaction) {
	log.Infof("[Worker BlockBuilder %v] Receive Voted Transaction From Other Shard TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	tx.AbortAndRetryLatencyDissection.Network2 = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.Network2
	tx.LatencyDissection.Network2 = tx.LatencyDissection.Network2 + tx.AbortAndRetryLatencyDissection.Network2
	tx.Transaction.AbortAndRetryLatencyDissection.DecideConsensusTime = time.Now().UnixMilli()

	builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
	tx.Committee_sig = append(tx.Committee_sig, builder_signature)
	r.Broadcast(tx)
}

func (r *WorkerBlockBuilderReplica) handleCommittedTransaction(tx message.CommittedTransaction) {
	if r.IsRootShard(&tx.Transaction) {
		tx.AbortAndRetryLatencyDissection.DecideConsensusTime = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.DecideConsensusTime
		tx.LatencyDissection.DecideConsensusTime = tx.LatencyDissection.DecideConsensusTime + tx.AbortAndRetryLatencyDissection.DecideConsensusTime
		tx.AbortAndRetryLatencyDissection.Network3 = time.Now().UnixMilli()
		log.DecideDebugf("[Worker BlockBuilder %v] Receive Committed Transaction TxHash: %v", r.GetShard(), tx.Transaction.Hash)
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
		log.DecideDebugf("[Worker BlockBuilder %v] Receive Committed Transaction from Root shard TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	}
	tx.AbortAndRetryLatencyDissection.Network3 = time.Now().UnixMilli() - tx.AbortAndRetryLatencyDissection.Network3
	tx.LatencyDissection.Network3 = tx.LatencyDissection.Network3 + tx.AbortAndRetryLatencyDissection.Network3
	tx.Transaction.AbortAndRetryLatencyDissection.CommitConsensusTime = time.Now().UnixMilli()
	builder_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Hash), nil)
	tx.Committee_sig = append(tx.Committee_sig, builder_signature)
	r.Broadcast(tx)
}

func (r *WorkerBlockBuilderReplica) handleConsensusCompleteTransaction(tx message.CompleteTransaction) {
	log.CommitDebugf("[Worker BlockBuilder %v] Complete Transaction TxHash: %v", r.GetShard(), tx.Transaction.Hash)
	r.AddCommittedTransaction(&tx.Transaction)
}

// Receive WorkerBlock completed consensus
func (r *WorkerBlockBuilderReplica) handleWorkerBlock(msg blockchain.WorkerBlock) {
	log.Infof("[Worker BlockBuilder %v] Receive Workerblock From ID: %v Blockhash: %v", r.GetShard(), msg.Proposer, msg.Block_hash)
	r.gatewaynodeTransport.Send(msg)
	r.SetBlockHeight(r.GetBlockHeight() + 1)
	// only logging
	// log.Errorf("[Worker BlockBuilder %v] State Commit Start BlockHeight %v", r.GetShard(), msg.Block_header.Block_height)
	cross := 0
	for _, tx := range msg.Transaction {
		if tx.IsCrossShardTx {
			cross++
		}
	}
	log.PerformanceInfof("[Worker BlockBuilder %v] Final Block 블록번호: %v, 크로스: %v, 로컬: %v, 전체: %v", r.GetShard(), msg.Block_header.Block_height, cross, len(msg.Transaction)-cross, len(msg.Transaction))
	// only logging
	// for _, lt := range msg.Transaction {
	// 	log.PerformanceInfof("[Worker BlockBuilder %v] Final Committed Transaction Hash: %v", r.GetShard(), lt.Hash)
	// }
	// only logging
	// log.PerformanceInfof("[Worker BlockBuilder %v] 남은 글로벌 시퀀스 수: %v, 남은 글로벌 스냅샷 수: %v", r.GetShard(), len(r.GetGlobalSequence()), len(r.GetGlobalSnapshot()))
}

func (r *WorkerBlockBuilderReplica) handleExperiment(msg message.Experiment) {
	r.WorkerBuilderTopic <- msg
}
