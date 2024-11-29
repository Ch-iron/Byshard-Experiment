package replica

import (
	"encoding/gob"
	"fmt"
	"sync"
	"time"

	"go.uber.org/atomic"

	"paperexperiment/blockchain"
	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/election"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/mempool"
	"paperexperiment/message"
	"paperexperiment/node"
	"paperexperiment/pacemaker"
	"paperexperiment/pbft"
	"paperexperiment/quorum"
	"paperexperiment/safety"
	"paperexperiment/types"
)

type Replica struct {
	node.Node
	safety.ConsensusSafety
	election.Election
	pd                  *mempool.Producer
	pm                  *pacemaker.Pacemaker
	pt                  *mempool.ManageTransactions
	start               chan bool // signal to start the node
	isStarted           atomic.Bool
	isByz               bool
	timer               *time.Timer // timeout for each view
	committedBlocks     chan *blockchain.ShardBlock
	forkedBlocks        chan *blockchain.ShardBlock
	reservedBlock       chan *blockchain.ShardBlock
	eventChan           chan interface{}
	preparingBlock      *blockchain.ShardBlock // block waiting for enough lockResponse
	reservedPreparBlock *blockchain.ShardBlock // reserved Preparing Block from ViewChange
	/* for monitoring node statistics */
	thrus                string
	lastViewTime         time.Time
	startTime            time.Time
	tmpTime              time.Time
	voteStart            time.Time
	totalCreateDuration  time.Duration
	totalProcessDuration time.Duration
	totalProposeDuration time.Duration
	totalDelay           time.Duration
	totalRoundTime       time.Duration
	totalVoteTime        time.Duration
	totalBlockSize       int
	receivedNo           int
	roundNo              int
	voteNo               int
	totalCommittedTx     int
	latencyNo            int
	proposedNo           int
	processedNo          int
	committedNo          int
	blockdatas           *mempool.Producer
}

// NewReplica creates a new replica instance
func NewReplica(id identity.NodeID, alg string, isByz bool, shard types.Shard) *Replica {
	r := new(Replica)
	r.Node = node.NewNode(id, isByz, shard)
	if isByz {
		log.Debugf("[%v %v] is Byzantine", r.ID())
	}

	r.Election = election.NewStatic(identity.NewNodeID(1), r.Shard(), config.Configuration.CommitteeNumber)
	r.isByz = isByz
	r.pd = mempool.NewProducer()
	r.pm = pacemaker.NewPacemaker(config.GetConfig().CommitteeNumber)
	r.pt = mempool.NewManageTransactions()
	r.start = make(chan bool)
	r.eventChan = make(chan interface{}, 16384)
	r.committedBlocks = make(chan *blockchain.ShardBlock, 100)
	r.forkedBlocks = make(chan *blockchain.ShardBlock, 100)
	r.reservedBlock = make(chan *blockchain.ShardBlock)
	r.preparingBlock = nil
	r.reservedPreparBlock = nil
	r.blockdatas = mempool.NewProducer()
	r.receivedNo = 0
	r.Register(blockchain.Block{}, r.HandleBlock)
	r.Register(blockchain.Accept{}, r.HandleAccept)
	r.Register(message.NodeStartMessage{}, r.HandleNodeStartMessage)
	r.Register(blockchain.ShardBlock{}, r.HandleShardBlock)
	r.Register(quorum.Vote{}, r.HandleVote)
	r.Register(quorum.Commit{}, r.HandleCommit)
	r.Register(pacemaker.TMO{}, r.HandleTmo)
	r.Register(pacemaker.TC{}, r.HandleTc)
	r.Register(message.CommunicatorSignedTransaction{}, r.HandleCommunicatorSignedTransaction)
	r.Register(message.VoteCommunicatorSignedTransaction{}, r.HandleVotingCommunicatorSignedTransaction)
	r.Register(message.SignedTransactionWithHeader{}, r.HandleSignedTransactionWithHeader)
	r.Register(quorum.TransactionVote{}, r.HandleTransactionVote)
	r.Register(quorum.TransactionCommit{}, r.HandleTransactionCommit)
	r.Register(message.TransactionAccept{}, r.HandleTransactionAccept)
	r.Register(message.VotedTransaction{}, r.HandleVotedTransaction)
	r.Register(message.VotedTransactionWithHeader{}, r.HandleVotedTransactionWithHeader)
	r.Register(quorum.DecideTransactionVote{}, r.HandleDecideTransactionVote)
	r.Register(quorum.DecideTransactionCommit{}, r.HandleDecideTransactionCommit)
	r.Register(message.DecideTransactionAccept{}, r.HandleDecideTransactionAccept)
	r.Register(message.CommittedTransaction{}, r.HandleCommittedTransaction)
	r.Register(message.CommittedTransactionWithHeader{}, r.HandleCommittedTransactionWithHeader)
	r.Register(quorum.CommitTransactionVote{}, r.HandleCommittedTransactionVote)
	r.Register(quorum.CommitTransactionCommit{}, r.HandleCommittedTransactionCommit)
	r.Register(message.CommitTransactionAccept{}, r.HandleCommittedTransactionAccept)
	r.Register(quorum.LocalTransactionVote{}, r.HandleLocalTransactionVote)
	r.Register(quorum.LocalTransactionCommit{}, r.HandleLocalTransactionCommit)
	r.Register(message.Query{}, r.handleQuery)

	gob.Register(message.ConsensusNodeRegister{})
	gob.Register(message.NodeStartAck{})
	gob.Register(blockchain.Block{})
	gob.Register(blockchain.Accept{})
	gob.Register(message.NodeStartMessage{})
	gob.Register(blockchain.ShardBlock{})
	gob.Register(quorum.Vote{})
	gob.Register(quorum.Commit{})
	gob.Register(pacemaker.TC{})
	gob.Register(pacemaker.TMO{})
	gob.Register(message.Transaction{})
	gob.Register(message.CommunicatorSignedTransaction{})
	gob.Register(message.VoteCommunicatorSignedTransaction{})
	gob.Register(message.RootShardVotedTransaction{})
	gob.Register(message.SignedTransactionWithHeader{})
	gob.Register(quorum.TransactionVote{})
	gob.Register(quorum.TransactionCommit{})
	gob.Register(message.TransactionAccept{})
	gob.Register(message.VotedTransaction{})
	gob.Register(message.VotedTransactionWithHeader{})
	gob.Register(quorum.DecideTransactionVote{})
	gob.Register(quorum.DecideTransactionCommit{})
	gob.Register(message.DecideTransactionAccept{})
	gob.Register(message.CommittedTransactionWithHeader{})
	gob.Register(quorum.CommitTransactionVote{})
	gob.Register(quorum.CommitTransactionCommit{})
	gob.Register(message.CommitTransactionAccept{})
	gob.Register(quorum.LocalTransactionVote{})
	gob.Register(quorum.LocalTransactionCommit{})
	gob.Register(message.CommittedTransaction{})
	gob.Register(message.Experiment{})

	// Is there a better way to reduce the number of parameters?
	r.ConsensusSafety = pbft.NewPBFT(r.Node, r.pm, r.pt, r.Election, r.committedBlocks, r.forkedBlocks, r.reservedBlock, r.blockdatas)
	return r
}

/* Message Handlers */
// Block Consensus Process Start
// Send Block completed consensus to communicator at the process end
func (r *Replica) HandleNodeStartMessage(nodestartmessage message.NodeStartMessage) {
	log.Debugf("Shard: %v, id: %v Receive Node Start Message", r.Shard(), r.ID())

	r.receivedNo++
	r.startSignal()
	// when get block kicks off the protocol
	if r.pm.GetCurView() == 0 {
		log.Debugf("[%v %v] start protocol ", r.ID(), r.Shard())
		r.pm.AdvanceView(0)
	}
	r.SendToCommunicator(message.NodeStartAck{
		ID: r.ID(),
	})
}

/* paperexperiment Handlers */
func (r *Replica) HandleCommunicatorSignedTransaction(tx message.CommunicatorSignedTransaction) {
	// log.Debugf("[HandleCommunicatorSignedTransaction] Shard: %v, id %v Receive Transaction From Communicator Hash: %v", r.Shard(), r.ID(), tx.Transaction.Hash)
	r.receivedNo++
	if !r.IsLeader(r.ID(), r.pm.GetCurView(), r.pm.GetCurEpoch()) {
		return
	}
	tx_header := message.TransactionHeader{
		View:     r.pm.GetCurView(),
		Epoch:    r.pm.GetCurEpoch(),
		Proposer: r.ID(),
	}

	// log.Debugf("[Consensus Leader] Shard: %v, id %v Receive Transaction From Communicator Hash: %v", r.Shard(), r.ID(), tx.Transaction.Hash)
	transactionIsVerified, _ := crypto.PubVerify(tx.Committee_sig[0], crypto.IDToByte(tx.Transaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] Received a block from communicator with an invalid signature", r.ID(), r.Shard())
		return
	}

	leader_signed_tx := message.SignedTransaction(tx)
	if !tx.IsCrossShardTx { // Local Transaction
		go r.InitLocalLockStep(&message.SignedTransactionWithHeader{
			Header:      tx_header,
			Transaction: leader_signed_tx,
		})
	} else { // Cross Transaction
		go r.InitVoteStep(&message.SignedTransactionWithHeader{
			Header:      tx_header,
			Transaction: leader_signed_tx,
		})
	}
}

func (r *Replica) HandleVotingCommunicatorSignedTransaction(tx message.VoteCommunicatorSignedTransaction) {
	// log.Debugf("[HandleVotingCommunicatorSignedTransaction] Shard: %v, id %v Receive Transaction From Communicator Hash: %v", r.Shard(), r.ID(), tx.Transaction.Hash)
	r.receivedNo++
	if !r.IsLeader(r.ID(), r.pm.GetCurView(), r.pm.GetCurEpoch()) {
		return
	}
	tx_header := message.TransactionHeader{
		View:     r.pm.GetCurView(),
		Epoch:    r.pm.GetCurEpoch(),
		Proposer: r.ID(),
	}

	// log.Debugf("[Consensus Leader] Shard: %v, id %v Receive Transaction From Communicator Hash: %v", r.Shard(), r.ID(), tx.Transaction.Hash)
	transactionIsVerified, _ := crypto.PubVerify(tx.Committee_sig[0], crypto.IDToByte(tx.Transaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] Received a block from communicator with an invalid signature", r.ID(), r.Shard())
		return
	}

	leader_signed_tx := message.SignedTransaction(tx)
	go r.InitVoteStep(&message.SignedTransactionWithHeader{
		Header:      tx_header,
		Transaction: leader_signed_tx,
	})
}

func (r *Replica) HandleSignedTransactionWithHeader(tx message.SignedTransactionWithHeader) {
	// log.Debugf("[HandleSignedTransactionWithHeader] Shard: %v, id %v Receive Transaction From Leader Hash: %v", r.Shard(), r.ID(), tx.Transaction.Hash)
	r.receivedNo++
	if !r.IsCommittee(r.ID(), tx.Header.Epoch) {
		return
	}

	// log.Debugf("[Consensus Committee] Shard: %v, id %v Receive Transaction From Leader Hash: %v", r.Shard(), r.ID(), tx.Transaction.Transaction.Hash)
	transactionIsVerified, _ := crypto.PubVerify(tx.Transaction.Committee_sig[1], crypto.IDToByte(tx.Transaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] Received a block from leader with an invalid signature", r.ID(), r.Shard())
		return
	}

	if !tx.Transaction.IsCrossShardTx {
		go r.InitLocalLockStep(&tx)
	} else {
		go r.InitVoteStep(&tx)
	}
}

func (r *Replica) HandleLocalTransactionVote(vote quorum.LocalTransactionVote) {
	if !r.IsCommittee(r.ID(), vote.Epoch) {
		return
	}

	// log.LocalDebugf("[%v %v] (HandleLocalTransactionVote) received vote from [%v], view: %v epoch: %v Hash: %v ordercount: %v", r.ID(), r.Shard(), vote.Voter, vote.View, vote.Epoch, vote.TransactionHash, vote.OrderCount)
	r.ConsensusSafety.ProcessLocalTransactionVote(&vote)
}

func (r *Replica) HandleLocalTransactionCommit(commitMsg quorum.LocalTransactionCommit) {
	if !r.IsCommittee(r.ID(), commitMsg.Epoch) {
		return
	}

	// log.LocalDebugf("[%v %v] (HandleLocalTransactionCommit) received commit from [%v], view: %v epoch: %v Hash: %v ordercount: %v", r.ID(), r.Shard(), commitMsg.Voter, commitMsg.View, commitMsg.Epoch, commitMsg.TransactionHash, commitMsg.OrderCount)
	r.ConsensusSafety.ProcessLocalTransactionCommit(&commitMsg)
}

func (r *Replica) HandleTransactionVote(vote quorum.TransactionVote) {
	if !r.IsCommittee(r.ID(), vote.Epoch) {
		return
	}

	// log.VoteDebugf("[%v %v] (HandleTransactionVote) received vote from [%v], view: %v epoch: %v Hash: %v", r.ID(), r.Shard(), vote.Voter, vote.View, vote.Epoch, vote.TransactionHash)
	r.ConsensusSafety.ProcessTransactionVote(&vote)
}

func (r *Replica) HandleTransactionCommit(commitMsg quorum.TransactionCommit) {
	if !r.IsCommittee(r.ID(), commitMsg.Epoch) {
		return
	}

	// log.VoteDebugf("[%v %v] (HandleTransactionCommit) received commit from [%v], view: %v epoch: %v Hash: %v", r.ID(), r.Shard(), commitMsg.Voter, commitMsg.View, commitMsg.Epoch, commitMsg.TransactionHash)
	r.ConsensusSafety.ProcessTransactionCommit(&commitMsg)
}

func (r *Replica) HandleTransactionAccept(accept message.TransactionAccept) {
	log.VoteDebugf("[%v %v] (HandleTransactionAccept) received accept from leader Hash: %v", r.ID(), r.Shard(), accept.Transaction.Hash)
	if accept.Transaction.IsCrossShardTx {
		r.ConsensusSafety.ProcessTransactionAccept(&accept)
	} else {
		r.ConsensusSafety.ProcessLocalTransactionAccept(&accept)
	}
}

func (r *Replica) HandleVotedTransaction(votedTransaction message.VotedTransaction) {
	if !r.IsCommittee(r.ID(), r.pm.GetCurEpoch()) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(votedTransaction.Committee_sig[0], crypto.IDToByte(votedTransaction.RootShardVotedTransaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] (HandleVotedTransaction) Received a block from communicator with an invalid signature", r.ID(), r.Shard())
		return
	}

	tx_header := message.TransactionHeader{
		View:     r.pm.GetCurView(),
		Epoch:    r.pm.GetCurEpoch(),
		Proposer: r.ID(),
	}

	// log.DecideDebugf("[%v %v] (HandleVotedTransaction) received votedtransaction from communicator Hash: %v", r.ID(), r.Shard(), votedTransaction.Hash)
	r.InitDecideStep(&message.VotedTransactionWithHeader{
		Header:      tx_header,
		Transaction: votedTransaction,
	})
}

func (r *Replica) HandleVotedTransactionWithHeader(votedTransaction message.VotedTransactionWithHeader) {
	if !r.IsCommittee(r.ID(), r.pm.GetCurEpoch()) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(votedTransaction.Transaction.Committee_sig[1], crypto.IDToByte(votedTransaction.Transaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] Received a block from leader with an invalid signature", r.ID(), r.Shard())
		return
	}

	// log.DecideDebugf("[%v %v] (HandleVotedTransactionWithHeader) received votedtransaction from leader Hash: %v", r.ID(), r.Shard(), votedTransaction.Hash)
	r.ProcessDecideStep(&votedTransaction)
}

func (r *Replica) HandleDecideTransactionVote(vote quorum.DecideTransactionVote) {
	if !r.IsCommittee(r.ID(), vote.Epoch) {
		return
	}

	// log.DecideDebugf("[%v %v] (HandleDecideTransactionVote) received vote from [%v], view: %v epoch: %v Hash: %v OrderCount: %v", r.ID(), r.Shard(), vote.Voter, vote.View, vote.Epoch, vote.TransactionHash, vote.OrderCount)
	r.ConsensusSafety.ProcessDecideTransactionVote(&vote)
}

func (r *Replica) HandleDecideTransactionCommit(commitMsg quorum.DecideTransactionCommit) {
	if !r.IsCommittee(r.ID(), commitMsg.Epoch) {
		return
	}

	// log.DecideDebugf("[%v %v] (HandleDecideTransactionCommit) received commit from [%v], view: %v epoch: %v Hash: %v", r.ID(), r.Shard(), commitMsg.Voter, commitMsg.View, commitMsg.Epoch, commitMsg.TransactionHash)
	r.ConsensusSafety.ProcessDecideTransactionCommit(&commitMsg)
}

func (r *Replica) HandleDecideTransactionAccept(accept message.DecideTransactionAccept) {
	log.DecideDebugf("[%v %v] (HandleDecideTransactionAccept) received accept from leader Hash: %v", r.ID(), r.Shard(), accept.Transaction.Hash)
	r.ConsensusSafety.ProcessDecideTransactionAccept(&accept)
}

func (r *Replica) HandleCommittedTransaction(committedTransaction message.CommittedTransaction) {
	if !r.IsLeader(r.ID(), r.pm.GetCurView(), r.pm.GetCurEpoch()) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(committedTransaction.Committee_sig[0], crypto.IDToByte(committedTransaction.RootShardVotedTransaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] (HandleCommittedTransaction) Received a block from communicator with an invalid signature", r.ID(), r.Shard())
		return
	}

	tx_header := message.TransactionHeader{
		View:     r.pm.GetCurView(),
		Epoch:    r.pm.GetCurEpoch(),
		Proposer: r.ID(),
	}

	// log.CommitDebugf("[%v %v] (HandleCommittedTransaction) received committedtransaction from communicator Hash: %v", r.ID(), r.Shard(), committedTransaction.Transaction.Hash)
	r.InitCommitStep(&message.CommittedTransactionWithHeader{
		Header:      tx_header,
		Transaction: committedTransaction,
	})
}

func (r *Replica) HandleCommittedTransactionWithHeader(committedTransactionwithheader message.CommittedTransactionWithHeader) {
	if !r.IsCommittee(r.ID(), committedTransactionwithheader.Header.Epoch) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(committedTransactionwithheader.Transaction.Committee_sig[1], crypto.IDToByte(committedTransactionwithheader.Transaction.Hash))
	if !transactionIsVerified {
		log.Errorf("[%v %v] Received a block from leader with an invalid signature", r.ID(), r.Shard())
		return
	}

	// log.CommitDebugf("[%v %v] (HandleCommittedTransactionWithHeader) received committedtransaction from leader Hash: %v", r.ID(), r.Shard(), committedTransactionwithheader.Transaction.Hash)
	r.InitCommitStep(&committedTransactionwithheader)
}

func (r *Replica) HandleCommittedTransactionVote(vote quorum.CommitTransactionVote) {
	if !r.IsCommittee(r.ID(), vote.Epoch) {
		return
	}

	// log.CommitDebugf("[%v %v] (HandleCommittedTransactionVote) received vote from [%v], view: %v epoch: %v Hash: %v", r.ID(), r.Shard(), vote.Voter, vote.View, vote.Epoch, vote.TransactionHash)
	r.ConsensusSafety.ProcessCommitTransactionVote(&vote)
}

func (r *Replica) HandleCommittedTransactionCommit(commitMsg quorum.CommitTransactionCommit) {
	if !r.IsCommittee(r.ID(), commitMsg.Epoch) {
		return
	}

	// log.CommitDebugf("[%v %v] (HandleCommittedTransactionCommit) received commit from [%v], view: %v epoch: %v Hash: %v", r.ID(), r.Shard(), commitMsg.Voter, commitMsg.View, commitMsg.Epoch, commitMsg.TransactionHash)
	r.ConsensusSafety.ProcessCommitTransactionCommit(&commitMsg)
}

func (r *Replica) HandleCommittedTransactionAccept(accept message.CommitTransactionAccept) {
	log.CommitDebugf("[%v %v] (HandleCommittedTransactionAccept) received accept from leader Hash: %v", r.ID(), r.Shard(), accept.Transaction.Hash)
	r.ConsensusSafety.ProcessCommitTransactionAccept(&accept)
}

/* Block Handlers */
func (r *Replica) HandleShardBlock(block blockchain.ShardBlock) {
	if !r.IsCommittee(r.ID(), block.Block_header.Epoch_num) {
		return
	}

	if block.Transaction == nil {
		block.Transaction = make([]*message.Transaction, 0)
	}

	// log.Debugf("[%v %v] (HandleBlock) received block blockHeight %v view %v epoch %v ID %v prevID %v from leader %v", r.ID(), r.Shard(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash, block.Block_header.Prev_block_hash, block.Proposer)
	r.eventChan <- block
}

func (r *Replica) HandleBlock(block blockchain.Block) {
	if !r.IsCommittee(r.ID(), block.Epoch) {
		return
	}

	log.Debugf("[%v %v] (HandleBlock) received block blockHeight %v view %v epoch %v ID %v prevID %v from leader %v", r.ID(), r.Shard(), block.BlockHeight, block.View, block.Epoch, block.ID, block.PrevID, block.Proposer)
	r.eventChan <- block
}

func (r *Replica) HandleVote(vote quorum.Vote) {
	if !r.IsCommittee(r.ID(), vote.Epoch) {
		return
	}

	// prevent voting last blockheight
	if vote.BlockHeight < r.GetHighBlockHeight() {
		return
	}

	// log.Debugf("[%v %v] (HandleVote) received vote from [%v], blockHeight: %v view: %v epoch: %v ID: %v", r.ID(), r.Shard(), vote.Voter, vote.BlockHeight, vote.View, vote.Epoch, vote.BlockID)
	r.eventChan <- vote
}

func (r *Replica) HandleCommit(commitMsg quorum.Commit) {
	if !r.IsCommittee(r.ID(), commitMsg.Epoch) {
		return
	}

	if commitMsg.BlockHeight < r.GetLastBlockHeight() {
		return
	}
	// log.Debugf("[%v %v] (HandleCommit) received commit from [%v], blockheight: %v view: %v epoch: %v ID: %v", r.ID(), r.Shard(), commitMsg.Voter, commitMsg.BlockHeight, commitMsg.View, commitMsg.Epoch, commitMsg.BlockID)
	r.eventChan <- commitMsg
}

func (r *Replica) HandleAccept(accept blockchain.Accept) {
	commitedBlock := accept.CommittedBlock
	if commitedBlock.Block_header.Block_height < r.GetLastBlockHeight() {
		return
	}
	log.Debugf("[%v %v] (HandleAccept) received accept from [%v], blockheight: %v view: %v epoch: %v ID: %v", r.ID(), r.Shard(), commitedBlock.Proposer, commitedBlock.Block_header.Block_height, commitedBlock.Block_header.View_num, commitedBlock.Block_header.Epoch_num, commitedBlock.Block_hash)
	r.eventChan <- accept
}

func (r *Replica) HandleTmo(tmo pacemaker.TMO) {
	if tmo.View < r.pm.GetCurView() {
		return
	}

	if !r.IsCommittee(r.ID(), tmo.Epoch) {
		return
	}

	log.Debugf("[%v %v] (HandleTmo) received a timeout from %v for view %v", r.ID(), r.Shard(), tmo.NodeID, tmo.View)
	r.eventChan <- tmo
}

func (r *Replica) HandleTc(tc pacemaker.TC) {
	if !(tc.NewView > r.pm.GetCurView()) {
		return
	}
	log.Debugf("[%v %v] (HandleTc) received a tc from %v for view %v", r.ID(), r.Shard(), tc.NodeID, tc.NewView)
	r.eventChan <- tc
}

// handleQuery replies a query with the statistics of the node
func (r *Replica) handleQuery(m message.Query) {
	latency := float64(r.totalDelay.Milliseconds()) / float64(r.latencyNo)
	r.thrus += fmt.Sprintf("Time: %v s. Throughput: %v txs/s\n", time.Since(r.startTime).Seconds(), float64(r.totalCommittedTx)/time.Since(r.tmpTime).Seconds())
	r.totalCommittedTx = 0
	r.tmpTime = time.Now()
	status := fmt.Sprintf("Latency: %v\n%s", latency, r.thrus)
	m.Reply(message.QueryReply{Info: status})
}

// handleRequestLeader replies a Leader of the node
func (r *Replica) handleRequestLeader(m message.RequestLeader) {
	if r.pm.GetCurView() == 0 {
		log.Debugf("[%v %v] start protocol ", r.ID(), r.Shard())
		r.pm.AdvanceView(0)
	}

	leader := r.FindLeaderFor(r.pm.GetCurView(), r.pm.GetCurEpoch())
	m.Reply(message.RequestLeaderReply{Leader: fmt.Sprint(leader)})
}

/* Processors */
func (r *Replica) processNewView(curEpoch types.Epoch, curView types.View) {

	log.Debugf("[%v %v] process new round for View: %v Epoch: %v 리더 노드[%v]", r.ID(), r.Shard(), curView, curEpoch, r.FindLeaderFor(curView, curEpoch))
	if !r.IsLeader(r.ID(), curView, curEpoch) {
		// Committee Check
		return
	}

	r.SetRole(types.LEADER)
	r.eventChan <- types.EpochView{Epoch: curEpoch, View: curView}
}

func (r *Replica) initConsensus(epoch types.Epoch, view types.View) {
	createStart := time.Now()
	var (
		block *blockchain.ShardBlock
	)

	if block == nil {
		// view change occured
		return
	}

	log.Debugf("[%v %v] (proposeLockRequest) block is built for epoch: %v view: %v blockheight: %v ID: %v", r.ID(), r.Shard(), block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height, block.Block_hash)

	// r.totalBlockSize += len(block.SCPayload)
	r.proposedNo++
	createEnd := time.Now()
	createDuration := createEnd.Sub(createStart)
	block.Block_header.Timestamp = time.Now()
	r.totalCreateDuration += createDuration

	r.BroadcastToSome(r.FindCommitteesFor(block.Block_header.Epoch_num), block)
	// _ = r.ConsensusSafety.ProcessShardBlock(block)
	r.voteStart = time.Now()
	r.preparingBlock = nil
}

func (r *Replica) processCommittedBlock(block *blockchain.ShardBlock) {
	r.committedNo++
	log.Infof("[%v %v] (processCommittedBlock) block is committed No. of transactions: , blockheight %v view %v epoch %v, id: %v", r.ID(), r.Shard(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	if !r.IsLeader(r.ID(), block.Block_header.View_num, block.Block_header.Epoch_num) {
		return
	}
}

func (r *Replica) processForkedBlock(block *blockchain.ShardBlock) {}

/* Listners */

// ListenCommittedBlocks listens committed blocks and forked blocks from the protocols
func (r *Replica) ListenCommittedBlocks() {
	for {
		select {
		case committedBlock := <-r.committedBlocks:
			r.processCommittedBlock(committedBlock)
		case forkedBlock := <-r.forkedBlocks:
			r.processForkedBlock(forkedBlock)
		case reservedPrepareBlock := <-r.reservedBlock:
			log.Debugf("[%v %v] reservedPrepareBlock exist, blockheight %v view %v", r.ID(), r.Shard(), reservedPrepareBlock.Block_header.Block_height, reservedPrepareBlock.Block_header.View_num)
			r.reservedPreparBlock = reservedPrepareBlock
		}
	}
}

// ListenLocalEvent listens new view and timeout events
func (r *Replica) ListenLocalEvent() {
	r.lastViewTime = time.Now()
	r.timer = time.NewTimer(r.pm.GetTimerForView())
	for {
		// Committee Check, if Validator, stop timer
		if r.Role() == types.VALIDATOR {
			r.timer.Stop()
		} else {
			switch r.State() {
			case types.READY:
				r.timer.Reset(r.pm.GetTimerForView())
			case types.VIEWCHANGING:
				r.timer.Reset(r.pm.GetTimerForViewChange())
			}
		}
	L:
		for {
			select {
			case EpochView := <-r.pm.EnteringViewEvent():
				var (
					epoch = EpochView.Epoch
					view  = EpochView.View
				)
				if view >= 2 {
					r.totalVoteTime += time.Since(r.voteStart)
				}
				// measure round time
				now := time.Now()
				lasts := now.Sub(r.lastViewTime)
				r.totalRoundTime += lasts
				r.roundNo++
				r.lastViewTime = now
				// if committee, set role committee. else, set role validator
				if r.IsCommittee(r.ID(), epoch) {
					r.SetRole(types.COMMITTEE)
				} else {
					r.SetRole(types.VALIDATOR)
				}
				log.Debugf("[%v %v] the last view %v lasts %v milliseconds", r.ID(), r.Shard(), view-1, lasts.Milliseconds())
				// r.eventChan <- EpochView
				r.processNewView(epoch, view)
				break L
			case newView := <-r.pm.EnteringTmoEvent():
				// Not ViewChange State: f + 1 tmo obtained : B cond
				// ViewChange State: higher anchorview tmo obtained : D cond
				log.Debugf("[%v %v] view-change start on for newview %v", r.ID(), r.Shard(), newView)
				r.ConsensusSafety.ProcessLocalTmo(r.pm.GetCurView())
				break L
			case <-r.timer.C:
				// already in viewchange state : C cond
				log.Debugf("[%v %v] timeout occured start viewchange", r.ID(), r.Shard())

				if r.State() == types.VIEWCHANGING {
					r.pm.UpdateNewView(r.pm.GetNewView() + 1)
				}
				r.ConsensusSafety.ProcessLocalTmo(r.pm.GetCurView())

				break L
			}
		}
	}
}

func (r *Replica) startSignal() {
	if !r.isStarted.Load() {
		r.startTime = time.Now()
		r.tmpTime = time.Now()
		log.Debugf("노드[%v %v] 부팅중", r.ID(), r.Shard())
		r.isStarted.Store(true)
		r.start <- true
	}
}

// Start event loop
func (r *Replica) Start(wg *sync.WaitGroup) {
	go r.Run(wg)

	wg.Add(1)
	committes := r.ElectCommittees(crypto.MakeID(0), 1)
	log.Debugf("[%v %v] elect new committees %v for new epoch 1", r.ID(), r.Shard(), committes)
	if r.IsCommittee(r.ID(), 1) {
		r.SetRole(types.COMMITTEE)
	}
	// wait for the start signal
	<-r.start
	interval := time.NewTicker(time.Millisecond * 1000)
	if r.IsLeader(r.ID(), r.pm.GetCurView(), r.pm.GetCurEpoch()) {
		go func() {
			for range interval.C {
				log.Debugf("[%v %v] Start CreateBlock!!!!!", r.ID(), r.Shard())
				block := r.ConsensusSafety.CreateShardBlock()
				block.Proposer = r.ID()
				leader_signature, _ := crypto.PrivSign(crypto.IDToByte(block.Block_hash), nil)
				block.Committee_sig = append(block.Committee_sig, leader_signature)

				r.BroadcastToSome(r.FindCommitteesFor(block.Block_header.Epoch_num), block)
				_ = r.ConsensusSafety.ProcessShardBlock(block)
				r.voteStart = time.Now()
				r.preparingBlock = nil
			}
		}()
	}
	go r.ListenLocalEvent()
	go r.ListenCommittedBlocks()
	go func() {
		for r.isStarted.Load() {
			event := <-r.eventChan
			switch v := event.(type) {
			case types.EpochView:
				r.initConsensus(v.Epoch, v.View)
			case blockchain.ShardBlock:
				_ = r.ConsensusSafety.ProcessShardBlock(&v)
				r.voteStart = time.Now()
				r.processedNo++
			case quorum.Vote:
				startProcessTime := time.Now()
				r.ConsensusSafety.ProcessVote(&v)
				processingDuration := time.Since(startProcessTime)
				r.totalVoteTime += processingDuration
				r.voteNo++
			case quorum.Commit:
				r.ConsensusSafety.ProcessCommit(&v)
			case blockchain.Accept:
				r.ConsensusSafety.ProcessAccept(&v)
			case pacemaker.TMO:
				r.ConsensusSafety.ProcessRemoteTmo(&v)
			case pacemaker.TC:
				r.ConsensusSafety.ProcessTC(&v)
			}
		}
	}()
	wg.Wait()
}
