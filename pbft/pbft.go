package pbft

import (
	"fmt"
	"sync"
	"time"

	"paperexperiment/blockchain"
	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/election"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/measure"
	"paperexperiment/mempool"
	"paperexperiment/message"
	"paperexperiment/node"
	"paperexperiment/pacemaker"
	"paperexperiment/quorum"
	"paperexperiment/types"
	"paperexperiment/utils"

	"github.com/ethereum/go-ethereum/common"
)

const FORK = "fork"

type PBFT struct {
	node.Node
	election.Election
	pm                   *pacemaker.Pacemaker
	pt                   *mempool.ManageTransactions
	lastVotedBlockHeight types.BlockHeight
	preferredBlockHeight types.BlockHeight
	highQC               *quorum.QC
	highCQC              *quorum.QC
	bc                   *blockchain.BlockChain
	voteQuorum           quorum.Quorum
	commitQuorum         quorum.Quorum
	committedBlocks      chan *blockchain.ShardBlock
	forkedBlocks         chan *blockchain.ShardBlock
	reservedBlock        chan *blockchain.ShardBlock
	updatedQC            chan *quorum.QC
	lastCreatedBlockQC   *quorum.QC
	bufferedQCs          map[common.Hash]*quorum.QC
	bufferedCQCs         map[common.Hash]*quorum.QC
	bufferedBlocks       map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.ShardBlock
	bufferedAccepts      map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.Accept
	agreeingBlocks       map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.ShardBlock
	mu                   sync.Mutex

	detectedTmos map[identity.NodeID]*pacemaker.TMO

	agreeingTransactions          map[common.Hash]map[int]*message.SignedTransactionWithHeader
	agreeingVoteTransactions      map[common.Hash]map[int]*quorum.TransactionQC
	agreeingLocalTransactions     map[common.Hash]map[int]*message.SignedTransactionWithHeader
	agreeingLocalVoteTransactions map[common.Hash]map[int]*quorum.LocalTransactionQC
	decidingTransactions          map[common.Hash]map[int]*message.VotedTransactionWithHeader
	decidingVoteTransactions      map[common.Hash]map[int]*quorum.DecideTransactionQC
	committingTransactions        map[common.Hash]map[int]*message.CommittedTransactionWithHeader
	committingVoteTransactions    map[common.Hash]map[int]*quorum.CommitTransactionQC

	transactionbufferedQCs        map[common.Hash]map[int]*quorum.TransactionQC
	transactionbufferedCQCs       map[common.Hash]map[int]*quorum.TransactionQC
	decidetransactionbufferedQCs  map[common.Hash]map[int]*quorum.DecideTransactionQC
	decidetransactionbufferedCQCs map[common.Hash]map[int]*quorum.DecideTransactionQC
	committransactionbufferedQCs  map[common.Hash]map[int]*quorum.CommitTransactionQC
	committransactionbufferedCQCs map[common.Hash]map[int]*quorum.CommitTransactionQC
	localtransactionbufferedQCs   map[common.Hash]map[int]*quorum.LocalTransactionQC
	localtransactionbufferedCQCs  map[common.Hash]map[int]*quorum.LocalTransactionQC

	transactionQuorum      map[common.Hash]*decideMajority
	committedTransaction   map[common.Hash]*message.Transaction
	lockTable_variable     map[common.Address]map[common.Hash][]*types.Lock
	lockTable_transaction  map[common.Hash][]*types.Lock
	committed_transactions *mempool.Producer

	locktablemu   sync.Mutex
	agreemu       sync.Mutex
	decidemu      sync.Mutex
	commitmu      sync.Mutex
	localmu       sync.Mutex
	majoritymu    sync.RWMutex
	executemu     sync.Mutex
	consensustxmu sync.Mutex
	measuremu     sync.RWMutex

	transactionvoteQuorum         quorum.TransactionQuorum
	transactioncommitQuorum       quorum.TransactionQuorum
	decidetransactionvoteQuorum   quorum.DecideTransactionQuorum
	decidetransactioncommitQuorum quorum.DecideTransactionQuorum
	committransactionvoteQuorum   quorum.CommitTransactionQuorum
	committransactioncommitQuorum quorum.CommitTransactionQuorum
	localtransactionvoteQuorum    quorum.LocalTransactionQuorum
	localtransactioncommitQuorum  quorum.LocalTransactionQuorum

	totalConsensusTx int

	pbftMeasure *measure.PBFTMeasure
}

func NewPBFT(
	node node.Node,
	pm *pacemaker.Pacemaker,
	pt *mempool.ManageTransactions,
	elec election.Election,
	committedBlocks chan *blockchain.ShardBlock,
	forkedBlocks chan *blockchain.ShardBlock,
	reservedBlock chan *blockchain.ShardBlock,
	blockdatas *mempool.Producer) *PBFT {
	pb := new(PBFT)
	pb.Node = node
	pb.Election = elec
	pb.pm = pm
	pb.pt = pt
	pb.bc = blockchain.NewBlockchain(pb.Shard(), config.GetConfig().DefaultBalance, pb.ID().Node())
	pb.voteQuorum = quorum.NewVoteQuorum(config.GetConfig().CommitteeNumber)
	pb.commitQuorum = quorum.NewCommitQuorum(config.GetConfig().CommitteeNumber)
	pb.bufferedBlocks = make(map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.ShardBlock)
	pb.bufferedAccepts = make(map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.Accept)
	pb.agreeingBlocks = make(map[types.Epoch]map[types.View]map[types.BlockHeight]*blockchain.ShardBlock)
	pb.bufferedQCs = make(map[common.Hash]*quorum.QC)
	pb.highQC = &quorum.QC{View: 0}
	pb.bufferedCQCs = make(map[common.Hash]*quorum.QC)
	pb.highCQC = &quorum.QC{View: 0}
	pb.committedBlocks = committedBlocks
	pb.forkedBlocks = forkedBlocks
	pb.reservedBlock = reservedBlock
	pb.lastCreatedBlockQC = &quorum.QC{View: 0}
	pb.updatedQC = make(chan *quorum.QC, 1)
	pb.detectedTmos = make(map[identity.NodeID]*pacemaker.TMO)

	pb.agreeingTransactions = make(map[common.Hash]map[int]*message.SignedTransactionWithHeader)
	pb.agreeingLocalTransactions = make(map[common.Hash]map[int]*message.SignedTransactionWithHeader)
	pb.agreeingVoteTransactions = make(map[common.Hash]map[int]*quorum.TransactionQC)
	pb.agreeingLocalVoteTransactions = make(map[common.Hash]map[int]*quorum.LocalTransactionQC)
	pb.decidingTransactions = make(map[common.Hash]map[int]*message.VotedTransactionWithHeader)
	pb.decidingVoteTransactions = make(map[common.Hash]map[int]*quorum.DecideTransactionQC)
	pb.committingTransactions = make(map[common.Hash]map[int]*message.CommittedTransactionWithHeader)
	pb.committingVoteTransactions = make(map[common.Hash]map[int]*quorum.CommitTransactionQC)

	pb.transactionbufferedQCs = make(map[common.Hash]map[int]*quorum.TransactionQC)
	pb.transactionbufferedCQCs = make(map[common.Hash]map[int]*quorum.TransactionQC)
	pb.decidetransactionbufferedQCs = make(map[common.Hash]map[int]*quorum.DecideTransactionQC)
	pb.decidetransactionbufferedCQCs = make(map[common.Hash]map[int]*quorum.DecideTransactionQC)
	pb.committransactionbufferedQCs = make(map[common.Hash]map[int]*quorum.CommitTransactionQC)
	pb.committransactionbufferedCQCs = make(map[common.Hash]map[int]*quorum.CommitTransactionQC)
	pb.localtransactionbufferedQCs = make(map[common.Hash]map[int]*quorum.LocalTransactionQC)
	pb.localtransactionbufferedCQCs = make(map[common.Hash]map[int]*quorum.LocalTransactionQC)

	pb.transactionQuorum = make(map[common.Hash]*decideMajority)
	pb.committedTransaction = make(map[common.Hash]*message.Transaction, 0)
	pb.lockTable_variable = make(map[common.Address]map[common.Hash][]*types.Lock)
	pb.lockTable_transaction = make(map[common.Hash][]*types.Lock)
	pb.committed_transactions = blockdatas

	pb.locktablemu = sync.Mutex{}
	pb.agreemu = sync.Mutex{}
	pb.decidemu = sync.Mutex{}
	pb.commitmu = sync.Mutex{}
	pb.localmu = sync.Mutex{}
	pb.majoritymu = sync.RWMutex{}
	pb.executemu = sync.Mutex{}
	pb.consensustxmu = sync.Mutex{}
	pb.measuremu = sync.RWMutex{}

	pb.transactionvoteQuorum = quorum.NewTransactionVoteQuorum(config.GetConfig().CommitteeNumber)
	pb.transactioncommitQuorum = quorum.NewTransactionCommitQuorum(config.GetConfig().CommitteeNumber)
	pb.decidetransactionvoteQuorum = quorum.NewDecideTransactionVoteQuorum(config.GetConfig().CommitteeNumber)
	pb.decidetransactioncommitQuorum = quorum.NewDecideTransactionCommitQuorum(config.GetConfig().CommitteeNumber)
	pb.committransactionvoteQuorum = quorum.NewCommitTransactionVoteQuorum(config.GetConfig().CommitteeNumber)
	pb.committransactioncommitQuorum = quorum.NewCommitTransactionCommitQuorum(config.GetConfig().CommitteeNumber)
	pb.localtransactionvoteQuorum = quorum.NewLocalTransactionVoteQuorum(config.GetConfig().CommitteeNumber)
	pb.localtransactioncommitQuorum = quorum.NewLocalTransactionCommitQuorum(config.GetConfig().CommitteeNumber)

	pb.totalConsensusTx = 0

	pb.pbftMeasure = measure.NewPBFTMeasure()
	return pb
}

func (pb *PBFT) AddConsensusTx(txhash common.Hash) {
	pb.consensustxmu.Lock()
	defer pb.consensustxmu.Unlock()

	pb.totalConsensusTx++
	log.StateInfof("[Shard %v] (AddConsensusTx) totalConsensusTx: %v, Hash: %v", pb.Shard(), pb.totalConsensusTx, txhash)
}

func (pb *PBFT) SubConsensusTx(txhash common.Hash) {
	pb.consensustxmu.Lock()
	defer pb.consensustxmu.Unlock()

	pb.totalConsensusTx--
	log.StateInfof("[Shard %v] (SubConsensusTx) totalConsensusTx: %v, Hash: %v", pb.Shard(), pb.totalConsensusTx, txhash)
}

func (pb *PBFT) IsExist(address common.Address) bool {
	var addressList []common.Address
	addressList = append(addressList, address)
	if utils.CalculateShardToSend(addressList)[0] == pb.Shard() {
		return true
	} else {
		return false
	}
}

func (pb *PBFT) WaitTransactionAddLock(waiting_transaction *message.Transaction, isReasonBlock bool) {
	if !isReasonBlock {
		log.LockDebugf("[%v %v] (WaitTransactionAddLock) Start paperexperiment Protocol in wait transaction Hash: %v, Nonce: %v, IsCross: %v", pb.ID(), pb.Shard(), waiting_transaction.Hash, waiting_transaction.Nonce+1, waiting_transaction.IsCrossShardTx)
		waiting_transaction.Nonce++
	}
	pb.SendToCommunicator(waiting_transaction)
}

func (pb *PBFT) CreateShardBlock() *blockchain.ShardBlock {
	blockdata := pb.committed_transactions.GeneratePayload(pb.pt)
	log.Debugf("[Shard %v] (CreateShardBlock) Start CreateBlock BlockHeight: %v Transactions: %v", pb.Shard(), pb.GetLastBlockHeight()+1, len(blockdata))

	for _, committedTransaction := range blockdata {
		if committedTransaction.IsCrossShardTx {
			committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime
			committedTransaction.LatencyDissection.BlockWaitingTime = committedTransaction.LatencyDissection.BlockWaitingTime + committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime
			committedTransaction.LatencyDissection.CommitToBlockTime = time.Now().UnixMilli()
		} else {
			committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime
			committedTransaction.LocalLatencyDissection.BlockWaitingTime = committedTransaction.LocalLatencyDissection.BlockWaitingTime + committedTransaction.AbortAndRetryLatencyDissection.BlockWaitingTime
			committedTransaction.LocalLatencyDissection.CommitToBlockTime = time.Now().UnixMilli()
		}
	}

	if pb.pbftMeasure.StartTime.IsZero() && len(blockdata) > 0 {
		pb.pbftMeasure.StartTime = time.Now()
	}

	log.Debugf("[%v %v] (CreateShardBlock)0 blockheight %v stateHash %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, pb.bc.GetCurrentStateHash())
	pb.executemu.Lock()
	log.Debugf("[%v %v] (CreateShardBlock)1 blockheight %v stateHash %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, pb.bc.GetCurrentStateHash())
	root, err := pb.bc.GetStateDB().Commit(0, true, 0)
	log.Debugf("[%v %v] (CreateShardBlock)2 blockheight %v stateHash %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, pb.bc.GetCurrentStateHash())
	pb.bc.SetStateDB(root)
	log.Debugf("[%v %v] (CreateShardBlock)3 blockheight %v stateHash %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, pb.bc.GetCurrentStateHash())
	pb.executemu.Unlock()
	if err != nil {
		log.Errorf("[%v %v] (ProcessShardBlock) BlockHeight: %v Commit State Error: %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, err)
	} else {
		log.Debugf("[%v %v] (ProcessShardBlock) Commit Root Hash: %v", pb.ID(), pb.Shard(), root)
	}

	// wait qc from updatedQC channel
	var qc *quorum.QC
	if pb.highQC.View == 0 {
		qc = pb.highQC
	} else if pb.highQC.BlockHeight == pb.lastCreatedBlockQC.BlockHeight {
		qc = <-pb.updatedQC
	} else {
		qc = pb.highQC
		<-pb.updatedQC
	}

	block := blockchain.CreateShardBlock(blockdata, pb.pm.GetCurEpoch(), pb.pm.GetCurView(), pb.bc.GetCurrentStateHash(), qc.BlockID, pb.GetHighBlockHeight(), qc, pb.Shard())
	pb.lastCreatedBlockQC = block.QC

	log.Debugf("[%v %v] (CreateShardBlock) finished making a proposal for blockheight %v view %v epoch %v Hash %v", pb.ID(), pb.Shard(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	log.StateInfof("[Shard %v] (CreateShardBlock) Start ProcessTransaction BlockHeight: %v", pb.Shard(), block.Block_header.Block_height)
	return block
}

/* Consensus Process */
func (pb *PBFT) ProcessShardBlock(block *blockchain.ShardBlock) error {
	log.Debugf("[%v %v] (ProcessShardBlock) processing block from leader %v blockHeight %v view %v epoch %v Hash %v", pb.ID(), pb.Shard(), block.Proposer, block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	curBlockHeight := pb.GetHighBlockHeight()

	if block.Block_header.View_num > pb.pm.GetCurView() {
		utils.AddEntity(pb.bufferedBlocks, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height-1, block)
		log.Debugf("[%v %v] (ProcessShardBlock) the block is buffered for large epoch %v ID %v ", pb.ID(), pb.Shard(), block.Block_header.View_num, block.Block_hash)
		return nil
	}

	// process block verify
	err := pb.processCertificateBlock(block)
	if err != nil {
		log.Errorf("[%v %v] (ProcessShardBlock) failed certificate block: %v, Hash %v", pb.ID(), pb.Shard(), err, block.Block_hash)
		return err
	}

	// For block order
	if block.Block_header.Block_height > curBlockHeight+1 {
		// buffer the block for which it can be processed after processing the block of block height - 1
		utils.AddEntity(pb.bufferedBlocks, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height-1, block)
		// pb.bufferedBlocks[types.Epoch(block.Block_header.Block_height)-1] = block
		log.Debugf("[%v %v] (ProcessShardBlock) the block is buffered for large blockheight %v ID %v", pb.ID(), pb.Shard(), block.Block_header.Block_height, block.Block_hash)
		return nil
	} else if pb.State() == types.VIEWCHANGING {
		utils.AddEntity(pb.bufferedBlocks, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height, block)
		// pb.bufferedBlocks[types.Epoch(block.Block_header.Block_height)] = block
		log.Debugf("[%v %v] (ProcessShardBlock) the block is buffered for viewchange ID %v", pb.ID(), pb.Shard(), block.Block_hash)
		return nil
	}

	if !pb.IsLeader(pb.ID(), block.Block_header.View_num, block.Block_header.Epoch_num) {
		pb.executemu.Lock()
		root, err := pb.bc.GetStateDB().Commit(0, true, 0)
		pb.bc.SetStateDB(root)
		pb.executemu.Unlock()
		if err != nil {
			log.Errorf("[%v %v] (ProcessShardBlock) BlockHeight: %v Commit State Error: %v", pb.ID(), pb.Shard(), pb.GetLastBlockHeight()+1, err)
		} else {
			log.PerformanceInfof("[%v %v] (ProcessShardBlock) Commit Root Hash: %v", pb.ID(), pb.Shard(), root)
		}
	}

	// after verify block, set prepared for node to start consensus
	pb.SetState(types.PREPARED)

	utils.AddEntity(pb.agreeingBlocks, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height, block)

	// process buffered QC
	qc, ok := pb.bufferedQCs[block.Block_hash]
	if ok {
		pb.processCertificateVote(qc)
		delete(pb.bufferedQCs, block.Block_hash)
		vote := quorum.MakeVote(block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height, pb.ID(), block.Block_hash)
		pb.BroadcastToSome(pb.FindCommitteesFor(block.Block_header.Epoch_num), vote)
		return nil
	}

	shouldVote, err := pb.votingRule(block)
	if err != nil {
		log.Errorf("[%v %v] (ProcessShardBlock) cannot decide whether to vote the block: %v", pb.ID(), pb.Shard(), err)
		return err
	}
	if !shouldVote {
		log.Warningf("[%v %v] (ProcessShardBlock) is not going to vote for block ID %v", pb.ID(), pb.Shard(), block.Block_hash)
		return nil
	}

	log.Debugf("[%v %v] (ProcessShardBlock) finished processing block from leader %v blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), block.Proposer, block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	vote := quorum.MakeVote(block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height, pb.ID(), block.Block_hash)

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(block.Block_header.Epoch_num), vote)
	pb.ProcessVote(vote)

	return nil
}

func (pb *PBFT) ProcessVote(vote *quorum.Vote) {
	log.Debugf("[%v %v] (ProcessVote) processing vote for blockHeight %v view %v epoch %v ID %v from %v", pb.ID(), pb.Shard(), vote.BlockHeight, vote.View, vote.Epoch, vote.BlockID, vote.Voter)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.BlockID))
	if err != nil {
		log.Errorf("[%v %v] (ProcessVote) error in verifying the signature in vote ID %v", pb.ID(), pb.Shard(), vote.BlockID)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessVote) received a vote with invalid signature ID %v", pb.ID(), pb.Shard(), vote.BlockID)
		return
	}

	isBuilt, qc := pb.voteQuorum.Add(vote)
	if !isBuilt {
		log.Debugf("[%v %v] (ProcessVote) votes are not sufficient to build a qc ID %v", pb.ID(), pb.Shard(), vote.BlockID)
		return
	}

	qc.Leader = pb.ID()

	// buffer the QC if the block has not been received
	_, exist := pb.agreeingBlocks[qc.Epoch][qc.View][qc.BlockHeight]
	if !exist {
		pb.bufferedQCs[qc.BlockID] = qc
		log.Debugf("[%v %v] (ProcessVote) the qc is buffered for not arrived block ID %v", pb.ID(), pb.Shard(), qc.BlockID)
		return
	}

	pb.processCertificateVote(qc)

	log.Debugf("[%v %v] (ProcessVote) finished processing vote for blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), vote.BlockHeight, vote.View, vote.Epoch, vote.BlockID)
}

func (pb *PBFT) ProcessCommit(commit *quorum.Commit) {
	log.Debugf("[%v %v] (ProcessCommit) processing commit for blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), commit.BlockHeight, commit.View, commit.Epoch, commit.BlockID)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.BlockID))
	if err != nil {
		log.Errorf("[%v %v] (ProcessCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.BlockID)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.BlockID)
		return
	}

	isBuilt, cqc := pb.commitQuorum.Add(commit)
	if !isBuilt {
		log.Debugf("[%v %v] (ProcessCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.BlockID)
		return
	}

	cqc.Leader = pb.ID()
	// buffer the QC if the block has not been received
	// which means qc is not certificated
	_, err = pb.bc.GetBlockByID(cqc.BlockID)
	if err != nil {
		pb.bufferedCQCs[cqc.BlockID] = cqc
		return
	}

	pb.processCertificateCqc(cqc)

	log.Debugf("[%v %v] (ProcessCommit) finished processing commit for blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.BlockHeight, cqc.View, cqc.Epoch, cqc.BlockID)
}

func (pb *PBFT) ProcessAccept(accept *blockchain.Accept) {
	if pb.IsCommittee(pb.ID(), accept.CommittedBlock.Block_header.Epoch_num) {
		return
	}

	block := accept.CommittedBlock
	log.Debugf("[%v %v] (ProcessAccept) processing accept commit completed block from leader %v blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), block.Proposer.Node(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	curEpoch := pb.pm.GetCurEpoch()
	curBlockHeight := pb.GetHighBlockHeight()

	if block.Block_header.Epoch_num > curEpoch {
		utils.AddEntity(pb.bufferedAccepts, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height-1, accept)
		log.Debugf("[%v %v] (ProcessAccept) the accept is buffered for large epoch ID %v ", pb.ID(), pb.Shard(), block.Block_hash)
		return
	}

	err := pb.processCertificateBlock(block)
	if err != nil {
		log.Errorf("[%v %v] (ProcessAccept) failed certificate block: %v", pb.ID(), pb.Shard(), err)
		return
	}

	if block.Block_header.Block_height > curBlockHeight+1 {
		// buffer the block for which it can be processed after processing the block of block height - 1
		utils.AddEntity(pb.bufferedAccepts, block.Block_header.Epoch_num, block.Block_header.View_num, block.Block_header.Block_height-1, accept)
		// pb.bufferedBlocks[block.BlockHeight-1] = block
		log.Debugf("[%v %v] (ProcessAccept) the accept is buffered for large blockheight ID %v", pb.ID(), pb.Shard(), block.Block_hash)
		return
	}

	pb.bc.AddBlock(block)

	// update QC
	_ = pb.updatePreferredBlockHeight(block.CQC)
	_ = pb.updateLastVotedBlockHeight(block.Block_header.Block_height)
	pb.updateHighQC(block.CQC)

	// Elect Committees, if we should
	if pb.pm.IsTimeToElect() {
		curEpoch := pb.pm.GetCurEpoch()
		committes := pb.ElectCommittees(block.Block_hash, curEpoch+1)
		if committes != nil {
			log.Debugf("[%v %v] (ProcessAccept) elect new committees %v for new epoch %v", pb.ID(), pb.Shard(), committes, curEpoch+1)
		}
	}

	// update view
	pb.pm.AdvanceView(block.Block_header.View_num)
	pb.updateHighCQC(block.CQC)

	// commit Block
	err = pb.commitBlock(block.CQC)
	if err != nil {
		log.Errorf("[%v %v] (ProcessAccept) Failed commit BlockHeight %v: %v", pb.ID(), pb.Shard(), block.CQC.BlockHeight, err)
		return
	}

	log.Debugf("[%v %v] (ProcessAccept) finished processing accept commit completed block from leader %v blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), block.Proposer.Node(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	curEpoch, curView := pb.pm.GetCurEpochView()

	if accept, ok := pb.bufferedAccepts[curEpoch][curView][block.Block_header.Block_height]; ok {
		pb.ProcessAccept(accept)
		delete(pb.bufferedAccepts[curEpoch][curView], block.Block_header.Block_height)
	}

	if !pb.IsCommittee(pb.ID(), curEpoch) {
		return
	}

	if block, exist := pb.bufferedBlocks[curEpoch][curView][block.Block_header.Block_height]; exist {
		_ = pb.ProcessShardBlock(block)
		delete(pb.bufferedBlocks[curEpoch][curView], block.Block_header.Block_height)
	}
}

/* Certificate */
func (pb *PBFT) processCertificateBlock(block *blockchain.ShardBlock) error {
	log.Debugf("[%v %v] (processCertificateBlock) processing certificate block blockHeight: %v view: %v epoch: %v ID: %v", pb.ID(), pb.Shard(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	qc := block.QC
	if qc.BlockHeight < pb.GetHighBlockHeight() {
		return fmt.Errorf("qc is not valid, blockheight %v highblockheight: %v hash: %v", qc.BlockHeight, pb.GetHighBlockHeight(), block.Block_hash)
	}

	if block.QC.Leader != pb.ID() {
		quorumIsVerified, _ := crypto.VerifyQuorumSignature(qc.AggSig, qc.BlockID, qc.Signers)
		if !quorumIsVerified {
			return fmt.Errorf("received a quorum with invalid signatures")
		}
	}

	curEpoch, curView := pb.pm.GetCurEpochView()
	curBlockHeight := pb.GetHighBlockHeight()

	if !pb.Election.IsLeader(block.Proposer, block.Block_header.View_num, block.Block_header.Epoch_num) {
		return fmt.Errorf("received a proposal view %v epoch %v from an invalid leader %v", block.Block_header.View_num, block.Block_header.Epoch_num, block.Proposer)
	}

	if block.Block_header.Epoch_num < curEpoch || block.Block_header.View_num < curView || block.Block_header.Block_height < curBlockHeight {
		return fmt.Errorf("received a stale proposal from %v view %v %v blockheight %v curBlockHeight %v hash %v", block.Proposer, block.Block_header.View_num, curView, block.Block_header.Block_height, curBlockHeight, block.Block_hash)
	}

	if block.Proposer != pb.ID() {
		blockIsVerified, _ := crypto.PubVerify(block.Committee_sig[0], crypto.IDToByte(block.Block_hash))
		if !blockIsVerified {
			return fmt.Errorf("received a block with an invalid signature")
		}
	}

	if block.QC == nil {
		return fmt.Errorf("the block should contain a QC")
	}

	if pb.IsByz() && config.GetConfig().Strategy == FORK && pb.IsLeader(pb.ID(), qc.View+1, pb.pm.GetCurEpoch()) {
		pb.pm.AdvanceView(qc.View)
		return nil
	}

	log.Debugf("[%v %v] (processCertificateBlock) finished processing certificate block blockHeight: %v view: %v epoch: %v ID: %v", pb.ID(), pb.Shard(), block.Block_header.Block_height, block.Block_header.View_num, block.Block_header.Epoch_num, block.Block_hash)

	return nil
}

func (pb *PBFT) processCertificateVote(qc *quorum.QC) {
	log.Debugf("[%v %v] (processCertificateVote) processing quorum certification blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), qc.BlockHeight, qc.View, qc.Epoch, qc.BlockID)

	if qc.BlockHeight < pb.GetHighBlockHeight() {
		return
	}

	block, exist := pb.agreeingBlocks[qc.Epoch][qc.View][qc.BlockHeight]
	if !exist {
		return
	}

	pb.bc.AddBlock(block)
	delete(pb.agreeingBlocks[qc.Epoch][qc.View], qc.BlockHeight)

	if pb.IsByz() && config.GetConfig().Strategy == FORK && pb.IsLeader(pb.ID(), qc.View+1, pb.pm.GetCurEpoch()) {
		pb.pm.AdvanceView(qc.View)
		return
	}

	err := pb.updatePreferredBlockHeight(qc)
	if err != nil {
		pb.bufferedQCs[qc.BlockID] = qc
		log.Debugf("[%v %v] (processCertificateVote) a qc is buffered for not ready to certificate ID %v: %v", pb.Shard(), pb.ID(), qc.BlockID, err)
		return
	}

	err = pb.updateLastVotedBlockHeight(qc.BlockHeight)
	if err != nil {
		pb.bufferedQCs[qc.BlockID] = qc
		log.Debugf("[%v %v] (processCertificateVote) a qc is buffered for not ready to certificate ID %v: %v", pb.Shard(), pb.ID(), qc.BlockID, err)
		return
	}

	pb.pm.AdvanceView(qc.View)
	pb.updateHighQC(qc)

	if pb.pm.IsTimeToElect() {
		curEpoch := pb.pm.GetCurEpoch()
		committes := pb.ElectCommittees(block.Block_hash, curEpoch+1)
		if committes != nil {
			log.Debugf("[%v %v] (processCertificateVote) elect new committees %v for new epoch %v", pb.ID(), pb.Shard(), committes, curEpoch+1)
		}
	}

	// log.Errorf("[%v %v] Vote Advance View: %v, Epoch: %v, blockheight: %v", pb.ID(), pb.Shard(), pb.pm.GetCurView(), pb.pm.GetCurEpoch(), pb.GetLastBlockHeight())

	pb.SetState(types.READY)
	log.Debugf("[%v %v] (processCertificateVote) finished processing quorum certification blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), qc.BlockHeight, qc.View, qc.Epoch, qc.BlockID)

	curEpoch, curView := pb.pm.GetCurEpochView()
	if block, ok := pb.bufferedBlocks[curEpoch][curView][qc.BlockHeight]; ok {
		_ = pb.ProcessShardBlock(block)
		delete(pb.bufferedBlocks[curEpoch][curView], qc.BlockHeight)
	}

	if cqc, ok := pb.bufferedCQCs[qc.BlockID]; ok {
		pb.processCertificateCqc(cqc)
		delete(pb.bufferedCQCs, qc.BlockID)
	}

	commit := quorum.MakeCommit(qc.Epoch, qc.View, qc.BlockHeight, pb.ID(), qc.BlockID)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessCommit(commit)
}

func (pb *PBFT) processCertificateCqc(cqc *quorum.QC) {
	log.Debugf("[%v %v] (processCertificateCqc) processing commit quorum certification blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.BlockHeight, cqc.View, cqc.Epoch, cqc.BlockID)

	if cqc.BlockHeight < pb.GetLastBlockHeight() {
		return
	}

	pb.updateHighCQC(cqc)

	curBlock, _ := pb.bc.GetBlockByID(cqc.BlockID)
	curBlock.CQC = cqc

	// If Leader, Accept Message Broadcast To Validator
	if curBlock.Proposer == pb.ID() {
		accept := &blockchain.Accept{CommittedBlock: curBlock, Timestamp: time.Now()}
		validators := pb.FindValidatorsFor(curBlock.Block_header.Epoch_num)
		pb.BroadcastToSome(validators, accept)
	}

	err := pb.commitBlock(cqc)
	if err != nil {
		log.Errorf("[%v %v] (processCertificateCqc) failed commit BlockHeight %v: %v", pb.ID(), pb.Shard(), cqc.BlockHeight, err)
		return
	}

	if curBlock.Proposer == pb.ID() {
		crossShardTransactionLatency := pb.pbftMeasure.CalculateMeasurements(curBlock, pb.Shard())
		pb.SendToCommunicator(&crossShardTransactionLatency)
		pb.SendToCommunicator(curBlock)
		log.Debugf("[%v %v] (processCertificateCqc) Send Block completed consensus To Communicator Committed Transaction: %v", pb.ID(), pb.Shard(), len(curBlock.Transaction))
	}
	log.Debugf("[%v %v] (processCertificateCqc) finished processing commit quorum certification blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.BlockHeight, cqc.View, cqc.Epoch, cqc.BlockID)
}

func (pb *PBFT) commitBlock(cqc *quorum.QC) error {
	log.Debugf("[%v %v] (commitBlock) commiting block blockHeight %v view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.BlockHeight, cqc.View, cqc.Epoch, cqc.BlockID)

	if cqc.BlockHeight < 2 {
		return nil
	}

	ok, block, _ := pb.commitRule(cqc)

	if !ok {
		return fmt.Errorf("cannot commit by rules")
	}
	// Commit Block which has previous block number
	committedBlocks, forkedBlocks, err := pb.bc.CommitBlock(block.Block_hash, block.Block_header.Block_height, []quorum.Quorum{pb.voteQuorum, pb.commitQuorum})
	if err != nil {
		return fmt.Errorf("[%v %v] cannot commit blocks, %v", pb.ID(), pb.Shard(), err)
	}

	for _, cBlock := range committedBlocks {
		pb.committedBlocks <- cBlock
	}
	for _, fBlock := range forkedBlocks {
		pb.forkedBlocks <- fBlock
	}

	log.Debugf("[%v %v] (commitBlock) finished commiting block blockHeight %v view %v epoch %v ID %v leader %v", pb.ID(), pb.Shard(), cqc.BlockHeight, cqc.View, cqc.Epoch, cqc.BlockID, cqc.Leader)
	return nil
}

func (pb *PBFT) votingRule(block *blockchain.ShardBlock) (bool, error) {
	if block.Block_header.Block_height <= 2 {
		return true, nil
	}
	parentBlock, err := pb.bc.GetBlockByID(block.Block_header.Prev_block_hash)
	if err != nil {
		return false, fmt.Errorf("cannot vote for block: %v", err)
	}
	if (block.Block_header.Block_height <= pb.lastVotedBlockHeight) || (parentBlock.Block_header.Block_height < pb.preferredBlockHeight) {
		return false, nil
	}
	return true, nil
}

func (pb *PBFT) commitRule(cqc *quorum.QC) (bool, *blockchain.ShardBlock, error) {
	parentBlock, err := pb.bc.GetParentBlock(cqc.BlockID)
	if err != nil {
		return false, nil, fmt.Errorf("cannot commit any block: %v", err)
	}
	if (parentBlock.Block_header.Block_height + 1) == cqc.BlockHeight {
		return true, parentBlock, nil
	}
	return false, nil, nil
}

/* Timer */
func (pb *PBFT) ProcessRemoteTmo(tmo *pacemaker.TMO) {
	log.Debugf("[%v %v] (ProcessRemoteTmo) processing remote timeout from %v for newview %v", pb.ID(), pb.Shard(), tmo.NodeID, tmo.NewView)

	if pb.State() != types.VIEWCHANGING {
		if !(pb.pm.GetCurView() < tmo.NewView) {
			return
		}

		for id, prevTmo := range pb.detectedTmos {
			if !(pb.pm.GetCurView() < prevTmo.NewView) {
				delete(pb.detectedTmos, id)
			}
		}

		pb.detectedTmos[tmo.NodeID] = tmo

		// check f+1 tmos detected
		if len(pb.detectedTmos) > (config.GetConfig().CommitteeNumber-1)/3 {
			log.Debugf("[%v %v] (ProcessRemoteTmo) Got more than f+1 message Propose for new view %v", pb.ID(), pb.Shard(), tmo.NewView)

			var maxAnchorView types.View
			// add detectedTmos into TMO
			for _, tmo := range pb.detectedTmos {
				pb.pm.ProcessRemoteTmo(tmo)

				if maxAnchorView < tmo.AnchorView {
					maxAnchorView = tmo.AnchorView
				}
			}

			if pb.pm.GetAnchorView() < maxAnchorView {
				pb.pm.UpdateAnchorView(maxAnchorView)
			}

			pb.pm.ExecuteViewChange(pb.pm.GetNewView())

			pb.detectedTmos = make(map[identity.NodeID]*pacemaker.TMO)
		}
		return
	}

	if len(pb.detectedTmos) > 0 {
		pb.detectedTmos = make(map[identity.NodeID]*pacemaker.TMO)
	}

	if tmo.AnchorView > pb.pm.GetAnchorView() {
		pb.pm.UpdateAnchorView(tmo.AnchorView)
		pb.pm.ExecuteViewChange(pb.pm.GetNewView())
		return
	}

	isBuilt, tc, reservedPrepareBlock := pb.pm.ProcessRemoteTmo(tmo) // tmo majority check
	if !isBuilt {
		return
	}

	tc.Epoch = pb.pm.GetCurEpoch()

	if pb.IsLeader(pb.ID(), tc.NewView, tc.Epoch) {
		return
	}

	if reservedPrepareBlock != nil {
		pb.reservedBlock <- reservedPrepareBlock
	}

	log.Debugf("[%v %v] (ProcessRemoteTmo) a tc is built for New View %v", pb.ID(), pb.Shard(), tc.NewView)

	tc.NodeID = pb.ID()

	pb.SetRole(types.LEADER)

	log.Debugf("[%v %v] (ProcessRemoteTmo) finished processing remote timeout from %v for newview %v", pb.ID(), pb.Shard(), tmo.NodeID, tmo.NewView)

	pb.Broadcast(tc)
	pb.ProcessTC(tc)
}

func (pb *PBFT) ProcessLocalTmo(view types.View) {
	log.Debugf("[%v %v] (ProcessLocalTmo) processing local timeout for view %v", pb.ID(), pb.Shard(), view)
	// BlockHeightLast = BlockHeight of which block is lastly appended into blockchain
	// BlockHeightPrepared = BlockHeight of which block is lastly prepared
	// BlockHeightLast is always same with BlockHeightPrepared because block is immediately inserted into blockchain after prepared...
	// If different, BlockHeightPrepared shouldn't be in the chain
	pb.SetState(types.VIEWCHANGING)

	tmo := &pacemaker.TMO{
		Shard:               pb.Shard(),
		Epoch:               pb.pm.GetCurEpoch(),
		View:                view,
		NewView:             pb.pm.GetNewView(),
		AnchorView:          pb.pm.GetAnchorView(),
		NodeID:              pb.ID(),
		BlockHeightLast:     pb.GetHighBlockHeight(),
		BlockHeightPrepared: pb.GetHighBlockHeight(),
		BlockLast:           pb.bc.GetBlockByBlockHeight(pb.GetHighBlockHeight()),
		BlockPrepared:       nil,
		HighQC:              pb.GetHighQC(),
	}

	//
	if block, exist := pb.agreeingBlocks[tmo.Epoch][tmo.View][tmo.BlockHeightLast+1]; exist {
		tmo.BlockHeightPrepared += 1
		tmo.BlockPrepared = block
	}

	log.Debugf("[%v %v] (ProcessLocalTmo) tmo is built for view %v new view %v anchor view %v last blockheight %v prepared blockheight %v", pb.ID(), pb.Shard(), tmo.View, tmo.NewView, tmo.AnchorView, tmo.BlockHeightLast, tmo.BlockHeightPrepared)

	pb.BroadcastToSome(pb.FindCommitteesFor(tmo.Epoch), tmo)
	pb.ProcessRemoteTmo(tmo)

	log.Debugf("[%v %v] (ProcessLocalTmo) finished processing local timeout for view %v", pb.ID(), pb.Shard(), view)
}

func (pb *PBFT) ProcessTC(tc *pacemaker.TC) {
	log.Debugf("[%v %v] (ProcessTC) processing timeout certification for new view %v", pb.ID(), pb.Shard(), tc.NewView)

	pb.SetState(types.READY)

	curEpoch, curView := pb.pm.GetCurEpochView()

	if _, exist := pb.agreeingBlocks[curEpoch][curView][tc.BlockHeightNew]; exist {
		delete(pb.agreeingBlocks[curEpoch][curView], tc.BlockHeightNew)
	}

	pb.pm.UpdateView(tc.NewView - 1)
	pb.pm.UpdateAnchorView(tc.AnchorView)

	log.Debugf("[%v %v] (ProcessTC) finished processing timeout certification for new view %v", pb.ID(), pb.Shard(), tc.NewView)

	curEpoch, curView = pb.pm.GetCurEpochView()

	if block, exist := pb.bufferedBlocks[curEpoch][curView][tc.BlockHeightNew]; exist {
		_ = pb.ProcessShardBlock(block)
		delete(pb.bufferedBlocks[curEpoch][curView], tc.BlockHeightNew)
	}

}

/* Util */
func (pb *PBFT) forkChoice() *quorum.QC {
	var choice *quorum.QC
	if !pb.IsByz() || config.GetConfig().Strategy != FORK {
		return pb.GetHighQC()
	}
	//	create a fork by returning highQC's parent's QC
	parBlockID := pb.GetHighQC().BlockID
	parBlock, err := pb.bc.GetBlockByID(parBlockID)
	if err != nil {
		log.Debugf("cannot get parent block of block ID %v: %v", parBlockID, err)
	}
	if parBlock.QC.BlockHeight < pb.GetHighBlockHeight() {
		choice = pb.GetHighQC()
	} else {
		choice = parBlock.QC
	}
	// to simulate TC's view
	choice.View = pb.pm.GetCurView() - 1
	return choice
}

func (pb *PBFT) GetHighQC() *quorum.QC {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.highQC
}

func (pb *PBFT) GetHighCQC() *quorum.QC {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.highCQC
}

func (pb *PBFT) GetChainStatus() string {
	chainGrowthRate := pb.bc.GetChainGrowth()
	blockIntervals := pb.bc.GetBlockIntervals()
	return fmt.Sprintf("[%v %v] The current view is: %v, chain growth rate is: %v, ave block interval is: %v", pb.ID(), pb.Shard(), pb.pm.GetCurView(), chainGrowthRate, blockIntervals)
}

func (pb *PBFT) GetHighBlockHeight() types.BlockHeight {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.highQC.BlockHeight
}

func (pb *PBFT) GetLastBlockHeight() types.BlockHeight {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.highCQC.BlockHeight
}

func (pb *PBFT) GetLastVotedBlockHeight() types.BlockHeight {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	return pb.lastVotedBlockHeight
}

func (pb *PBFT) updateHighQC(qc *quorum.QC) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if qc.BlockHeight > pb.highQC.BlockHeight {
		pb.highQC = qc
	}
}

func (pb *PBFT) updateHighCQC(cqc *quorum.QC) {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	if cqc.BlockHeight > pb.highCQC.BlockHeight {
		pb.highCQC = cqc
		if pb.IsLeader(pb.ID(), pb.pm.GetCurView(), pb.pm.GetCurEpoch()) {
			pb.updatedQC <- cqc
		}
	}
}

// update block height we have just voted block lastly
func (pb *PBFT) updateLastVotedBlockHeight(targetHeight types.BlockHeight) error {
	if targetHeight < pb.lastVotedBlockHeight {
		return fmt.Errorf("target blockHeight is lower than the last voted blockHeight")
	}
	pb.lastVotedBlockHeight = targetHeight
	return nil
}

// preferred block height is what we trully preferred block height
// which means preferred height confirmed by 1 block ahead
func (pb *PBFT) updatePreferredBlockHeight(qc *quorum.QC) error {
	if qc.BlockHeight <= 1 {
		return nil
	}
	_, err := pb.bc.GetBlockByID(qc.BlockID)
	if err != nil {
		return fmt.Errorf("cannot update preferred blockHeight: %v", err)
	}
	parentBlock, err := pb.bc.GetParentBlock(qc.BlockID)
	if err != nil {
		return fmt.Errorf("cannot update preferred blockHeight: %v", err)
	}
	if parentBlock.Block_header.Block_height > pb.preferredBlockHeight {
		pb.preferredBlockHeight = parentBlock.Block_header.Block_height
	}
	return nil
}
