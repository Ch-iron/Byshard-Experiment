package quorum

import (
	"fmt"
	"sync"

	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

type LocalTransactionVoteQuorum struct {
	total       int
	votes       map[common.Hash]map[int]map[identity.NodeID]*LocalTransactionVote
	decidevotes map[common.Hash]map[int]map[bool]int

	mu sync.Mutex
}

func NewLocalTransactionVoteQuorum(n int) *LocalTransactionVoteQuorum {
	return &LocalTransactionVoteQuorum{
		total:       n,
		votes:       make(map[common.Hash]map[int]map[identity.NodeID]*LocalTransactionVote),
		decidevotes: make(map[common.Hash]map[int]map[bool]int),
	}
}

type LocalTransactionVote struct {
	types.Epoch
	types.View
	Voter           identity.NodeID
	TransactionHash common.Hash
	crypto.Signature
	IsCommit   bool
	Nonce      int
	OrderCount int
}

func MakeLocalTransactionVote(epoch types.Epoch, view types.View, voter identity.NodeID, hash common.Hash, iscommit bool, nonce int) *LocalTransactionVote {
	sig, err := crypto.PrivSign(crypto.IDToByte(hash), nil)
	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}
	return &LocalTransactionVote{
		Epoch:           epoch,
		View:            view,
		Voter:           voter,
		TransactionHash: hash,
		Signature:       sig,
		IsCommit:        iscommit,
		Nonce:           nonce,
	}
}

// Add adds id to quorum ack records
func (q *LocalTransactionVoteQuorum) Add(message interface{}) (bool, *LocalTransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*LocalTransactionVote)
	if !ok {
		return false, nil
	}
	if result, isCommit := q.superMajority(vote.TransactionHash, vote.Nonce); result {
		return false, nil
	} else {
		if isCommit {
			aggSig, signers, err := q.getSigs(vote.TransactionHash, vote.Nonce)
			if err != nil {
				log.Warningf("(Vote-Add) cannot generate a valid qc view %v epoch %v transaction hash %x: %v", vote.View, vote.Epoch, vote.TransactionHash, err)
			}
			qc := &LocalTransactionQC{
				Epoch:           vote.Epoch,
				View:            vote.View,
				TransactionHash: vote.TransactionHash,
				AggSig:          aggSig,
				Signers:         signers,
				IsCommit:        isCommit,
				Nonce:           vote.Nonce,
				OrderCount:      vote.OrderCount,
			}
			return true, qc
		}
	}
	if _, exist := q.votes[vote.TransactionHash]; !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.TransactionHash] = make(map[int]map[identity.NodeID]*LocalTransactionVote)
	}
	if _, exist := q.votes[vote.TransactionHash][vote.Nonce]; !exist {
		q.votes[vote.TransactionHash][vote.Nonce] = make(map[identity.NodeID]*LocalTransactionVote)
	}
	if _, exist := q.decidevotes[vote.TransactionHash]; !exist {
		q.decidevotes[vote.TransactionHash] = make(map[int]map[bool]int)
	}
	if _, exist := q.decidevotes[vote.TransactionHash][vote.Nonce]; !exist {
		q.decidevotes[vote.TransactionHash][vote.Nonce] = make(map[bool]int)
	}
	if vote.IsCommit {
		q.decidevotes[vote.TransactionHash][vote.Nonce][true]++
	} else {
		q.decidevotes[vote.TransactionHash][vote.Nonce][false]++
	}
	q.votes[vote.TransactionHash][vote.Nonce][vote.Voter] = vote
	if result, isCommit := q.superMajority(vote.TransactionHash, vote.Nonce); result {
		aggSig, signers, err := q.getSigs(vote.TransactionHash, vote.Nonce)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc view %v epoch %v transaction hash %x: %v", vote.View, vote.Epoch, vote.TransactionHash, err)
		}
		qc := &LocalTransactionQC{
			Epoch:           vote.Epoch,
			View:            vote.View,
			TransactionHash: vote.TransactionHash,
			AggSig:          aggSig,
			Signers:         signers,
			IsCommit:        isCommit,
			Nonce:           vote.Nonce,
		}
		return true, qc
	}
	return false, nil
}

// Super majority quorum satisfied
func (q *LocalTransactionVoteQuorum) superMajority(transactionhash common.Hash, nonce int) (bool, bool) {
	if q.decidevotes[transactionhash][nonce][true] > q.total*2/3 {
		return true, true
	} else if q.decidevotes[transactionhash][nonce][false] > q.total*2/3 {
		return true, false
	} else if q.decidevotes[transactionhash][nonce][true]+q.decidevotes[transactionhash][nonce][false] == q.total {
		return true, false
	} else {
		return false, false
	}
}

// Add adds id to quorum ack records
func (q *LocalTransactionVoteQuorum) AddMajority(message interface{}) (bool, *LocalTransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*LocalTransactionVote)
	if !ok {
		return false, nil
	}
	if q.Majority(vote.TransactionHash) {
		return false, nil
	}
	if _, exist := q.votes[vote.TransactionHash]; !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.TransactionHash] = make(map[int]map[identity.NodeID]*LocalTransactionVote)
		if _, exist := q.votes[vote.TransactionHash][vote.Nonce]; !exist {
			q.votes[vote.TransactionHash][vote.Nonce] = make(map[identity.NodeID]*LocalTransactionVote)
		}
	}
	q.votes[vote.TransactionHash][vote.Nonce][vote.Voter] = vote
	if q.Majority(vote.TransactionHash) {
		aggSig, signers, err := q.getSigs(vote.TransactionHash, vote.Nonce)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc view %v epoch %v block id %x: %v", vote.View, vote.Epoch, vote.TransactionHash, err)
		}
		qc := &LocalTransactionQC{
			Epoch:           vote.Epoch,
			View:            vote.View,
			TransactionHash: vote.TransactionHash,
			AggSig:          aggSig,
			Signers:         signers,
		}
		return true, qc
	}
	return false, nil
}

// Super majority quorum satisfied
func (q *LocalTransactionVoteQuorum) Majority(transactionhash common.Hash) bool {
	// log.Warning(q.total)
	return q.size(transactionhash) > q.total*1/2
}

// Size returns ack size for the block
func (q *LocalTransactionVoteQuorum) size(transactionhash common.Hash) int {
	return len(q.votes[transactionhash])
}

func (q *LocalTransactionVoteQuorum) getSigs(transactionhash common.Hash, nonce int) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := q.votes[transactionhash]
	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, id: %x", transactionhash)
	}
	for _, vote := range q.votes[transactionhash][nonce] {
		sigs = append(sigs, vote.Signature)
		signers = append(signers, vote.Voter)
	}

	return sigs, signers, nil
}

func (q *LocalTransactionVoteQuorum) Delete(transactionhash common.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.votes[transactionhash]
	if ok {
		delete(q.votes, transactionhash)
	}
}
