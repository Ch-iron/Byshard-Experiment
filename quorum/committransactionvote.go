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

type CommitTransactionVoteQuorum struct {
	total int
	votes map[common.Hash]map[int]map[identity.NodeID]*CommitTransactionVote

	mu sync.Mutex
}

func NewCommitTransactionVoteQuorum(n int) *CommitTransactionVoteQuorum {
	return &CommitTransactionVoteQuorum{
		total: n,
		votes: make(map[common.Hash]map[int]map[identity.NodeID]*CommitTransactionVote),
	}
}

type CommitTransactionVote struct {
	types.Epoch
	types.View
	Voter           identity.NodeID
	TransactionHash common.Hash
	crypto.Signature
	IsCommit   bool
	Nonce      int
	OrderCount int
}

func MakeCommitTransactionVote(epoch types.Epoch, view types.View, voter identity.NodeID, hash common.Hash, iscommit bool, nonce int) *CommitTransactionVote {
	sig, err := crypto.PrivSign(crypto.IDToByte(hash), nil)
	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}
	return &CommitTransactionVote{
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
func (q *CommitTransactionVoteQuorum) Add(message interface{}) (bool, *CommitTransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*CommitTransactionVote)
	if !ok {
		return false, nil
	}
	if q.superMajority(vote.TransactionHash, vote.Nonce) {
		// delete(q.votes, vote.TransactionHash)
		return false, nil
	}
	if _, exist := q.votes[vote.TransactionHash]; !exist {
		q.votes[vote.TransactionHash] = make(map[int]map[identity.NodeID]*CommitTransactionVote)
	}
	if _, exist := q.votes[vote.TransactionHash][vote.Nonce]; !exist {
		q.votes[vote.TransactionHash][vote.Nonce] = make(map[identity.NodeID]*CommitTransactionVote)
	}
	q.votes[vote.TransactionHash][vote.Nonce][vote.Voter] = vote
	if q.superMajority(vote.TransactionHash, vote.Nonce) {
		aggSig, signers, err := q.getSigs(vote.TransactionHash, vote.Nonce)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc view %v epoch %v transaction hash %x: %v", vote.View, vote.Epoch, vote.TransactionHash, err)
		}
		qc := &CommitTransactionQC{
			Epoch:           vote.Epoch,
			View:            vote.View,
			TransactionHash: vote.TransactionHash,
			AggSig:          aggSig,
			Signers:         signers,
			IsCommit:        vote.IsCommit,
			Nonce:           vote.Nonce,
			OrderCount:      vote.OrderCount,
		}
		return true, qc
	}
	return false, nil
}

// Super majority quorum satisfied
func (q *CommitTransactionVoteQuorum) superMajority(transactionhash common.Hash, nonce int) bool {
	return q.size(transactionhash, nonce) > q.total*2/3
}

// Add adds id to quorum ack records
func (q *CommitTransactionVoteQuorum) AddMajority(message interface{}) (bool, *CommitTransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*CommitTransactionVote)
	if !ok {
		return false, nil
	}
	if q.Majority(vote.TransactionHash, vote.Nonce) {
		return false, nil
	}
	if _, exist := q.votes[vote.TransactionHash]; !exist {
		//	first time of receiving the vote for this block
		if _, exist := q.votes[vote.TransactionHash][vote.Nonce]; !exist {
			q.votes[vote.TransactionHash][vote.Nonce] = make(map[identity.NodeID]*CommitTransactionVote)
		}
	}
	q.votes[vote.TransactionHash][vote.Nonce][vote.Voter] = vote
	if q.Majority(vote.TransactionHash, vote.Nonce) {
		aggSig, signers, err := q.getSigs(vote.TransactionHash, vote.Nonce)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc view %v epoch %v block id %x: %v", vote.View, vote.Epoch, vote.TransactionHash, err)
		}
		qc := &CommitTransactionQC{
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
func (q *CommitTransactionVoteQuorum) Majority(transactionhash common.Hash, nonce int) bool {
	// log.Warning(q.total)
	return q.size(transactionhash, nonce) > q.total*1/2
}

// Size returns ack size for the block
func (q *CommitTransactionVoteQuorum) size(transactionhash common.Hash, nonce int) int {
	return len(q.votes[transactionhash][nonce])
}

func (q *CommitTransactionVoteQuorum) getSigs(transactionhash common.Hash, nonce int) (crypto.AggSig, []identity.NodeID, error) {
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

func (q *CommitTransactionVoteQuorum) Delete(transactionhash common.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.votes[transactionhash]
	if ok {
		delete(q.votes, transactionhash)
	}
}
