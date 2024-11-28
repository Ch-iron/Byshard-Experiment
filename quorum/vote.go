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

type VoteQuorum struct {
	total int
	votes map[common.Hash]map[identity.NodeID]*Vote

	mu sync.Mutex
}

func NewVoteQuorum(n int) *VoteQuorum {
	return &VoteQuorum{
		total: n,
		votes: make(map[common.Hash]map[identity.NodeID]*Vote),
	}
}

type Vote struct {
	types.Epoch
	types.View
	types.BlockHeight
	Voter   identity.NodeID
	BlockID common.Hash
	crypto.Signature
}

func MakeVote(epoch types.Epoch, view types.View, blockHeight types.BlockHeight, voter identity.NodeID, id common.Hash) *Vote {
	sig, err := crypto.PrivSign(crypto.IDToByte(id), nil)
	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}
	return &Vote{
		Epoch:       epoch,
		View:        view,
		BlockHeight: blockHeight,
		Voter:       voter,
		BlockID:     id,
		Signature:   sig,
	}
}

func (q *VoteQuorum) Add(message interface{}) (bool, *QC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*Vote)
	if !ok {
		return false, nil
	}
	if q.superMajority(vote.BlockID) {
		return false, nil
	}
	_, exist := q.votes[vote.BlockID]
	if !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.BlockID] = make(map[identity.NodeID]*Vote)
	}
	q.votes[vote.BlockID][vote.Voter] = vote
	if q.superMajority(vote.BlockID) {
		aggSig, signers, err := q.getSigs(vote.BlockID)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc blockheight %v view %v epoch %v block id %x: %v", vote.BlockHeight, vote.View, vote.Epoch, vote.BlockID, err)
		}
		qc := &QC{
			Epoch:       vote.Epoch,
			View:        vote.View,
			BlockHeight: vote.BlockHeight,
			BlockID:     vote.BlockID,
			AggSig:      aggSig,
			Signers:     signers,
		}
		return true, qc
	}
	return false, nil
}

// Super majority quorum satisfied
func (q *VoteQuorum) superMajority(blockID common.Hash) bool {
	// log.Warning(q.total)
	return q.size(blockID) > q.total*2/3
}

func (q *VoteQuorum) AddMajority(message interface{}) (bool, *QC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	vote, ok := message.(*Vote)
	if !ok {
		return false, nil
	}
	if q.Majority(vote.BlockID) {
		return false, nil
	}
	_, exist := q.votes[vote.BlockID]
	if !exist {
		//	first time of receiving the vote for this block
		q.votes[vote.BlockID] = make(map[identity.NodeID]*Vote)
	}
	q.votes[vote.BlockID][vote.Voter] = vote
	if q.Majority(vote.BlockID) {
		aggSig, signers, err := q.getSigs(vote.BlockID)
		if err != nil {
			log.Warningf("(Vote-Add) cannot generate a valid qc blockheight %v view %v epoch %v block id %x: %v", vote.BlockHeight, vote.View, vote.Epoch, vote.BlockID, err)
		}
		qc := &QC{
			Epoch:       vote.Epoch,
			View:        vote.View,
			BlockHeight: vote.BlockHeight,
			BlockID:     vote.BlockID,
			AggSig:      aggSig,
			Signers:     signers,
		}
		return true, qc
	}
	return false, nil
}

// Super majority quorum satisfied
func (q *VoteQuorum) Majority(blockID common.Hash) bool {
	// log.Warning(q.total)
	return q.size(blockID) > q.total*1/2
}

// Size returns ack size for the block
func (q *VoteQuorum) size(blockID common.Hash) int {
	return len(q.votes[blockID])
}

func (q *VoteQuorum) getSigs(blockID common.Hash) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := q.votes[blockID]
	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, id: %x", blockID)
	}
	for _, vote := range q.votes[blockID] {
		sigs = append(sigs, vote.Signature)
		signers = append(signers, vote.Voter)
	}

	return sigs, signers, nil
}

func (q *VoteQuorum) Delete(blockID common.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.votes[blockID]
	if ok {
		delete(q.votes, blockID)
	}
}
