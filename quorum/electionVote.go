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

type ElectionVote struct {
	CurView types.View
	Voter   identity.NodeID
	Sig     crypto.Signature
}

type ElectionVoteQuorum struct {
	total int
	votes map[types.View]map[identity.NodeID]*ElectionVote
	mu    sync.Mutex
}

func MakeElectionVote(view types.View, voter identity.NodeID, id common.Hash) *ElectionVote {
	sig, err := crypto.PrivSign(crypto.IDToByte(id), nil)

	if err != nil {
		log.Fatalf("[%v] has an error when signing a vote", voter)
		return nil
	}

	return &ElectionVote{
		CurView: view,
		Voter:   voter,
		Sig:     sig,
	}
}

func NewElectionVoteQuorum(n int) *ElectionVoteQuorum {
	return &ElectionVoteQuorum{
		total: n,
		votes: make(map[types.View]map[identity.NodeID]*ElectionVote),
	}
}

func (e *ElectionVoteQuorum) Add(message interface{}) (bool, *QC) {
	e.mu.Lock()
	defer e.mu.Unlock()

	electVote, ok := message.(*ElectionVote)

	if !ok {
		return false, nil
	}

	if e.majority(electVote.CurView) {
		return false, nil
	}

	_, exists := e.votes[electVote.CurView]

	if !exists {
		e.votes[electVote.CurView] = make(map[identity.NodeID]*ElectionVote)
	}

	e.votes[electVote.CurView][electVote.Voter] = electVote

	if e.majority(electVote.CurView) {
		_, _, err := e.getSigs(electVote.CurView)

		if err != nil {
			log.Warningf("(Vote Add) cannot generate a valid vote: %v", err)
		}

		return true, nil
	}

	return false, nil
}

func (e *ElectionVoteQuorum) Delete(blockId common.Hash) {
}

func (e *ElectionVoteQuorum) majority(view types.View) bool {
	return e.size(view) > e.total*1/2
}

func (e *ElectionVoteQuorum) size(view types.View) int {
	return len(e.votes[view])
}

func (e *ElectionVoteQuorum) getSigs(view types.View) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := e.votes[view]

	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, view: %x", view)
	}

	for _, electVote := range e.votes[view] {
		sigs = append(sigs, electVote.Sig)
		signers = append(signers, electVote.Voter)
	}

	return sigs, signers, nil
}

func (e *ElectionVoteQuorum) AddMajority(message interface{}) (bool, *QC) {
	return false, nil
}
