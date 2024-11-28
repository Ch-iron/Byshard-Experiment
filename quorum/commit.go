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

type CommitQuorum struct {
	total   int
	commits map[common.Hash]map[identity.NodeID]*Commit

	mu sync.Mutex
}

func NewCommitQuorum(n int) *CommitQuorum {
	return &CommitQuorum{
		total:   n,
		commits: make(map[common.Hash]map[identity.NodeID]*Commit),
	}
}

type Commit struct {
	types.Epoch
	types.View
	types.BlockHeight
	Voter   identity.NodeID
	BlockID common.Hash
	crypto.Signature
}

func MakeCommit(epoch types.Epoch, view types.View, blockHeight types.BlockHeight, voter identity.NodeID, id common.Hash) *Commit {
	sig, err := crypto.PrivSign(crypto.IDToByte(id), nil)
	if err != nil {
		log.Fatalf("[%v] 커밋을 위한 사인에 에러가 있음", voter)
		return nil
	}
	return &Commit{
		Epoch:       epoch,
		View:        view,
		BlockHeight: blockHeight,
		Voter:       voter,
		BlockID:     id,
		Signature:   sig,
	}
}

func (q *CommitQuorum) Add(message interface{}) (bool, *QC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	commit, ok := message.(*Commit)
	if !ok {
		return false, nil
	}
	if q.superMajority(commit.BlockID) {
		return false, nil
	}
	_, exist := q.commits[commit.BlockID]
	// log.Debugf("%v", exist)
	if !exist {
		//	first time of receiving the vote for this block
		q.commits[commit.BlockID] = make(map[identity.NodeID]*Commit)
	}
	q.commits[commit.BlockID][commit.Voter] = commit

	if q.superMajority(commit.BlockID) {
		aggSig, signers, err := q.getSigs(commit.BlockID)
		if err != nil {
			log.Warningf("(Commit-Add) cannot generate a valid qc blockheight %v view %v epoch %v block id %x: %v", commit.BlockHeight, commit.View, commit.Epoch, commit.BlockID, err)
		}
		qc := &QC{
			Epoch:       commit.Epoch,
			View:        commit.View,
			BlockHeight: commit.BlockHeight,
			BlockID:     commit.BlockID,
			AggSig:      aggSig,
			Signers:     signers,
		}
		return true, qc
	}
	return false, nil
}

func (q *CommitQuorum) superMajority(blockID common.Hash) bool {
	return q.size(blockID) > q.total*2/3
}

func (q *CommitQuorum) AddMajority(message interface{}) (bool, *QC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	commit, ok := message.(*Commit)
	if !ok {
		return false, nil
	}
	if q.Majority(commit.BlockID) {
		return false, nil
	}
	_, exist := q.commits[commit.BlockID]
	// log.Debugf("%v", exist)
	if !exist {
		//	first time of receiving the vote for this block
		q.commits[commit.BlockID] = make(map[identity.NodeID]*Commit)
	}
	q.commits[commit.BlockID][commit.Voter] = commit

	if q.Majority(commit.BlockID) {
		aggSig, signers, err := q.getSigs(commit.BlockID)
		if err != nil {
			log.Warningf("(Commit-Add) cannot generate a valid qc blockheight %v view %v epoch %v block id %x: %v", commit.BlockHeight, commit.View, commit.Epoch, commit.BlockID, err)
		}
		qc := &QC{
			Epoch:       commit.Epoch,
			View:        commit.View,
			BlockHeight: commit.BlockHeight,
			BlockID:     commit.BlockID,
			AggSig:      aggSig,
			Signers:     signers,
		}
		return true, qc
	}
	return false, nil
}

func (q *CommitQuorum) Majority(blockID common.Hash) bool {
	return q.size(blockID) > q.total*1/2
}

func (q *CommitQuorum) size(blockID common.Hash) int {
	return len(q.commits[blockID])
}

func (q *CommitQuorum) getSigs(blockID common.Hash) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := q.commits[blockID]
	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, id: %x", blockID)
	}
	for _, commit := range q.commits[blockID] {
		sigs = append(sigs, commit.Signature)
		signers = append(signers, commit.Voter)
	}

	return sigs, signers, nil
}

func (q *CommitQuorum) Delete(blockID common.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.commits[blockID]
	if ok {
		delete(q.commits, blockID)
	}
}
