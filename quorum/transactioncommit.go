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

type TransactionCommitQuorum struct {
	total   int
	commits map[common.Hash]map[int]map[identity.NodeID]*TransactionCommit

	mu sync.Mutex
}

func NewTransactionCommitQuorum(n int) *TransactionCommitQuorum {
	return &TransactionCommitQuorum{
		total:   n,
		commits: make(map[common.Hash]map[int]map[identity.NodeID]*TransactionCommit),
	}
}

type TransactionCommit struct {
	types.Epoch
	types.View
	Voter           identity.NodeID
	TransactionHash common.Hash
	crypto.Signature
	IsCommit   bool
	Nonce      int
	OrderCount int
}

func MakeTransactionCommit(epoch types.Epoch, view types.View, voter identity.NodeID, hash common.Hash, iscommit bool, nonce int, ordercount int) *TransactionCommit {
	sig, err := crypto.PrivSign(crypto.IDToByte(hash), nil)
	if err != nil {
		log.Fatalf("[%v] 커밋을 위한 사인에 에러가 있음", voter)
		return nil
	}
	return &TransactionCommit{
		Epoch:           epoch,
		View:            view,
		Voter:           voter,
		TransactionHash: hash,
		Signature:       sig,
		IsCommit:        iscommit,
		Nonce:           nonce,
		OrderCount:      ordercount,
	}
}

func (q *TransactionCommitQuorum) Add(message interface{}) (bool, *TransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	commit, ok := message.(*TransactionCommit)
	if !ok {
		return false, nil
	}
	if q.superMajority(commit.TransactionHash, commit.Nonce) {
		return false, nil
	}
	_, exist := q.commits[commit.TransactionHash]
	// log.Debugf("%v", exist)
	if !exist {
		//	first time of receiving the vote for this block
		q.commits[commit.TransactionHash] = make(map[int]map[identity.NodeID]*TransactionCommit)
	}
	if _, exist := q.commits[commit.TransactionHash][commit.Nonce]; !exist {
		q.commits[commit.TransactionHash][commit.Nonce] = make(map[identity.NodeID]*TransactionCommit)
	}
	q.commits[commit.TransactionHash][commit.Nonce][commit.Voter] = commit

	if q.superMajority(commit.TransactionHash, commit.Nonce) {
		aggSig, signers, err := q.getSigs(commit.TransactionHash, commit.Nonce)
		if err != nil {
			log.Warningf("(Commit-Add) cannot generate a valid qc view %v epoch %v block id %x: %v", commit.View, commit.Epoch, commit.TransactionHash, err)
		}
		qc := &TransactionQC{
			Epoch:           commit.Epoch,
			View:            commit.View,
			TransactionHash: commit.TransactionHash,
			AggSig:          aggSig,
			Signers:         signers,
			IsCommit:        commit.IsCommit,
			Nonce:           commit.Nonce,
			OrderCount:      commit.OrderCount,
		}
		return true, qc
	}
	return false, nil
}

func (q *TransactionCommitQuorum) superMajority(blockID common.Hash, nonce int) bool {
	return q.size(blockID, nonce) > q.total*2/3
}

func (q *TransactionCommitQuorum) AddMajority(message interface{}) (bool, *TransactionQC) {
	q.mu.Lock()
	defer q.mu.Unlock()
	commit, ok := message.(*TransactionCommit)
	if !ok {
		return false, nil
	}
	if q.Majority(commit.TransactionHash, commit.Nonce) {
		return false, nil
	}
	_, exist := q.commits[commit.TransactionHash]
	// log.Debugf("%v", exist)
	if !exist {
		q.commits[commit.TransactionHash] = make(map[int]map[identity.NodeID]*TransactionCommit)
	}
	if _, exist := q.commits[commit.TransactionHash][commit.Nonce]; !exist {
		q.commits[commit.TransactionHash][commit.Nonce] = make(map[identity.NodeID]*TransactionCommit)
	}
	q.commits[commit.TransactionHash][commit.Nonce][commit.Voter] = commit

	if q.Majority(commit.TransactionHash, commit.Nonce) {
		aggSig, signers, err := q.getSigs(commit.TransactionHash, commit.Nonce)
		if err != nil {
			log.Warningf("(Commit-Add) cannot generate a valid qc view %v epoch %v block id %x: %v", commit.View, commit.Epoch, commit.TransactionHash, err)
		}
		qc := &TransactionQC{
			Epoch:           commit.Epoch,
			View:            commit.View,
			TransactionHash: commit.TransactionHash,
			AggSig:          aggSig,
			Signers:         signers,
		}
		return true, qc
	}
	return false, nil
}

func (q *TransactionCommitQuorum) Majority(transactionhash common.Hash, nonce int) bool {
	return q.size(transactionhash, nonce) > q.total*1/2
}

func (q *TransactionCommitQuorum) size(transactionhash common.Hash, nonce int) int {
	return len(q.commits[transactionhash][nonce])
}

func (q *TransactionCommitQuorum) getSigs(transactionhash common.Hash, nonce int) (crypto.AggSig, []identity.NodeID, error) {
	var sigs crypto.AggSig
	var signers []identity.NodeID
	_, exists := q.commits[transactionhash]
	if !exists {
		return nil, nil, fmt.Errorf("sigs does not exist, id: %x", transactionhash)
	}
	for _, commit := range q.commits[transactionhash][nonce] {
		sigs = append(sigs, commit.Signature)
		signers = append(signers, commit.Voter)
	}

	return sigs, signers, nil
}

func (q *TransactionCommitQuorum) Delete(transactionhash common.Hash) {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, ok := q.commits[transactionhash]
	if ok {
		delete(q.commits, transactionhash)
	}
}
