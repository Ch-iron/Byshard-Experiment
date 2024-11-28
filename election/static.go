package election

import (
	"paperexperiment/identity"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

type Static struct {
	committees  identity.IDs
	committeeNo int
	master      identity.NodeID
	shard       types.Shard
}

func NewStatic(master identity.NodeID, shard types.Shard, committeeNo int) *Static {
	return &Static{
		master:      master,
		shard:       shard,
		committeeNo: committeeNo,
		committees:  []identity.NodeID{},
	}
}

func (st *Static) IsLeader(id identity.NodeID, view types.View, epoch types.Epoch) bool {
	return id == st.master
}

func (st *Static) FindLeaderFor(view types.View, epoch types.Epoch) identity.NodeID {
	return st.master
}

func (st *Static) IsCommittee(id identity.NodeID, epoch types.Epoch) bool {
	for _, ID := range st.committees {
		if ID == id {
			return true
		}
	}
	return false
}

func (st *Static) FindCommitteesFor(epoch types.Epoch) identity.IDs {
	return st.committees
}

func (st *Static) FindValidatorsFor(epoch types.Epoch) identity.IDs {
	return nil
}

func (st *Static) ElectLeader(id identity.NodeID, newView types.View) bool {
	return true
}

func (st *Static) ElectCommittees(blockHash common.Hash, newEpoch types.Epoch) identity.IDs {
	if len(st.committees) == 0 {
		for i := 1; i <= st.committeeNo; i++ {
			st.committees = append(st.committees, identity.NewNodeID(i))
		}
	}
	return st.committees
}

func (st *Static) ReplaceCommittee(epoch types.Epoch, oldPublicKey identity.NodeID, newPublicKey identity.NodeID) identity.IDs {
	return nil
}
