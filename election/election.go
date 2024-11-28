package election

import (
	"paperexperiment/identity"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

type Election interface {
	IsLeader(id identity.NodeID, view types.View, epoch types.Epoch) bool
	FindLeaderFor(view types.View, epoch types.Epoch) identity.NodeID
	IsCommittee(id identity.NodeID, epoch types.Epoch) bool
	FindCommitteesFor(epoch types.Epoch) identity.IDs
	FindValidatorsFor(epoch types.Epoch) identity.IDs
	ElectLeader(id identity.NodeID, newView types.View) bool
	ElectCommittees(blockHash common.Hash, newEpoch types.Epoch) identity.IDs
	ReplaceCommittee(epoch types.Epoch, oldPublicKey identity.NodeID, newPublicKey identity.NodeID) identity.IDs
}
