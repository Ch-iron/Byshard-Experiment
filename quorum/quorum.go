package quorum

import (
	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

type Quorum interface {
	Add(message interface{}) (bool, *QC)
	AddMajority(message interface{}) (bool, *QC)
	Delete(blockID common.Hash)
}

type TransactionQuorum interface {
	Add(message interface{}) (bool, *TransactionQC)
	AddMajority(message interface{}) (bool, *TransactionQC)
	Delete(transactionhash common.Hash)
}

type DecideTransactionQuorum interface {
	Add(message interface{}) (bool, *DecideTransactionQC)
	AddMajority(message interface{}) (bool, *DecideTransactionQC)
	Delete(transactionhash common.Hash)
}

type CommitTransactionQuorum interface {
	Add(message interface{}) (bool, *CommitTransactionQC)
	AddMajority(message interface{}) (bool, *CommitTransactionQC)
	Delete(transactionhash common.Hash)
}

type LocalTransactionQuorum interface {
	Add(message interface{}) (bool, *LocalTransactionQC)
	AddMajority(message interface{}) (bool, *LocalTransactionQC)
	Delete(transactionhash common.Hash)
}

type QC struct {
	Leader identity.NodeID
	Epoch  types.Epoch
	View   types.View
	types.BlockHeight
	BlockID common.Hash
	Signers []identity.NodeID
	crypto.AggSig
	crypto.Signature
	types.QcType
}

type TransactionQC struct {
	Leader          identity.NodeID
	Epoch           types.Epoch
	View            types.View
	TransactionHash common.Hash
	Signers         []identity.NodeID
	crypto.AggSig
	crypto.Signature
	types.QcType
	IsCommit   bool
	Nonce      int
	OrderCount int
}

type DecideTransactionQC TransactionQC
type CommitTransactionQC TransactionQC
type LocalTransactionQC TransactionQC
