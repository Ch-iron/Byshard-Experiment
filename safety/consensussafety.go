package safety

import (
	"paperexperiment/blockchain"
	"paperexperiment/message"
	"paperexperiment/pacemaker"
	"paperexperiment/quorum"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

type ConsensusSafety interface {
	CreateShardBlock() *blockchain.ShardBlock
	ProcessShardBlock(block *blockchain.ShardBlock) error
	ProcessVote(vote *quorum.Vote)
	ProcessAccept(commit *blockchain.Accept)
	ProcessRemoteTmo(tmo *pacemaker.TMO)
	ProcessLocalTmo(view types.View)
	ProcessTC(tc *pacemaker.TC)
	GetHighBlockHeight() types.BlockHeight
	GetLastBlockHeight() types.BlockHeight
	GetChainStatus() string
	ProcessCommit(commit *quorum.Commit)

	IsRootShard(to common.Address) bool
	InitVoteStep(tx *message.SignedTransactionWithHeader) *message.SignedTransactionWithHeader
	ProcessTransactionVote(vote *quorum.TransactionVote)
	ProcessTransactionCommit(commit *quorum.TransactionCommit)
	ProcessTransactionAccept(accept *message.TransactionAccept)

	InitDecideStep(tx *message.VotedTransactionWithHeader) *message.VotedTransactionWithHeader
	ProcessDecideStep(leaderVotedTx *message.VotedTransactionWithHeader)
	ProcessDecideTransactionVote(vote *quorum.DecideTransactionVote)
	ProcessDecideTransactionCommit(commit *quorum.DecideTransactionCommit)
	ProcessDecideTransactionAccept(accept *message.DecideTransactionAccept)

	InitCommitStep(tx *message.CommittedTransactionWithHeader) *message.CommittedTransactionWithHeader
	ProcessCommitTransactionVote(vote *quorum.CommitTransactionVote)
	ProcessCommitTransactionCommit(commit *quorum.CommitTransactionCommit)
	ProcessCommitTransactionAccept(accept *message.CommitTransactionAccept)

	InitLocalLockStep(tx *message.SignedTransactionWithHeader) *message.SignedTransactionWithHeader
	ProcessLocalTransactionVote(vote *quorum.LocalTransactionVote)
	ProcessLocalTransactionCommit(commit *quorum.LocalTransactionCommit)
	ProcessLocalTransactionAccept(accept *message.TransactionAccept)
}
