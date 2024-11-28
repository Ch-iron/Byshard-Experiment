package blockchain

import (
	"time"

	crypto "paperexperiment/crypto"
	identity "paperexperiment/identity"
	message "paperexperiment/message"
	quorum "paperexperiment/quorum"
	types "paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

/*
	 	==========================
	     paperexperiment Block Structure
		==========================
*/
type TransactionId common.Hash

// Sequence is array of TransactionId
type Sequence []*message.Transaction

type WorkerBlock struct {
	Shard         types.Shard
	Block_header  *WorkerBlockHeader
	Block_hash    common.Hash
	Transaction   []*message.Transaction
	Committee_sig []crypto.Signature

	QC       *quorum.QC
	CQC      *quorum.QC
	Proposer identity.NodeID
}

type WorkerBlockHeader struct {
	Epoch_num       types.Epoch
	View_num        types.View
	State_root      common.Hash
	Prev_block_hash common.Hash
	Block_height    types.BlockHeight
	Timestamp       time.Time
}

// MakeBlock creates an unsigned block
// func CreateWorkerBlockData(cross_payload []*message.Transaction, local_payload []*message.Transaction) *WorkerBlockData {
// 	b := new(WorkerBlockData)
// 	b.Cross_transaction = cross_payload
// 	b.Local_transaction = local_payload

// 	return b
// }

func CreateWorkerBlock(committed_transaction []*message.Transaction, epoch types.Epoch, view types.View, state_root common.Hash, prev_block_hash common.Hash, current_blockheight types.BlockHeight, qc *quorum.QC, shard types.Shard) *WorkerBlock {
	block_header := &WorkerBlockHeader{
		Epoch_num:       epoch,
		View_num:        view,
		State_root:      state_root,
		Prev_block_hash: prev_block_hash,
		Block_height:    current_blockheight + 1,
	}
	workerblock := new(WorkerBlock)
	workerblock.Shard = shard
	workerblock.Block_header = block_header
	workerblock.Block_hash = workerblock.MakeHash(workerblock.Block_header)
	workerblock.Transaction = committed_transaction
	// workerblock.Committee_sig = blockwithoutheader.Committee_sig
	workerblock.QC = qc

	return workerblock
}

func (wb *WorkerBlock) MakeHash(b interface{}) common.Hash {
	return crypto.MakeID(b)
}

type Block struct {
	types.View
	types.BlockHeight
	types.Epoch
	QC        *quorum.QC
	CQC       *quorum.QC
	Proposer  identity.NodeID
	Timestamp time.Time
	SCPayload []*message.Transaction
	StateHash common.Hash
	PrevID    common.Hash
	Sig       crypto.Signature
	ID        common.Hash
	Ts        time.Duration
}

type Request struct {
	Block Block
}

type OrderReq struct {
	Block             Block
	TargetBlockHeight types.BlockHeight
}

type ConfirmReq struct {
	Block *Block
}

type rawBlock struct {
	types.View
	QC       *quorum.QC
	CQC      *quorum.QC
	Proposer identity.NodeID
	Payload  []common.Hash
	PrevID   common.Hash
	Sig      crypto.Signature
	ID       common.Hash
}

// MakeBlock creates an unsigned block
func CreateBlock(view types.View, epoch types.Epoch, qc *quorum.QC, prevID common.Hash, scPayload []*message.Transaction, proposer identity.NodeID, stateHash common.Hash) *Block {
	b := new(Block)
	b.View = view
	b.Epoch = epoch
	b.BlockHeight = qc.BlockHeight + 1
	b.Proposer = proposer
	b.QC = qc
	//b.CQC = cqc
	b.SCPayload = scPayload
	b.PrevID = prevID
	b.StateHash = stateHash
	b.makeID(proposer)
	return b
}

func (b *Block) makeID(nodeID identity.NodeID) {
	raw := &rawBlock{
		View:     b.View,
		QC:       b.QC,
		CQC:      b.CQC,
		Proposer: b.Proposer,
		PrevID:   b.PrevID,
	}
	var payloadIDs []common.Hash
	raw.Payload = payloadIDs
	b.ID = crypto.MakeID(raw)
	b.Sig, _ = crypto.PrivSign(crypto.IDToByte(b.ID), nil)
}

type Accept struct {
	CommittedBlock *WorkerBlock
	*quorum.QC
	Timestamp time.Time
}
