package pacemaker

import (
	"paperexperiment/blockchain"
	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/quorum"
	"paperexperiment/types"
)

type TMO struct {
	types.Shard
	types.Epoch
	View                types.View
	NewView             types.View
	AnchorView          types.View
	NodeID              identity.NodeID
	BlockHeightLast     types.BlockHeight
	BlockHeightPrepared types.BlockHeight
	BlockLast           *blockchain.ShardBlock
	BlockPrepared       *blockchain.ShardBlock
	HighQC              *quorum.QC
}

type TC struct {
	types.Shard
	types.Epoch
	NodeID         identity.NodeID
	NewView        types.View
	AnchorView     types.View
	BlockHeightNew types.BlockHeight
	BlockMax       *blockchain.ShardBlock
	crypto.AggSig
	crypto.Signature
}

func NewTC(newView types.View, requesters map[identity.NodeID]*TMO) (*TC, *blockchain.ShardBlock) {
	// set variable
	var (
		shard          types.Shard
		anchorView     types.View
		blockHeightNew types.BlockHeight
		blockMax       *blockchain.ShardBlock
		blockPrepared  *blockchain.ShardBlock
	)

	var (
		maxAnchorView          types.View
		maxBlockHeightLast     types.BlockHeight
		maxBlockHeightPrepared types.BlockHeight
	)

	for _, tmo := range requesters {
		if maxAnchorView < tmo.AnchorView {
			maxAnchorView = tmo.AnchorView
		}

		if maxBlockHeightLast < tmo.BlockHeightLast {
			maxBlockHeightLast = tmo.BlockHeightLast
			blockMax = tmo.BlockLast
		}

		if maxBlockHeightPrepared < tmo.BlockHeightPrepared {
			maxBlockHeightPrepared = tmo.BlockHeightPrepared
			blockPrepared = tmo.BlockPrepared
		}
	}

	anchorView = maxAnchorView
	blockHeightNew = maxBlockHeightLast + 1

	// if there is prepared block existed
	if maxBlockHeightPrepared < blockHeightNew {
		blockPrepared = nil
	}
	return &TC{
		Shard:          shard,
		NewView:        newView,
		AnchorView:     anchorView,
		BlockHeightNew: blockHeightNew,
		BlockMax:       blockMax,
	}, blockPrepared
}
