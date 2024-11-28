package blockchain

import "github.com/ethereum/go-ethereum/common"

// BlockContainer wraps a block to implement forest.Vertex
// In addition, it holds some additional properties for efficient processing of blocks
// by the Finalizer
type BlockContainer struct {
	Block *WorkerBlock
}

// functions implementing forest.vertex
func (b *BlockContainer) VertexID() common.Hash { return b.Block.Block_hash }
func (b *BlockContainer) Level() uint64         { return uint64(b.Block.Block_header.Block_height) }
func (b *BlockContainer) Parent() (common.Hash, uint64) {
	return b.Block.Block_header.Prev_block_hash, uint64(b.Block.QC.BlockHeight)
}
func (b *BlockContainer) GetBlock() *WorkerBlock { return b.Block }
