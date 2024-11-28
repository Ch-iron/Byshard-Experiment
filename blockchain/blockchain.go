package blockchain

import (
	"fmt"
	"os"
	"strconv"

	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/evm"
	"paperexperiment/evm/db/rawdb"
	"paperexperiment/evm/state"
	"paperexperiment/evm/state/snapshot"
	"paperexperiment/evm/triedb"
	"paperexperiment/evm/triedb/hashdb"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/quorum"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

type BlockChain struct {
	forest  *LeveledForest
	statedb *state.StateDB
	nodedb  state.Database
	// measurement
	highestComitted     int
	committedBlockNo    int
	totalBlockIntervals int
	prunedBlockNo       int
}

func NewBlockchain(shard types.Shard, defaultBalance int, nodeID int) *BlockChain {
	bc := new(BlockChain)
	bc.forest = NewLeveledForest()

	stateRoots := []string{}
	for i := 1; i <= config.GetConfig().ShardCount; i++ {
		shardRoot, _ := os.ReadFile(fmt.Sprintf("../common/statedb/shard%v_root.txt", i))
		stateRoots = append(stateRoots, string(shardRoot))
	}
	shardRoot := common.HexToHash(stateRoots[int(shard)-1])

	snapconfig := snapshot.Config{
		CacheSize:  256,
		Recovery:   true,
		NoBuild:    false,
		AsyncBuild: false,
	}

	triedbConfig := &triedb.Config{
		Preimages: true,
		HashDB: &hashdb.Config{
			CleanCacheSize: 256 * 1024 * 1024,
		},
	}

	dbObject, err := rawdb.NewLevelDBDatabase("../common/statedb/"+strconv.Itoa(int(shard))+"/"+strconv.Itoa(nodeID), 128, 1024, "", false)
	if err != nil {
		log.Errorf("Error while reading state root: %v", err)
	}

	tdb := triedb.NewDatabase(dbObject, triedbConfig)
	snaps, _ := snapshot.New(snapconfig, dbObject, tdb, shardRoot)
	bc.nodedb = state.NewDatabase(tdb, snaps)
	bc.statedb, _ = state.New(shardRoot, bc.nodedb)

	return bc
}

func (bc *BlockChain) Exists(id common.Hash) bool {
	return bc.forest.HasVertex(id)
}

func (bc *BlockChain) AddBlock(block *WorkerBlock) {
	blockContainer := &BlockContainer{block}
	bc.forest.AddVertex(blockContainer)
}

func (bc *BlockChain) GetCurrentStateHash() common.Hash {
	return bc.statedb.GetTrie().Hash()
}

func (bc *BlockChain) ExecuteTransaction(tx *message.Transaction) error {
	// Update Balance of Sender and Receiver
	return evm.Transfer(bc.statedb, tx.From, tx.To, uint256.NewInt(uint64(tx.Value)))
}

func (bc *BlockChain) GetStateDB() *state.StateDB {
	return bc.statedb
}

func (bc *BlockChain) SetStateDB(rootHash common.Hash) {
	bc.statedb, _ = state.New(rootHash, bc.nodedb)
}

func (bc *BlockChain) GetBlockByID(id common.Hash) (*WorkerBlock, error) {
	vertex, exists := bc.forest.GetVertex(id)
	if !exists {
		return nil, fmt.Errorf("the block does not exist, id: %x", id)
	}
	return vertex.GetBlock(), nil
}

func (bc *BlockChain) GetParentBlock(id common.Hash) (*WorkerBlock, error) {
	vertex, exists := bc.forest.GetVertex(id)
	if !exists {
		return nil, fmt.Errorf("the block does not exist, id: %x", id)
	}
	parentID, _ := vertex.Parent()
	parentVertex, exists := bc.forest.GetVertex(parentID)
	if !exists {
		return nil, fmt.Errorf("parent block does not exist, id: %x", parentID)
	}
	return parentVertex.GetBlock(), nil
}

func (bc *BlockChain) GetGrandParentBlock(id common.Hash) (*WorkerBlock, error) {
	parentBlock, err := bc.GetParentBlock(id)
	if err != nil {
		return nil, fmt.Errorf("cannot get parent block: %w", err)
	}
	return bc.GetParentBlock(parentBlock.Block_hash)
}

func (bc *BlockChain) GetStateHashByBlockID(id common.Hash) (common.Hash, error) {
	block, err := bc.GetBlockByID(id)
	if err != nil {
		return crypto.MakeID(nil), err
	}
	return block.Block_header.State_root, nil
}

// CommitBlock prunes blocks and returns committed blocks up to the last committed one and prunedBlocks
func (bc *BlockChain) CommitBlock(id common.Hash, blockHeight types.BlockHeight, quorums []quorum.Quorum) ([]*WorkerBlock, []*WorkerBlock, error) {
	vertex, ok := bc.forest.GetVertex(id)
	if !ok {
		return nil, nil, fmt.Errorf("cannot find the block, id: %x", id)
	}
	committedBlockHeight := vertex.GetBlock().Block_header.Block_height
	bc.highestComitted = int(vertex.GetBlock().Block_header.Block_height)
	var committedBlocks []*WorkerBlock
	// Block이 정상적으로 합의에 이르렀는지 체크
	// 범위: ( Lowest Level block, Commit 대상 Block ]
	for block := vertex.GetBlock(); uint64(block.Block_header.Block_height) > bc.forest.LowestLevel; {
		committedBlocks = append(committedBlocks, block)

		for _, quourm := range quorums {
			quourm.Delete(block.Block_hash)
		}
		bc.committedBlockNo++
		bc.totalBlockIntervals += int(blockHeight - block.Block_header.Block_height)
		vertex, exists := bc.forest.GetVertex(block.Block_header.Prev_block_hash)
		if !exists {
			break
		}
		block = vertex.GetBlock()
	}

	// commit 대상 block의 view에 대하여, pruning 진행
	forkedBlocks, prunedNo, err := bc.forest.PruneUpToLevel(uint64(committedBlockHeight))
	if err != nil {
		return nil, nil, fmt.Errorf("cannot prune the blockchain to the committed block, id: %w", err)
	}
	bc.prunedBlockNo += prunedNo

	return committedBlocks, forkedBlocks, nil
}

func (bc *BlockChain) RevertBlock(blockheight types.BlockHeight) {
	bc.forest.revertHighestBlock(uint64(blockheight))
}

func (bc *BlockChain) GetChildrenBlocks(id common.Hash) []*WorkerBlock {
	var blocks []*WorkerBlock
	iterator := bc.forest.GetChildren(id)
	for I := iterator; I.HasNext(); {
		blocks = append(blocks, I.NextVertex().GetBlock())
	}
	return blocks
}

func (bc *BlockChain) GetChainGrowth() float64 {
	return float64(bc.committedBlockNo) / float64(bc.prunedBlockNo+1)
}

func (bc *BlockChain) GetBlockIntervals() float64 {
	return float64(bc.totalBlockIntervals) / float64(bc.committedBlockNo)
}

func (bc *BlockChain) GetHighestCommitted() int {
	return bc.highestComitted
}

func (bc *BlockChain) GetCommittedBlocks() int {
	return bc.committedBlockNo
}

func (bc *BlockChain) GetBlockByBlockHeight(blockHeight types.BlockHeight) *WorkerBlock {
	iterator := bc.forest.GetVerticesAtLevel(uint64(blockHeight))
	return iterator.next.GetBlock()
}
