package blockchain

import (
	"fmt"

	"paperexperiment/log"

	"github.com/ethereum/go-ethereum/common"
)

const prunedBlockLimit uint64 = 128

// LevelledForest contains multiple trees (which is a potentially disconnected planar graph).
// Each vertexContainer in the graph has a level (view) and a hash. A vertexContainer can only have one parent
// with strictly smaller level (view). A vertexContainer can have multiple children, all with
// strictly larger level (view).
// A LevelledForest provides the ability to prune all vertices up to a specific level.
// A tree whose root is below the pruning threshold might decompose into multiple
// disconnected subtrees as a result of pruning.
type LeveledForest struct {
	vertices          VertexSet
	verticesAtLevel   map[uint64]VertexList
	LowestLevel       uint64
	LowestPrunedLevel uint64
}

type VertexList []*vertexContainer
type VertexSet map[common.Hash]*vertexContainer

// vertexContainer holds information about a tree vertex. Internally, we distinguish between
//   - FULL container: has non-nil value for vertex.
//     Used for vertices, which have been added to the tree.
//   - EMPTY container: has NIL value for vertex.
//     Used for vertices, which have NOT been added to the tree, but are
//     referenced by vertices in the tree. An empty container is converted to a
//     full container when the respective vertex is added to the tree
type vertexContainer struct {
	id       common.Hash
	level    uint64
	children VertexList

	// the following are only set if the block is actually known
	vertex Vertex
}

// NewLevelledForest initializes a LevelledForest
func NewLeveledForest() *LeveledForest {
	return &LeveledForest{
		vertices:        make(VertexSet),
		verticesAtLevel: make(map[uint64]VertexList),
	}
}

// PruneUpToLevel prunes all blocks UP TO but NOT INCLUDING `level`
func (f *LeveledForest) PruneUpToLevel(level uint64) ([]*ShardBlock, int, error) {
	// 1. find committed levels
	// 2. go through each level and prune, if it is not committed, add it to pruned
	var prunedBlockNo int
	forkedBlocks := make([]*ShardBlock, 0)
	committedLevels := make(map[uint64]bool)
	if level < f.LowestLevel {
		return nil, prunedBlockNo, fmt.Errorf("new lowest level %d cannot be smaller than previous last retained level %d", level, f.LowestLevel)
	}
	for l := level; l >= f.LowestLevel && l > 1; {
		// assume each level has only one vertex
		vertex := f.verticesAtLevel[l][0].vertex
		parentID, _ := vertex.Parent()
		parentVertex, ok := f.GetVertex(parentID)
		if !ok || parentVertex.Level() < f.LowestLevel {
			break
		}
		committedLevels[parentVertex.Level()] = true
		l = parentVertex.Level()
	}
	for l := f.LowestLevel; l < level; l++ {
		// find fork blocks
		for _, v := range f.verticesAtLevel[l] { // nil map behaves like empty map when iterating over it
			if !committedLevels[l] && l > 1 {
				if v.vertex != nil {
					log.Debugf("found a forked block, view: %v, id: %x", v.vertex.Level(), v.vertex.VertexID())
					forkedBlocks = append(forkedBlocks, v.vertex.GetBlock())
				}
			}
		}
	}

	f.LowestLevel = level

	if level < prunedBlockLimit {
		return forkedBlocks, prunedBlockNo, nil
	}
	for l := f.LowestPrunedLevel; l <= level-prunedBlockLimit; l++ {
		// find fork blocks
		for _, v := range f.verticesAtLevel[l] { // nil map behaves like empty map when iterating over it
			prunedBlockNo++
			delete(f.vertices, v.id)
		}
		delete(f.verticesAtLevel, l)
	}

	f.LowestPrunedLevel = level - prunedBlockLimit

	return forkedBlocks, prunedBlockNo, nil
}

// HasVertex returns true iff full vertex exists
func (f *LeveledForest) HasVertex(id common.Hash) bool {
	container, exists := f.vertices[id]
	return exists && !f.isEmptyContainer(container)
}

// isEmptyContainer returns true iff vertexContainer container is empty, i.e. full vertex itself has not been added
func (f *LeveledForest) isEmptyContainer(vertexContainer *vertexContainer) bool {
	return vertexContainer.vertex == nil
}

// GetVertex returns (<full vertex>, true) if the vertex with `id` and `level` was found
// (nil, false) if full vertex is unknown
func (f *LeveledForest) GetVertex(id common.Hash) (Vertex, bool) {
	container, exists := f.vertices[id]
	if !exists || f.isEmptyContainer(container) {
		return nil, false
	}
	return container.vertex, true
}

// GetChildren returns a VertexIterator to iterate over the children
// An empty VertexIterator is returned, if no vertices are known whose parent is `id` , `level`
func (f *LeveledForest) GetChildren(id common.Hash) VertexIterator {
	container := f.vertices[id]
	// if vertex does not exists, container is the default zero value for vertexContainer, which contains a nil-slice for its children
	return newVertexIterator(container.children) // VertexIterator gracefully handles nil slices
}

// GetVerticesAtLevel returns a VertexIterator to iterate over the Vertices at the specified height
func (f *LeveledForest) GetNumberOfChildren(id common.Hash) int {
	container := f.vertices[id] // if vertex does not exists, container is the default zero value for vertexContainer, which contains a nil-slice for its children
	num := 0
	for _, child := range container.children {
		if child.vertex != nil {
			num++
		}
	}
	return num
}

// GetVerticesAtLevel returns a VertexIterator to iterate over the Vertices at the specified height
// An empty VertexIterator is returned, if no vertices are known at the specified `level`
func (f *LeveledForest) GetVerticesAtLevel(level uint64) VertexIterator {
	return newVertexIterator(f.verticesAtLevel[level]) // go returns the zero value for a missing level. Here, a nil slice
}

// GetVerticesAtLevel returns a VertexIterator to iterate over the Vertices at the specified height
func (f *LeveledForest) GetNumberOfVerticesAtLevel(level uint64) int {
	num := 0
	for _, container := range f.verticesAtLevel[level] {
		if container.vertex != nil {
			num++
		}
	}
	return num
}

// AddVertex adds vertex to forest if vertex is within non-pruned levels
// Handles repeated addition of same vertex (keeps first added vertex).
// If vertex is at or below pruning level: method is NoOp.
// UNVALIDATED:
// requires that vertex would pass validity check LevelledForest.VerifyVertex(vertex).
func (f *LeveledForest) AddVertex(vertex Vertex) {
	if vertex.Level() < f.LowestLevel {
		return
	}
	container := f.getOrCreateVertexContainer(vertex.VertexID(), vertex.Level())
	if !f.isEmptyContainer(container) { // the vertex was already stored
		return
	}
	// container is empty, i.e. full vertex is new and should be stored in container
	container.vertex = vertex // add vertex to container
	f.registerWithParent(container)
}

func (f *LeveledForest) registerWithParent(vertexContainer *vertexContainer) {
	// caution: do not modify this combination of check (a) and (a)
	// Deliberate handling of root vertex (genesis block) whose view is _exactly_ at LowestLevel
	// For this block, we don't care about its parent and the exception is allowed where
	// vertex.level = vertex.Parent().Level = LowestLevel = 0
	if vertexContainer.level <= f.LowestLevel { // check (a)
		return
	}

	_, parentView := vertexContainer.vertex.Parent()
	if parentView < f.LowestLevel {
		return
	}
	parentContainer := f.getOrCreateVertexContainer(vertexContainer.vertex.Parent())
	parentContainer.children = append(parentContainer.children, vertexContainer) // append works on nil slices: creates slice with capacity 2
}

// getOrCreateVertexContainer returns the vertexContainer if there exists one
// or creates a new vertexContainer and adds it to the internal data structures.
// It errors if a vertex with same id but different Level is already known
// (i.e. there exists an empty or full container with the same id but different level).
func (f *LeveledForest) getOrCreateVertexContainer(id common.Hash, level uint64) *vertexContainer {
	container, exists := f.vertices[id] // try to find vertex container with same ID
	if !exists {                        // if no vertex container found, create one and store it
		container = &vertexContainer{
			id:    id,
			level: level,
		}
		f.vertices[container.id] = container
		vtcs := f.verticesAtLevel[container.level]                   // returns nil slice if not yet present
		f.verticesAtLevel[container.level] = append(vtcs, container) // append works on nil slices: creates slice with capacity 2
	}
	return container
}

// VerifyVertex verifies that vertex satisfies the following conditions
// (1)
func (f *LeveledForest) VerifyVertex(vertex Vertex) error {
	if vertex.Level() < f.LowestLevel {
		return nil
	}
	isKnownVertex, err := f.isEquivalentToStoredVertex(vertex)
	if err != nil {
		return fmt.Errorf("invalid Vertex: %w", err)
	}
	if isKnownVertex {
		return nil
	}
	// vertex not found in storage => new vertex

	// verify new vertex
	if vertex.Level() == f.LowestLevel {
		return nil
	}
	return f.verifyParent(vertex)
}

// isEquivalentToStoredVertex
// evaluates whether a vertex is equivalent to already stored vertex.
// for vertices at pruning level, parents are ignored
//
// (1) return value (false, nil)
// Two vertices are _not equivalent_ if they have different IDs (Hashes).
//
// (2) return value (true, nil)
// Two vertices _are equivalent_ if their respective fields are identical:
// ID, Level, and Parent (both parent ID and parent Level)
//
// (3) return value (false, error)
// errors if the vertices' IDs are identical but they differ
// in any of the _relevant_ fields (as defined in (2)).
func (f *LeveledForest) isEquivalentToStoredVertex(vertex Vertex) (bool, error) {
	storedVertex, haveStoredVertex := f.GetVertex(vertex.VertexID())
	if !haveStoredVertex {
		return false, nil //have no vertex with same id stored
	}

	// found vertex in storage with identical ID
	// => we expect all other (relevant) fields to be identical
	if vertex.Level() != storedVertex.Level() { // view number
		return false, fmt.Errorf("conflicting vertices with ID %v", vertex.VertexID())
	}
	if vertex.Level() <= f.LowestLevel {
		return true, nil
	}
	newParentId, newParentView := vertex.Parent()
	storedParentId, storedParentView := storedVertex.Parent()
	if newParentId != storedParentId { // qc.blockID
		return false, fmt.Errorf("conflicting vertices with ID %v", vertex.VertexID())
	}
	if newParentView != storedParentView { // qc.view
		return false, fmt.Errorf("conflicting vertices with ID %v", vertex.VertexID())
	}
	// all _relevant_ fields identical
	return true, nil
}

// verifyParent verifies whether vertex.Parent() is consistent with current forest.
// An error is raised if
// * there is a parent with the same id but different view;
// * the parent's level is _not_ smaller than the vertex's level
func (f *LeveledForest) verifyParent(vertex Vertex) error {
	// verify parent
	parentID, parentLevel := vertex.Parent()
	if !(vertex.Level() > parentLevel) {
		return fmt.Errorf("parent vertex's level (%d) must be smaller than the vertex's level (%d)", parentLevel, vertex.Level())
	}
	parentVertex, haveParentStored := f.GetVertex(parentID)
	if !haveParentStored {
		return nil
	}
	if parentVertex.Level() != parentLevel {
		return fmt.Errorf("parent vertex of %v has different level (%d) than the stored vertex (%d)",
			vertex.VertexID(), parentLevel, parentVertex.Level(),
		)
	}
	return nil
}

// remove highest block from blockchain
func (f *LeveledForest) revertHighestBlock(level uint64) {
	for _, v := range f.verticesAtLevel[level] { // nil map behaves like empty map when iterating over it
		delete(f.vertices, v.id)
	}
	delete(f.verticesAtLevel, level)
}
