package identity

import (
	"strconv"

	"paperexperiment/log"
)

// NodeID represents a generic common.Hash in format of Zone.Node
type NodeID string

// NewNodeID returns a new NodeID type given two int number of zone and node
func NewNodeID(node int) NodeID {
	if node < 0 {
		node = -node
	}
	// return NodeID(fmt.Sprintf("%d.%d", zone, node))
	return NodeID(strconv.Itoa(node))
}

// Node returns Node NodeID component
func (i NodeID) Node() int {
	s := string(i)
	node, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		log.Errorf("Failed to convert Node %s to int\n", s)
		return 0
	}
	return int(node)
}

type IDs []NodeID

func (a IDs) Len() int      { return len(a) }
func (a IDs) Swap(i, j int) { a[i], a[j] = a[j], a[i] }
