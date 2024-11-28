package crypto

import (
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

// MakeID creates an ID from the hash of encoded data.
func MakeID(body interface{}) common.Hash {
	data := types.DefaultEncoder.MustEncode(body)
	hasher := NewSHA3_256()
	hash := hasher.ComputeHash(data)
	return HashToID(hash)
}

func HashToID(hash []byte) common.Hash {
	var id common.Hash
	copy(id[:], hash)
	return id
}

func IDToByte(id common.Hash) []byte {
	return id[:]
}
