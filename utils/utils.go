package utils

import (
	"encoding/binary"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"time"

	"paperexperiment/config"
	"paperexperiment/message"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

type CaWithIdx struct {
	CA  common.Address
	Idx int
}
type CaBundle struct {
	TrainCA            []common.Address
	HotelCA            []common.Address
	CrossShardTravelCA []CaWithIdx
	NormalTravelCA     []common.Address
}

func GetBetweenShardTimer(from types.Shard, to types.Shard) *time.Timer {
	var timer *time.Timer
	if from == 1 && to == 2 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard12Latency))
	} else if from == 1 && to == 3 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard13Latency))
	} else if from == 2 && to == 1 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard12Latency))
	} else if from == 2 && to == 3 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard23Latency))
	} else if from == 3 && to == 1 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard13Latency))
	} else if from == 3 && to == 2 {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().Shard23Latency))
	} else if from == to {
		timer = time.NewTimer(time.Millisecond * time.Duration(config.GetConfig().InShardLatency))
	}
	return timer
}

func SlotToKey(slot uint64) common.Hash {
	key := common.BigToHash(big.NewInt(int64(slot)))
	return key
}

func RemoveSliceIndex[T any](index int, slice []T) []T {
	subSlice := []T{}
	for idx, value := range slice {
		if index == idx {
			continue
		}
		subSlice = append(subSlice, value)
	}
	return subSlice
}

func RemoveSliceTransaction(target_transaction *message.Transaction, slice []*message.Transaction) []*message.Transaction {
	subSlice := make([]*message.Transaction, 0)
	for _, transaction := range slice {
		if target_transaction.Hash == transaction.Hash {
			continue
		}
		subSlice = append(subSlice, transaction)
	}
	return subSlice
}

func RemoveReplicableValue(replicableshard []types.Shard) []types.Shard {
	key := make(map[types.Shard]struct{}, 0)
	result := make([]types.Shard, 0)

	for _, shard := range replicableshard {
		if _, ok := key[shard]; ok {
			continue
		} else {
			key[shard] = struct{}{}
			result = append(result, shard)
		}
	}
	return result
}

func CalculateShardToSend(addressList []common.Address) []types.Shard {
	shardNum := config.GetConfig().ShardCount
	shardToSend := []types.Shard{}
	for _, address := range addressList {
		a := new(big.Int)
		a.SetString(address.Hex()[2:], 16)
		shard := types.Shard(a.Mod(a, big.NewInt(int64(shardNum))).Int64() + 1)
		if !Contains(shardToSend, shard) {
			shardToSend = append(shardToSend, shard)
		}
	}
	return shardToSend
}

func CalculateMappingSlotIndex(address common.Address, slot uint64) []byte {
	// left pad address with 0 to 32 bytes
	addressBytes := common.LeftPadBytes(address.Bytes(), 32)

	// left pad slot with 0 to 32 bytes
	slotBytes := make([]byte, 32)
	binary.BigEndian.PutUint64(slotBytes[24:], slot)

	return crypto.Keccak256(addressBytes, slotBytes)
}

func FindIntSlice(slice []int, val int) bool {
	for _, item := range slice {
		if item == val {
			return true
		}
	}
	return false
}

func RandomPick(n int, f int) []int {
	var randomPick []int
	for i := 0; i < f; i++ {
		var randomID int
		exists := true
		for exists {
			s := rand.NewSource(time.Now().UnixNano())
			r := rand.New(s)
			randomID = r.Intn(n)
			exists = FindIntSlice(randomPick, randomID)
		}
		randomPick = append(randomPick, randomID)
	}
	return randomPick
}

// Max of two int
func Max(a, b int) int {
	if a < b {
		return b
	}
	return a
}

// VMax of a vector
func VMax(v ...int) int {
	max := v[0]
	for _, i := range v {
		if max < i {
			max = i
		}
	}
	return max
}

// Retry function f sleep time between attempts
func Retry(f func() error, attempts int, sleep time.Duration) error {
	var err error
	for i := 0; ; i++ {
		err = f()
		if err == nil {
			return nil
		}

		if i >= attempts-1 {
			break
		}

		// exponential delay
		time.Sleep(sleep * time.Duration(i+1))
	}
	return fmt.Errorf("after %d attempts, last error: %s", attempts, err)
}

// Schedule repeatedly call function with intervals
func Schedule(f func(), delay time.Duration) chan bool {
	stop := make(chan bool)

	go func() {
		for {
			f()
			select {
			case <-time.After(delay):
			case <-stop:
				return
			}
		}
	}()

	return stop
}

func MapRandomKeyGet(mapI interface{}) interface{} {
	keys := reflect.ValueOf(mapI).MapKeys()

	return keys[rand.Intn(len(keys))].Interface()
}

func IdentifierFixture() common.Hash {
	var id common.Hash
	_, _ = rand.Read(id[:])
	return id
}

func Map[T, V any](ts []T, fn func(T) V) []V {
	result := make([]V, len(ts))
	for i, t := range ts {
		result[i] = fn(t)
	}
	return result
}

func AddEntity[T any](hashMap map[types.Epoch]map[types.View]map[types.BlockHeight]T, epoch types.Epoch, view types.View, blockHeight types.BlockHeight, v T) {
	if _, ok := hashMap[epoch]; !ok {
		hashMap[epoch] = make(map[types.View]map[types.BlockHeight]T)
		hashMap[epoch][view] = make(map[types.BlockHeight]T)
	} else if _, ok := hashMap[epoch][view]; !ok {
		hashMap[epoch][view] = make(map[types.BlockHeight]T)
	}

	hashMap[epoch][view][blockHeight] = v
}

func Contains(s []types.Shard, target types.Shard) bool {
	for _, item := range s {
		if item == target {
			return true
		}
	}
	return false
}
