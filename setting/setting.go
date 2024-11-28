package setting

import (
	"fmt"
	"math"
	"math/big"
	"os"
	"paperexperiment/config"
	"paperexperiment/evm/db/rawdb"
	"paperexperiment/evm/state"
	"paperexperiment/evm/state/snapshot"
	"paperexperiment/evm/state/tracing"
	"paperexperiment/evm/triedb"
	"paperexperiment/evm/triedb/hashdb"
	evmTypes "paperexperiment/evm/types"
	"paperexperiment/evm/vm"
	"paperexperiment/evm/vm/runtime"
	"paperexperiment/log"
	"paperexperiment/types"
	"paperexperiment/utils"
	"strconv"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
	"github.com/holiman/uint256"
)

var path = "../common/"

const SHANGHAI_BLOCK_COUNT = 16830000

func setDefaults(blockheight int) *runtime.Config {
	cfg := new(runtime.Config)
	if cfg.Difficulty == nil {
		cfg.Difficulty = big.NewInt(0)
	}
	if cfg.GasLimit == 0 {
		cfg.GasLimit = math.MaxUint64
	}
	if cfg.GasPrice == nil {
		cfg.GasPrice = new(big.Int)
	}
	if cfg.Value == nil {
		cfg.Value = new(big.Int)
	}
	if cfg.GetHashFn == nil {
		cfg.GetHashFn = func(n uint64) common.Hash {
			return common.BytesToHash(crypto.Keccak256([]byte(new(big.Int).SetUint64(n).String())))
		}
	}
	if cfg.BaseFee == nil {
		cfg.BaseFee = big.NewInt(params.InitialBaseFee)
	}
	if cfg.BlobBaseFee == nil {
		cfg.BlobBaseFee = big.NewInt(params.BlobTxMinBlobGasprice)
	}

	cfg.ChainConfig = params.SepoliaChainConfig

	// Decide evm version
	// EVM version is must more than shanghai because recent solidity compile has PUSH0 opcode
	cfg.BlockNumber = big.NewInt(SHANGHAI_BLOCK_COUNT + int64(blockheight))
	cfg.Time = uint64(time.Now().Unix())
	random := common.BytesToHash([]byte("OSDC"))
	cfg.Random = &random

	return cfg
}

func SetLevelDBState(shard types.Shard) {
	cfg := setDefaults(0)

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

	leveldb, err := rawdb.NewLevelDBDatabase("../common/statedb/"+strconv.Itoa(int(shard)), 128, 1024, "", false)
	if err != nil {
		log.Errorf("Error while creating leveldb: %s", err)
	}
	tdb := triedb.NewDatabase(leveldb, triedbConfig)
	snaps, _ := snapshot.New(snapconfig, leveldb, tdb, evmTypes.EmptyRootHash)
	statedb := state.NewDatabase(tdb, snaps)
	levelDBState, _ := state.New(evmTypes.EmptyRootHash, statedb)

	deployAddress := common.BytesToAddress([]byte("OSDC"))
	levelDBState.CreateAccount(deployAddress)
	levelDBState.SetBalance(deployAddress, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
	deployer := vm.AccountRef(deployAddress)

	addresses := config.GetAddresses()
	travelnormalCAs, trainnormalCAs, hotelnormalCAs, travelcrossCAs, traincrossCAs, hotelcrossCAs, travelcrossParams := config.GetCA(shard)

	trainCode, _ := os.ReadFile(path + "contracts/train/train.bin")
	hotelCode, _ := os.ReadFile(path + "contracts/hotel/hotel.bin")
	travelCode, _ := os.ReadFile(path + "contracts/travel/travel.bin")

	// Deploy Contract
	evm_ := runtime.NewEnv(cfg, levelDBState)

	for _, trainCA := range trainnormalCAs {
		code := config.GetDeployCode("train", trainCode, "", "")
		_, _, _, err := evm_.CreateFDCM(trainCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying train contract: %s %v", err, trainCA)
		}
	}
	log.Debugf("Deployed normal train contracts for shard %v", shard)

	for _, hotelCA := range hotelnormalCAs {
		code := config.GetDeployCode("hotel", hotelCode, "", "")
		_, _, _, err := evm_.CreateFDCM(hotelCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying hotel contract: %s", err)
		}
	}
	log.Debugf("Deployed normal hotel contracts for shard %v", shard)

	for idx, travelCA := range travelnormalCAs {
		code := config.GetDeployCode("travel", travelCode, trainnormalCAs[idx].Hex()[2:], hotelnormalCAs[idx].Hex()[2:])
		_, _, _, err := evm_.CreateFDCM(travelCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying travel contract: %s", err)
		}
	}
	log.Debugf("Deployed normal travel contracts for shard %v", shard)

	for _, trainCA := range traincrossCAs {
		code := config.GetDeployCode("train", trainCode, "", "")
		_, _, _, err := evm_.CreateFDCM(trainCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying train contract: %s", err)
		}
	}
	log.Debugf("Deployed cross train contracts for shard %v", shard)

	for _, hotelCA := range hotelcrossCAs {
		code := config.GetDeployCode("hotel", hotelCode, "", "")
		_, _, _, err := evm_.CreateFDCM(hotelCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying hotel contract: %s", err)
		}
	}
	log.Debugf("Deployed cross hotel contracts for shard %v", shard)

	for idx, travelCA := range travelcrossCAs {
		code := config.GetDeployCode("travel", travelCode, travelcrossParams[idx][0].Hex()[2:], travelcrossParams[idx][1].Hex()[2:])
		_, _, _, err := evm_.CreateFDCM(travelCA, deployer, code, levelDBState.GetBalance(deployAddress).Uint64(), uint256.NewInt(0))
		if err != nil {
			log.Errorf("Error while deploying travel contract: %s", err)
		}
	}
	log.Debugf("Deployed cross travel contracts for shard %v", shard)

	for _, address := range addresses {
		addr := []common.Address{*address}
		if utils.CalculateShardToSend(addr)[0] == shard {
			levelDBState.SetBalance(*address, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
			levelDBState.SetNonce(*address, 0)
		}
	}

	log.Debugf("Set Account for shard %v", shard)

	root, err := levelDBState.Commit(0, true, 0)
	if err != nil {
		log.Errorf("Error while committing state: %s", err)
	}
	log.Debugf("Commit Root: %v", root)

	file, err := os.Create(fmt.Sprintf("../common/statedb/shard%v_root.txt", shard))
	if err != nil {
		os.Remove(fmt.Sprintf(path+"statedb/shard%v_root.txt", shard))
		file, _ = os.Create(fmt.Sprintf("../common/statedb/shard%v_root.txt", shard))
	}

	if _, err = file.WriteString(root.Hex()); err != nil {
		log.Errorf("Error while writing root: %s", err)
	}

	if err := tdb.Commit(root, false); err != nil {
		log.Errorf("failed to commit state trie: %s\n", err)
	}
	leveldb.Close()
}

func SetContractAddress() {
	one, two, three := 0, 0, 0

	addresses := config.GetAddresses()

	dir := "../common/ca/"
	file1, _ := os.Create(fmt.Sprintf(dir + "1/normal.txt"))
	file2, _ := os.Create(fmt.Sprintf(dir + "2/normal.txt"))
	file3, _ := os.Create(fmt.Sprintf(dir + "3/normal.txt"))
	file4, _ := os.Create(fmt.Sprintf(dir + "1/cross.txt"))
	file5, _ := os.Create(fmt.Sprintf(dir + "2/cross.txt"))
	file6, _ := os.Create(fmt.Sprintf(dir + "3/cross.txt"))

	for _, address := range addresses {
		addr := []common.Address{*address}
		shard := utils.CalculateShardToSend(addr)[0]

		if shard == 1 && one == 10000 {
			continue
		} else if shard == 2 && two == 10000 {
			continue
		} else if shard == 3 && three == 10000 {
			continue
		}

		if one == 10000 && two == 10000 && three == 10000 {
			break
		}

		travelCA := crypto.CreateAddress(addr[0], uint64(0))
		travelCAShard := utils.CalculateShardToSend([]common.Address{travelCA})[0]
		if shard != travelCAShard {
			travelCA = common.BigToAddress(travelCA.Big().Add(travelCA.Big(), big.NewInt(int64(shard)-int64(travelCAShard))))
		}

		trainCA := crypto.CreateAddress(travelCA, uint64(0))
		trainCAShard := utils.CalculateShardToSend([]common.Address{trainCA})[0]
		if shard != trainCAShard {
			trainCA = common.BigToAddress(trainCA.Big().Add(trainCA.Big(), big.NewInt(int64(shard)-int64(trainCAShard))))
		}

		hotelCA := crypto.CreateAddress(travelCA, uint64(1))
		hotelCAShard := utils.CalculateShardToSend([]common.Address{hotelCA})[0]
		if shard != hotelCAShard {
			hotelCA = common.BigToAddress(hotelCA.Big().Add(hotelCA.Big(), big.NewInt(int64(shard)-int64(hotelCAShard))))
		}
		normalCA := fmt.Sprintf("%v,%v,%v", travelCA, trainCA, hotelCA)

		crosstravelCA := crypto.CreateAddress(addr[0], uint64(1))
		crosstravelCASahrd := utils.CalculateShardToSend([]common.Address{crosstravelCA})[0]
		if shard != crosstravelCASahrd {
			crosstravelCA = common.BigToAddress(crosstravelCA.Big().Add(crosstravelCA.Big(), big.NewInt(int64(shard)-int64(crosstravelCASahrd))))
		}

		crosstrainCA := crypto.CreateAddress(crosstravelCA, uint64(0))
		crosstrainCASahrd := utils.CalculateShardToSend([]common.Address{crosstrainCA})[0]
		if shard != crosstrainCASahrd {
			crosstrainCA = common.BigToAddress(crosstrainCA.Big().Add(crosstrainCA.Big(), big.NewInt(int64(shard)-int64(crosstrainCASahrd))))
		}

		crosshotelCA := crypto.CreateAddress(crosstravelCA, uint64(1))
		crosshotelCASahrd := utils.CalculateShardToSend([]common.Address{crosshotelCA})[0]
		if shard != crosshotelCASahrd {
			crosshotelCA = common.BigToAddress(crosshotelCA.Big().Add(crosshotelCA.Big(), big.NewInt(int64(shard)-int64(crosshotelCASahrd))))
		}
		crossCA := fmt.Sprintf("%v,%v,%v", crosstravelCA, crosstrainCA, crosshotelCA)

		switch shard {
		case 1:
			one++
			fmt.Fprintln(file1, normalCA)
			fmt.Fprintln(file4, crossCA)
		case 2:
			two++
			fmt.Fprintln(file2, normalCA)
			fmt.Fprintln(file5, crossCA)
		case 3:
			three++
			fmt.Fprintln(file3, normalCA)
			fmt.Fprintln(file6, crossCA)
		}
	}
}
