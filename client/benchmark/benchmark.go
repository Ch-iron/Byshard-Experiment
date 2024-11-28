package benchmark

import (
	"encoding/gob"
	"fmt"
	"paperexperiment/client/ycsb"
	"paperexperiment/config"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/transport"
	"paperexperiment/types"
	"paperexperiment/utils"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"golang.org/x/exp/rand"
)

var count uint64

// DB is general interface implemented by client to call client library
type DB interface {
	Init() error
	Write(key int, value []byte, shards []types.Shard) error
	Stop() error
}

// DefaultBConfig returns a default benchmark config
func DefaultBConfig() config.Bconfig {
	return config.Bconfig{
		T:           60,
		N:           0,
		Throttle:    0,
		Concurrency: 1,
	}
}

// Benchmark is benchmarking tool that generates workload and collects operation history and latency
type Benchmark struct {
	db                   DB // read/write operation interface
	gatewaynodeTransport transport.Transport
	config.Bconfig
	*History

	rate      *Limiter
	latency   []time.Duration // latency per operation
	startTime time.Time
	// counter   int

	addressZipfGenerator        map[types.Shard]*ycsb.ZipfGenerator
	normalTravelCAZipfGenerator map[types.Shard]*ycsb.ZipfGenerator
	crossTravelCAZipfGenerator  map[types.Shard]*ycsb.ZipfGenerator
	txRatio                     map[types.TransactionForm]int
	wait                        sync.WaitGroup // waiting for all generated keys to complete

	address        map[types.Shard][]*common.Address
	normalTravelCA map[types.Shard][]config.CAwithExternalAddress
	crossTravelCA  map[types.Shard][]config.CAwithExternalAddress
	travelAbi      abi.ABI
	trainAbi       abi.ABI
	hotelAbi       abi.ABI
	paymentAbi     abi.ABI
}

// NewBenchmark returns new Benchmark object given implementation of DB interface
func NewBenchmark(db DB) *Benchmark {
	addrs := config.Configuration.Addrs
	b := new(Benchmark)
	b.db = db
	b.gatewaynodeTransport = transport.NewTransport(addrs[types.Shard(0)][identity.NewNodeID(0)] + "2999")
	b.Bconfig = config.Configuration.Benchmark
	b.History = NewHistory()
	if b.Throttle > 0 {
		b.rate = NewLimiter(b.Throttle)
	}

	// all EOA store
	b.address = make(map[types.Shard][]*common.Address)
	addresses := config.GetAddresses()
	for i := 0; i < len(addresses); i++ {
		addr := addresses[i]
		shard := utils.CalculateShardToSend([]common.Address{*addr})[0]
		if _, ok := b.address[shard]; !ok {
			b.address[shard] = make([]*common.Address, 0)
		}
		b.address[shard] = append(b.address[shard], addr)
	}

	// all CA store
	b.normalTravelCA = make(map[types.Shard][]config.CAwithExternalAddress)
	b.crossTravelCA = make(map[types.Shard][]config.CAwithExternalAddress)
	for i := 1; i <= config.GetConfig().ShardCount; i++ {
		if _, ok := b.normalTravelCA[types.Shard(i)]; !ok {
			b.normalTravelCA[types.Shard(i)] = make([]config.CAwithExternalAddress, 0)
		}
		b.normalTravelCA[types.Shard(i)] = config.GetTravelCA("normal", types.Shard(i))
		if _, ok := b.crossTravelCA[types.Shard(i)]; !ok {
			b.crossTravelCA[types.Shard(i)] = make([]config.CAwithExternalAddress, 0)
		}
		b.crossTravelCA[types.Shard(i)] = config.GetTravelCA("cross", types.Shard(i))
	}

	b.addressZipfGenerator = make(map[types.Shard]*ycsb.ZipfGenerator)
	b.normalTravelCAZipfGenerator = make(map[types.Shard]*ycsb.ZipfGenerator)
	b.crossTravelCAZipfGenerator = make(map[types.Shard]*ycsb.ZipfGenerator)
	for i := 1; i <= config.GetConfig().ShardCount; i++ {
		source := rand.New(rand.NewSource(uint64(time.Now().UnixNano())))
		b.addressZipfGenerator[types.Shard(i)], _ = ycsb.NewZipfGenerator(source, 0, uint64(len(b.address[types.Shard(i)])-1), b.Bconfig.ZipfTheta, false)
		b.normalTravelCAZipfGenerator[types.Shard(i)], _ = ycsb.NewZipfGenerator(source, 0, uint64(len(b.normalTravelCA[types.Shard(i)])-1), b.Bconfig.ZipfTheta, false)
		b.crossTravelCAZipfGenerator[types.Shard(i)], _ = ycsb.NewZipfGenerator(source, 0, uint64(len(b.crossTravelCA[types.Shard(i)])-1), b.Bconfig.ZipfTheta, false)
	}

	var rate int
	b.txRatio = make(map[types.TransactionForm]int)
	// NormalTranfer, NormalSmartContract, CrossShardTransfer, CrossShardSmartContract rate
	for i, value := range b.Bconfig.TXRatioPerType {
		rate += value
		b.txRatio[types.TransactionForm(i)] = rate
	}

	/* Register to gob en/decoder */
	gob.Register(message.TransactionForm{})
	return b
}

// Run starts the main logic of benchmarking
func (b *Benchmark) Run() {
	err := utils.Retry(b.gatewaynodeTransport.Dial, 100, time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}
	defer b.gatewaynodeTransport.Close()

	var genCount, sendCount, confirmCount uint64
	b.latency = make([]time.Duration, 0)
	keys := make(chan int, b.Concurrency)
	latencies := make(chan time.Duration, 1000)
	defer close(latencies)
	go b.collect(latencies)

	for i := 0; i < b.Concurrency; i++ {
		go b.worker(keys, latencies)
	}

	_ = b.db.Init()
	b.startTime = time.Now()
	if b.T > 0 {
		timer := time.NewTimer(time.Second * time.Duration(b.T))
	loop:
		for {
			select {
			case <-timer.C:
				log.Infof("Benchmark stops")
				break loop
			default:
				b.wait.Add(1)
				k := b.next()
				genCount++
				keys <- k
				sendCount++
			}
		}
	} else {
		for i := 0; i < b.N; i++ {
			b.wait.Add(1)
			keys <- b.next()
		}
		b.wait.Wait()
	}

	t := time.Since(b.startTime)

	_ = b.db.Stop()
	close(keys)
	stat := Statistic(b.latency)
	confirmCount = uint64(len(b.latency))
	log.Infof("Concurrency = %d", b.Concurrency)
	log.Infof("Benchmark Time = %v\n", t)
	log.Infof("Throughput = %f\n", float64(len(b.latency))/t.Seconds())
	log.Infof("genCount: %d, sendCount: %d, confirmCount: %d", genCount, sendCount, confirmCount)
	log.Info(stat)

	log.Warning("Client Program is Done. Please check client log files to check benchmark performance")
}

func getRandomIntUnder(n int) int {
	rand.NewSource(uint64(time.Now().UnixNano()))

	return rand.Intn(n)
}

func (b *Benchmark) makeNormalTransferTransaction() message.TransactionForm {
	sendTime := time.Now().UnixMilli()
	shardNum := types.Shard(getRandomIntUnder(config.GetConfig().ShardCount)) + 1
	idx := b.addressZipfGenerator[shardNum].Uint64()

	to := b.address[shardNum][idx]
	var from *common.Address
	for {
		idx := b.addressZipfGenerator[shardNum].Uint64()
		from = b.address[shardNum][idx]
		if from != to {
			break
		}
	}
	val := getRandomIntUnder(100)

	return message.TransactionForm{From: *from, To: *to, Value: val, Timestamp: sendTime}
}

func (b *Benchmark) makeCrossShardTransferTransaction() message.TransactionForm {
	sendTime := time.Now().UnixMilli()
	shardNum := types.Shard(getRandomIntUnder(config.GetConfig().ShardCount)) + 1
	receivedShardNum := (shardNum % types.Shard(config.GetConfig().ShardCount)) + 1
	fromIdx := b.addressZipfGenerator[shardNum].Uint64()
	toIdx := b.addressZipfGenerator[receivedShardNum].Uint64()

	from := b.address[shardNum][fromIdx]
	to := b.address[receivedShardNum][toIdx]
	val := getRandomIntUnder(100)

	return message.TransactionForm{From: *from, To: *to, Value: val, Timestamp: sendTime}
}

func (b *Benchmark) makeNormalSmartContractTransaction(abi abi.ABI) message.TransactionForm {
	sendTime := time.Now().UnixMilli()
	shardNum := types.Shard(getRandomIntUnder(config.GetConfig().ShardCount)) + 1
	fromIdx := b.addressZipfGenerator[shardNum].Uint64()
	contractIdx := b.normalTravelCAZipfGenerator[shardNum].Uint64()

	to, externalAddressList := b.normalTravelCA[shardNum][contractIdx].CA, b.normalTravelCA[shardNum][contractIdx].ExternalAddressList
	from := b.address[shardNum][fromIdx]
	val := getRandomIntUnder(100)

	data, err := abi.Pack("bookTrainAndHotel")
	if err != nil {
		fmt.Println(err)
	}

	mappingIdx := utils.CalculateMappingSlotIndex(*from, 0)

	return message.TransactionForm{From: *from, To: to, Value: val, Data: data, ExternalAddressList: externalAddressList, MappingIdx: mappingIdx, Timestamp: sendTime}
}

func (b *Benchmark) makeCrossShardSmartContractTransaction(abi abi.ABI) message.TransactionForm {
	sendTime := time.Now().UnixMilli()
	shardNum := types.Shard(getRandomIntUnder(config.GetConfig().ShardCount)) + 1
	fromIdx := b.addressZipfGenerator[shardNum].Uint64()
	contractIdx := b.crossTravelCAZipfGenerator[shardNum].Uint64()

	to, externalAddressList := b.crossTravelCA[shardNum][contractIdx].CA, b.crossTravelCA[shardNum][contractIdx].ExternalAddressList
	from := b.address[shardNum][fromIdx]
	val := getRandomIntUnder(100)

	data, err := abi.Pack("bookTrainAndHotel")
	if err != nil {
		fmt.Println(err)
	}

	mappingIdx := utils.CalculateMappingSlotIndex(*from, 0)

	return message.TransactionForm{From: *from, To: to, Value: val, Data: data, ExternalAddressList: externalAddressList, MappingIdx: mappingIdx, Timestamp: sendTime}
}

func (b *Benchmark) worker(keys <-chan int, result chan<- time.Duration) {
	abiObj := config.GetAbi()

	for k := range keys {
		op := new(operation)
		s := time.Now()

		rate := getRandomIntUnder(1000) + 1
		if rate <= b.txRatio[types.NORMALTRANSFER] {
			msg := b.makeNormalTransferTransaction()
			b.gatewaynodeTransport.Send(msg)
			log.Debugf("Send Normal Transfer Transaction %v", msg)
		} else if rate <= b.txRatio[types.NORMALSMARTCONTRACT] {
			msg := b.makeNormalSmartContractTransaction(abiObj)
			b.gatewaynodeTransport.Send(msg)
			log.Debugf("Send Normal Contract Transaction %v", msg)
		} else if rate <= b.txRatio[types.CROSSSHARDTRANSFER] {
			msg := b.makeCrossShardTransferTransaction()
			b.gatewaynodeTransport.Send(msg)
			log.Debugf("Send Cross Transfer Transaction %v", msg)
		} else {
			msg := b.makeCrossShardSmartContractTransaction(abiObj)
			b.gatewaynodeTransport.Send(msg)
			log.Debugf("Send Cross Contract Transaction %v", msg)
		}
		e := time.Now()

		result <- e.Sub(s)

		b.History.AddOperation(k, op)
	}
	log.Debugf("[Benchmark] Send Transaction Finish!!!")
}

// generates key based on distribution
func (b *Benchmark) next() int {
	var key int
	switch b.Distribution {
	case "uniform":
		log.Debugf("count: %v", count)
		key = int(count)
		count += uint64(config.GetConfig().N() - config.GetConfig().ByzNo)
	default:
		log.Fatalf("unknown distribution %s", b.Distribution)
	}

	if b.Throttle > 0 {
		b.rate.Wait()
	}

	return key
}

func (b *Benchmark) collect(latencies <-chan time.Duration) {
	for t := range latencies {
		b.latency = append(b.latency, t)
		b.wait.Done()
	}
}
