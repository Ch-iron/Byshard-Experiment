package main

import (
	"flag"
	"strconv"
	"sync"
	"time"

	"paperexperiment"
	"paperexperiment/blockbuilder"
	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/types"
	"paperexperiment/worker"

	"net/http"
	_ "net/http/pprof"
)

var (
	algorithm  = flag.String("algorithm", "pbft", "BFT consensus algorithm")
	id         = flag.Int("id", 0, "NodeID of the node")
	simulation = flag.Bool("sim", true, "simulation mode")
	mode       = flag.String("mode", "coordination", "Select worker or coordination shard") // worker, coordination, gateway
	shard      = flag.Int("shard", 0, "shard that node belongs to (only available on mode is blockbuilder or node)")
)

func initReplica(id identity.NodeID, isByz bool, shard types.Shard, wg *sync.WaitGroup) {
	if isByz {
		log.Infof("node %v is Byzantine", id)
	}
	switch *algorithm {
	case "pbft":
		log.Info("Consensus Algorithm: PBFT")
		log.Debugf("Shard %v worker node %v start...", shard, id)
		r := worker.NewReplica(id, *algorithm, isByz, shard)
		r.Start(wg)

	default:
		r := worker.NewReplica(id, *algorithm, isByz, shard)
		r.Start(wg)

	}
}

func main() {
	go func() {
		http.ListenAndServe("0.0.0.0:6060", nil)
	}()
	paperexperiment.Init()

	// the private and public keys are generated here
	errCrypto := crypto.SetKey()
	if errCrypto != nil {
		log.Fatal("Could not generate keys:", errCrypto)
	}

	if *simulation {
		var wg sync.WaitGroup
		config.Simulation()

		// Blockchain Init Network Connection
		log.Debugf("Gateway Start")
		gw := blockbuilder.NewInitGateway("tcp://127.0.0.1:2999", types.Shard(0))
		gw.Start()
		log.Debugf("Gateway ip: %v", gw.GetIP())

		time.Sleep(1 * time.Second / 2)

		log.Debugf("[Blockbuilder Init] Init BlockBuilder & Gateway Complete")

		log.Debugf("Worker BlockBuilder Start")
		blockbuilders := make([]*blockbuilder.WorkerBlockBuilder, 0)
		for i := 1; i <= config.GetConfig().ShardCount; i++ {
			port := strconv.Itoa(3000 + i)
			wbb := blockbuilder.NewInitWorkerBlockBuilder("tcp://127.0.0.1:"+port, types.Shard(i), "tcp://127.0.0.1:2999")
			blockbuilders = append(blockbuilders, wbb)
			wbb.Start()
			log.Debugf("Worker BlockBuilder shard: %v, ip: %v", wbb.GetShard(), wbb.GetIP())
		}

		// Worker consensus Node
		for shard, addrs := range config.GetConfig().Addrs {
			for id := range addrs {
				isByz := false
				if id.Node()%config.GetConfig().NPerShard()+1 <= config.GetConfig().ByzNo && shard != 0 {
					isByz = true
				}
				wg.Add(1)
				go initReplica(id, isByz, shard, &wg)
			}
			time.Sleep(1 * time.Second)
			log.Debugf("[Worker Shard %v Init] Worker Node Setup Complete", shard)
		}
		wg.Wait()
		log.Debugf("Setup Finish")

		time.Sleep(1 * time.Second)

		wg.Add(1)
		for _, blockbuilder := range blockbuilders {
			go blockbuilder.WorkerBlockBuilderReplica.SendNodeStartMessage()
		}
		wg.Wait()
	} else {
		var wg sync.WaitGroup
		addrs := config.Configuration.Addrs
		gatewayAddress := addrs[types.Shard(0)][identity.NewNodeID(0)]

		if *mode == "gateway" {
			log.Debugf("Gateway Start")
			gw := blockbuilder.NewInitGateway(gatewayAddress+"2999", types.Shard(0))
			gw.Start()
			log.Debugf("Gateway ip: %v", gw.GetIP())
		} else if *mode == "blockbuilder" {
			log.Debugf("Worker BlockBuilder Start")
			sh := *shard
			port := strconv.Itoa(3000 + sh)
			wbb := blockbuilder.NewInitWorkerBlockBuilder(addrs[types.Shard(sh)][identity.NewNodeID(0)]+port, types.Shard(sh), gatewayAddress+"2999")
			wbb.Start()
			log.Debugf("Worker BlockBuilder shard: %v, ip: %v", wbb.GetShard(), wbb.GetIP())
		} else if *mode == "node" {
			sh := *shard
			nodeID := *id
			isByz := false
			wg.Add(1)
			go initReplica(identity.NewNodeID(nodeID), isByz, types.Shard(sh), &wg)
			log.Debugf("[Worker Shard %v Init] Worker Node Setup Complete", sh)
			wg.Wait()
			log.Debugf("Setup Finish")
		}
	}
}
