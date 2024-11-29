package main

import (
	"flag"
	"strconv"
	"sync"
	"time"

	"paperexperiment"
	"paperexperiment/communicator"
	"paperexperiment/config"
	"paperexperiment/crypto"
	"paperexperiment/gateway"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/replica"
	"paperexperiment/types"

	"net/http"
	_ "net/http/pprof"
)

var (
	algorithm  = flag.String("algorithm", "pbft", "BFT consensus algorithm")
	id         = flag.Int("id", 0, "NodeID of the node")
	simulation = flag.Bool("sim", true, "simulation mode")
	mode       = flag.String("mode", "gateway", "Select node or communicatior shard") // node, communicator, gateway
	shard      = flag.Int("shard", 0, "shard that node belongs to (only available on mode is communicator or node)")
)

func initReplica(id identity.NodeID, isByz bool, shard types.Shard, wg *sync.WaitGroup) {
	if isByz {
		log.Infof("node %v is Byzantine", id)
	}
	switch *algorithm {
	case "pbft":
		log.Info("Consensus Algorithm: PBFT")
		log.Debugf("Shard %v node %v start...", shard, id)
		r := replica.NewReplica(id, *algorithm, isByz, shard)
		r.Start(wg)

	default:
		r := replica.NewReplica(id, *algorithm, isByz, shard)
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
		gw := gateway.NewInitGateway("tcp://127.0.0.1:2999", types.Shard(0))
		gw.Start()
		log.Debugf("Gateway ip: %v", gw.GetIP())

		time.Sleep(1 * time.Second / 2)

		log.Debugf("[communicator Init] Init communicator & Gateway Complete")

		log.Debugf("Shard communicator Start")
		communicators := make([]*communicator.Communicator, 0)
		for i := 1; i <= config.GetConfig().ShardCount; i++ {
			port := strconv.Itoa(3000 + i)
			comm := communicator.NewInitCommunicator("tcp://127.0.0.1:"+port, types.Shard(i), "tcp://127.0.0.1:2999")
			communicators = append(communicators, comm)
			comm.Start()
			log.Debugf("Shard communicator shard: %v, ip: %v", comm.GetShard(), comm.GetIP())
		}

		// Shard consensus Node
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
			log.Debugf("[Shard Node %v Init] Shard Node Setup Complete", shard)
		}
		wg.Wait()
		log.Debugf("Setup Finish")

		time.Sleep(1 * time.Second)

		wg.Add(1)
		for _, communicator := range communicators {
			go communicator.CommunicatorReplica.SendNodeStartMessage()
		}
		wg.Wait()
	} else {
		var wg sync.WaitGroup
		addrs := config.Configuration.Addrs
		gatewayAddress := addrs[types.Shard(0)][identity.NewNodeID(0)]

		if *mode == "gateway" {
			log.Debugf("Gateway Start")
			gw := gateway.NewInitGateway(gatewayAddress+"2999", types.Shard(0))
			gw.Start()
			log.Debugf("Gateway ip: %v", gw.GetIP())
		} else if *mode == "communicator" {
			log.Debugf("Communicator Start")
			sh := *shard
			port := strconv.Itoa(3000 + sh)
			comm := communicator.NewInitCommunicator(addrs[types.Shard(sh)][identity.NewNodeID(0)]+port, types.Shard(sh), gatewayAddress+"2999")
			comm.Start()
			log.Debugf("Communicator shard: %v, ip: %v", comm.GetShard(), comm.GetIP())
		} else if *mode == "node" {
			sh := *shard
			nodeID := *id
			isByz := false
			wg.Add(1)
			go initReplica(identity.NewNodeID(nodeID), isByz, types.Shard(sh), &wg)
			log.Debugf("[Shard Node %v Init] Worker Node Setup Complete", sh)
			wg.Wait()
			log.Debugf("Setup Finish")
		}
	}
}
