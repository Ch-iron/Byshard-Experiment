package blockbuilder

import (
	"reflect"
	"sync"
	"time"

	"paperexperiment/config"
	"paperexperiment/identity"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/transport"
	"paperexperiment/types"
	"paperexperiment/utils"

	"github.com/ethereum/go-ethereum/common"
)

type (
	WorkerBlockBuilder struct {
		WorkerBlockBuilderReplica
		gatewayAddress         string
		gatewayTransport       transport.Transport
		consensusNodeTransport map[identity.NodeID]transport.Transport
		nodestart              []identity.NodeID
		sharedVariableList     string
		workerList             message.WorkerList
		workerTransport        []transport.Transport
	}
)

func NewInitWorkerBlockBuilder(ip string, shard types.Shard, gatewayAddress string) *WorkerBlockBuilder {
	wbb := WorkerBlockBuilder{
		WorkerBlockBuilderReplica: *NewWorkerBlockBuilder(ip, shard),
		gatewayAddress:            gatewayAddress,
		gatewayTransport:          transport.NewTransport(gatewayAddress),
		consensusNodeTransport:    make(map[identity.NodeID]transport.Transport),
		sharedVariableList:        "Alive",
		workerList:                make(message.WorkerList, config.GetConfig().ShardCount),
		workerTransport:           make([]transport.Transport, config.GetConfig().ShardCount),
	}

	return &wbb
}

func (wbb *WorkerBlockBuilder) Start() {
	var wg sync.WaitGroup
	go wbb.WorkerBlockBuilderReplica.Start()

	err := utils.Retry(wbb.gatewayTransport.Dial, 100, time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}

	wbb.gatewayTransport.Send(message.WorkerBuilderRegister{
		SenderShard: wbb.GetShard(),
		Address:     wbb.GetIP(),
	})
	log.Debugf("Register to Gateway")

	log.Debugf("start worker builder event loop")
	wg.Add(1)
	go func() {
		for {
			select {
			case msg := <-wbb.WorkerBlockBuilderReplica.WorkerBuilderTopic:
				switch v := (msg).(type) {
				case message.WorkerSharedVariableRegisterRequest:
					wbb.gatewayAddress = v.Gateway
					wbb.gatewayTransport = transport.NewTransport(v.Gateway)
					err := utils.Retry(wbb.gatewayTransport.Dial, 100, time.Duration(50)*time.Millisecond)
					if err != nil {
						panic(err)
					}
					wbb.gatewayTransport.Send(message.WorkerSharedVariableRegisterResponse{
						SenderShard: wbb.GetShard(),
						Variables:   wbb.sharedVariableList,
					})
					log.Debugf("[WorkerBlockbuilder %v] Received request of my shared variable from Gateway %s", wbb.GetShard(), v.Gateway)
				case message.WorkerList:
					copy(wbb.workerList, v)
					for shard, ip := range wbb.workerList {
						wbb.workerTransport[shard] = transport.NewTransport(ip)
						err := utils.Retry(wbb.workerTransport[shard].Dial, 100, time.Duration(50)*time.Millisecond)
						if err != nil {
							panic(err)
						}
					}
					log.Debugf("[WorkerBlockbuilder %v] Complete Register All Worker BlockBuilder List %v", wbb.GetShard(), wbb.workerList)
				case message.Experiment:
					wbb.gatewayTransport.Send(v)
				default:
					log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
				}
			case msg := <-wbb.WorkerBlockBuilderReplica.ConsensusNodeTopic:
				switch v := (msg).(type) {
				case message.ConsensusNodeRegister:
					t := transport.NewTransport(v.IP)
					err := utils.Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
					if err != nil {
						panic(err)
					}
					wbb.consensusNodeTransport[v.ConsensusNodeID] = t
					log.Debugf("[WorkerBlockbuilder %v] Consensus ID: %v, IP: %v Register Complete!! Length: %v", wbb.GetShard(), v.ConsensusNodeID, v.IP, len(wbb.consensusNodeTransport))
					if len(wbb.consensusNodeTransport) == config.GetConfig().CommitteeNumber {
						wbb.SendNodeStartMessage()
					}
				case message.NodeStartAck:
					wbb.nodestart = append(wbb.nodestart, v.ID)
					log.Debugf("[WorkerBlockbuilder %v] Node ID: %v Node Start Complete!!", wbb.GetShard(), v.ID)
					if len(wbb.nodestart) == config.GetConfig().CommitteeNumber {
						log.Debugf("[WorkerBlockbuilder %v] Send ClientStart Message To Gateway", wbb.GetShard())
						wbb.gatewayTransport.Send(message.ClientStart{
							Shard: wbb.GetShard(),
						})
					}
				}
			// drop another topic
			case <-wbb.GatewayTopic:
				continue
			}
		}
	}()
	go func() {
		for {
			msg := <-wbb.WorkerBlockBuilderReplica.RootShardVotedTransaction
			switch v := (msg).(type) {
			case message.RootShardVotedTransaction:
				if wbb.IsRootShard(&v.Transaction) {
					var associate_shards []types.Shard
					associate_addresses := []common.Address{}
					if v.Transaction.TXType == types.TRANSFER {
						associate_addresses = append(associate_addresses, v.Transaction.From)
						associate_addresses = append(associate_addresses, v.Transaction.To)
					} else if v.Transaction.TXType == types.SMARTCONTRACT {
						associate_addresses = append(associate_addresses, v.ExternalAddressList...)
					}
					associate_shards = utils.CalculateShardToSend(associate_addresses)
					wbb.AddProcessingTransaction(&v.Transaction, len(associate_shards)-1)
					v.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli() - v.AbortAndRetryLatencyDissection.RootVoteConsensusTime
					v.LatencyDissection.RootVoteConsensusTime = v.LatencyDissection.RootVoteConsensusTime + v.AbortAndRetryLatencyDissection.RootVoteConsensusTime
					v.AbortAndRetryLatencyDissection.Network1 = time.Now().UnixMilli()
					for _, shard := range associate_shards {
						if shard != wbb.GetShard() {
							wbb.workerTransport[shard-1].Send(v.Transaction)
							log.Debugf("[WorkerBlockbuilder %v] Send Cross Transaction to associated shard %v Hash: %v", wbb.GetShard(), shard, v.Hash)
						}
					}
				} else {
					var root_shard []types.Shard
					root_address := []common.Address{}
					if v.Transaction.TXType == types.TRANSFER {
						root_address = append(root_address, v.Transaction.From)
					} else if v.Transaction.TXType == types.SMARTCONTRACT {
						root_address = append(root_address, v.Transaction.To)
					}
					root_shard = utils.CalculateShardToSend(root_address)
					votedtransaction := message.VotedTransaction{
						RootShardVotedTransaction: v,
					}
					votedtransaction.AbortAndRetryLatencyDissection.VoteConsensusTime = time.Now().UnixMilli() - votedtransaction.AbortAndRetryLatencyDissection.VoteConsensusTime
					votedtransaction.LatencyDissection.VoteConsensusTime = votedtransaction.LatencyDissection.VoteConsensusTime + votedtransaction.AbortAndRetryLatencyDissection.VoteConsensusTime
					votedtransaction.AbortAndRetryLatencyDissection.Network2 = time.Now().UnixMilli()
					latencyTimer := utils.GetBetweenShardTimer(wbb.GetShard(), root_shard[0])
					go wbb.workerTransport[root_shard[0]-1].LatencySend(votedtransaction, latencyTimer)
					// go wbb.workerTransport[root_shard[0]-1].Send(votedtransaction)
					log.Debugf("[WorkerBlockbuilder %v] Send Cross Transaction to root shard %v %v", wbb.GetShard(), root_shard[0], v.Hash)
				}
			default:
				log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
			}
		}
	}()
	go func() {
		for {
			msg := <-wbb.WorkerBlockBuilderReplica.CommittedTransaction
			switch v := (msg).(type) {
			case message.CommittedTransaction:
				log.Debugf("[WorkerBlockbuilder %v] Receive Committed Transaction to consensus node Hash: %v", wbb.GetShard(), v.Hash)
				var associate_shards []types.Shard
				associate_addresses := []common.Address{}
				if v.Transaction.TXType == types.TRANSFER {
					associate_addresses = append(associate_addresses, v.From)
					associate_addresses = append(associate_addresses, v.To)
				} else if v.Transaction.TXType == types.SMARTCONTRACT {
					associate_addresses = append(associate_addresses, v.ExternalAddressList...)
				}
				associate_shards = utils.CalculateShardToSend(associate_addresses)
				for _, shard := range associate_shards {
					if shard != wbb.GetShard() {
						wbb.workerTransport[shard-1].Send(v)
						log.Debugf("[WorkerBlockbuilder %v] Send Committed Transaction to associated shard to shard %v Hash: %v", wbb.GetShard(), shard, v.Hash)
					}
				}
			default:
				log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
			}
		}
	}()
	wg.Wait()
}
