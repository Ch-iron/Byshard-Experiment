package communicator

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
	Communicator struct {
		CommunicatorReplica
		gatewayAddress         string
		gatewayTransport       transport.Transport
		consensusNodeTransport map[identity.NodeID]transport.Transport
		nodestart              []identity.NodeID
		sharedVariableList     string
		ShardList              message.ShardList
		communicatorTransport  []transport.Transport
	}
)

func NewInitCommunicator(ip string, shard types.Shard, gatewayAddress string) *Communicator {
	comm := Communicator{
		CommunicatorReplica:    *NewCommunicator(ip, shard),
		gatewayAddress:         gatewayAddress,
		gatewayTransport:       transport.NewTransport(gatewayAddress),
		consensusNodeTransport: make(map[identity.NodeID]transport.Transport),
		sharedVariableList:     "Alive",
		ShardList:              make(message.ShardList, config.GetConfig().ShardCount),
		communicatorTransport:  make([]transport.Transport, config.GetConfig().ShardCount),
	}

	return &comm
}

func (comm *Communicator) Start() {
	var wg sync.WaitGroup
	go comm.CommunicatorReplica.Start()

	err := utils.Retry(comm.gatewayTransport.Dial, 100, time.Duration(50)*time.Millisecond)
	if err != nil {
		panic(err)
	}

	comm.gatewayTransport.Send(message.CommunicatorRegister{
		SenderShard: comm.GetShard(),
		Address:     comm.GetIP(),
	})
	log.Debugf("Register to Gateway")

	log.Debugf("start communicator event loop")
	wg.Add(1)
	go func() {
		for {
			select {
			case msg := <-comm.CommunicatorReplica.CommunicatorTopic:
				switch v := (msg).(type) {
				case message.ShardSharedVariableRegisterRequest:
					comm.gatewayAddress = v.Gateway
					comm.gatewayTransport = transport.NewTransport(v.Gateway)
					err := utils.Retry(comm.gatewayTransport.Dial, 100, time.Duration(50)*time.Millisecond)
					if err != nil {
						panic(err)
					}
					comm.gatewayTransport.Send(message.ShardSharedVariableRegisterResponse{
						SenderShard: comm.GetShard(),
						Variables:   comm.sharedVariableList,
					})
					log.Debugf("[Communicator %v] Received request of my shared variable from Gateway %s", comm.GetShard(), v.Gateway)
				case message.ShardList:
					copy(comm.ShardList, v)
					for shard, ip := range comm.ShardList {
						comm.communicatorTransport[shard] = transport.NewTransport(ip)
						err := utils.Retry(comm.communicatorTransport[shard].Dial, 100, time.Duration(50)*time.Millisecond)
						if err != nil {
							panic(err)
						}
					}
					log.Debugf("[Communicator %v] Complete Register All Communicator List %v", comm.GetShard(), comm.ShardList)
				case message.Experiment:
					comm.gatewayTransport.Send(v)
				default:
					log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
				}
			case msg := <-comm.CommunicatorReplica.ConsensusNodeTopic:
				switch v := (msg).(type) {
				case message.ConsensusNodeRegister:
					t := transport.NewTransport(v.IP)
					err := utils.Retry(t.Dial, 100, time.Duration(50)*time.Millisecond)
					if err != nil {
						panic(err)
					}
					comm.consensusNodeTransport[v.ConsensusNodeID] = t
					log.Debugf("[Communicator %v] Consensus ID: %v, IP: %v Register Complete!! Length: %v", comm.GetShard(), v.ConsensusNodeID, v.IP, len(comm.consensusNodeTransport))
					if len(comm.consensusNodeTransport) == config.GetConfig().CommitteeNumber {
						comm.SendNodeStartMessage()
					}
				case message.NodeStartAck:
					comm.nodestart = append(comm.nodestart, v.ID)
					log.Debugf("[Communicator %v] Node ID: %v Node Start Complete!!", comm.GetShard(), v.ID)
					if len(comm.nodestart) == config.GetConfig().CommitteeNumber {
						log.Debugf("[Communicator %v] Send ClientStart Message To Gateway", comm.GetShard())
						comm.gatewayTransport.Send(message.ClientStart{
							Shard: comm.GetShard(),
						})
					}
				}
			// drop another topic
			case <-comm.GatewayTopic:
				continue
			}
		}
	}()
	go func() {
		for {
			msg := <-comm.CommunicatorReplica.RootShardVotedTransaction
			switch v := (msg).(type) {
			case message.RootShardVotedTransaction:
				if comm.IsRootShard(&v.Transaction) {
					var associate_shards []types.Shard
					associate_addresses := []common.Address{}
					if v.Transaction.TXType == types.TRANSFER {
						associate_addresses = append(associate_addresses, v.Transaction.From)
						associate_addresses = append(associate_addresses, v.Transaction.To)
					} else if v.Transaction.TXType == types.SMARTCONTRACT {
						associate_addresses = append(associate_addresses, v.ExternalAddressList...)
					}
					associate_shards = utils.CalculateShardToSend(associate_addresses)
					comm.AddProcessingTransaction(&v.Transaction, len(associate_shards)-1)
					v.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli() - v.AbortAndRetryLatencyDissection.RootVoteConsensusTime
					v.LatencyDissection.RootVoteConsensusTime = v.LatencyDissection.RootVoteConsensusTime + v.AbortAndRetryLatencyDissection.RootVoteConsensusTime
					v.AbortAndRetryLatencyDissection.Network1 = time.Now().UnixMilli()
					for _, shard := range associate_shards {
						if shard != comm.GetShard() {
							comm.communicatorTransport[shard-1].Send(v.Transaction)
							log.Debugf("[Communicator %v] Send Cross Transaction to associated shard %v Hash: %v", comm.GetShard(), shard, v.Hash)
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
					latencyTimer := utils.GetBetweenShardTimer(comm.GetShard(), root_shard[0])
					go comm.communicatorTransport[root_shard[0]-1].LatencySend(votedtransaction, latencyTimer)
					// go comm.communicatorTransport[root_shard[0]-1].Send(votedtransaction)
					log.Debugf("[Communicator %v] Send Cross Transaction to root shard %v %v", comm.GetShard(), root_shard[0], v.Hash)
				}
			default:
				log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
			}
		}
	}()
	go func() {
		for {
			msg := <-comm.CommunicatorReplica.CommittedTransaction
			switch v := (msg).(type) {
			case message.CommittedTransaction:
				log.Debugf("[Communicator %v] Receive Committed Transaction to consensus node Hash: %v", comm.GetShard(), v.Hash)
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
					if shard != comm.GetShard() {
						comm.communicatorTransport[shard-1].Send(v)
						log.Debugf("[Communicator %v] Send Committed Transaction to associated shard to shard %v Hash: %v", comm.GetShard(), shard, v.Hash)
					}
				}
			default:
				log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
			}
		}
	}()
	wg.Wait()
}
