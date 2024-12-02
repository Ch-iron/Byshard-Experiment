package gateway

import (
	"encoding/gob"
	"encoding/hex"
	"math"
	"os/exec"
	"reflect"
	"sync"
	"time"

	"paperexperiment/blockchain"
	"paperexperiment/config"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/node"
	"paperexperiment/transport"
	"paperexperiment/types"
	"paperexperiment/utils"

	"github.com/ethereum/go-ethereum/common"
)

type (
	Gateway struct {
		node.GatewayNode

		CommunicatorTopic                                                                 chan interface{}
		GatewayTopic                                                                      chan interface{}
		CommunicatorList                                                                  map[types.Shard]string
		CommunicatorTransports                                                            map[types.Shard]transport.Transport
		shardSharedVariableList                                                           map[types.Shard][]string
		contractRwSet                                                                     map[common.Address]map[string]types.RwSet
		sendedTransactionList                                                             map[types.Shard][]common.Hash
		clientStart                                                                       []types.Shard
		Experiment                                                                        map[common.Hash]int64
		flagForExperiment                                                                 map[types.Shard]bool
		savedExperimentTransactionResult                                                  []message.ExperimentTransactionResult
		savedExperiment                                                                   map[types.Shard]message.Experiment
		once                                                                              sync.Once
		cnt                                                                               int
		one_local, one_cross, two_local, two_cross, three_local, three_cross, total_cross int
	}
)

func NewInitGateway(ip string, shard types.Shard) *Gateway {
	gw := new(Gateway)
	gw.GatewayNode = node.NewGatewayNode(ip, shard)
	gw.CommunicatorTopic = make(chan interface{}, 128)
	gw.GatewayTopic = make(chan interface{}, 128)
	gw.CommunicatorList = make(map[types.Shard]string)
	gw.CommunicatorTransports = make(map[types.Shard]transport.Transport)
	gw.shardSharedVariableList = make(map[types.Shard][]string)
	gw.contractRwSet = make(map[common.Address]map[string]types.RwSet)
	gw.sendedTransactionList = make(map[types.Shard][]common.Hash)
	gw.Experiment = make(map[common.Hash]int64)
	gw.flagForExperiment = make(map[types.Shard]bool)
	gw.savedExperimentTransactionResult = make([]message.ExperimentTransactionResult, 0, 4)
	initExperimentResult := message.ExperimentTransactionResult{
		TotalTransaction: 0,
		CrossTransaction: 0,
		LocalTransaction: 0,
		RunningTime:      0,
	}
	for i := 0; i <= config.GetConfig().ShardCount; i++ {
		gw.savedExperimentTransactionResult = append(gw.savedExperimentTransactionResult, initExperimentResult)
	}
	gw.savedExperiment = make(map[types.Shard]message.Experiment)
	gw.cnt = 0
	gw.one_local, gw.one_cross, gw.two_local, gw.two_cross, gw.three_local, gw.three_cross, gw.total_cross = 0, 0, 0, 0, 0, 0, 0

	/* Register to gob en/decoder */
	gob.Register(message.ShardSharedVariableRegisterRequest{})
	gob.Register(message.CommunicatorRegister{})
	gob.Register(message.ShardSharedVariableRegisterResponse{})
	gob.Register(message.TransactionForm{})
	gob.Register(message.Transaction{})
	gob.Register(blockchain.ShardBlock{})
	gob.Register(message.ShardList{})
	gob.Register(message.ClientStart{})
	gob.Register(message.Experiment{})

	/* Register message handler */
	gw.Register(message.CommunicatorRegister{}, gw.handleCommunicatorRegister)
	gw.Register(message.ShardSharedVariableRegisterResponse{}, gw.handleShardShardVariableRegisterResponse)
	gw.Register(message.TransactionForm{}, gw.handleTransactionForm)
	gw.Register(blockchain.ShardBlock{}, gw.handleShardBlock)
	gw.Register(message.ClientStart{}, gw.handleClientStart)
	gw.Register(message.Experiment{}, gw.handleExperiment)

	return gw
}

func (gw *Gateway) Start() {
	var wg sync.WaitGroup
	go gw.GatewayNode.Run()

	for i := 1; i <= config.Configuration.ShardCount; i++ {
		travelnormalCAs, trainnormalCAs, hotelnormalCAs, travelcrossCAs, traincrossCAs, hotelcrossCAs, _ := config.GetCA(types.Shard(i))

		trainRwSet := config.GetRwSet("train")
		hotelRwSet := config.GetRwSet("hotel")
		travelRwSet := config.GetRwSet("travel")

		for _, ca := range travelnormalCAs {
			gw.contractRwSet[ca] = travelRwSet
		}
		for _, ca := range trainnormalCAs {
			gw.contractRwSet[ca] = trainRwSet
		}
		for _, ca := range hotelnormalCAs {
			gw.contractRwSet[ca] = hotelRwSet
		}
		for _, ca := range travelcrossCAs {
			gw.contractRwSet[ca] = travelRwSet
		}
		for _, ca := range traincrossCAs {
			gw.contractRwSet[ca] = trainRwSet
		}
		for _, ca := range hotelcrossCAs {
			gw.contractRwSet[ca] = hotelRwSet
		}
	}

	log.Debugf("start Gateway event loop")

	wg.Add(1)
	interval := time.NewTicker(time.Millisecond * 1000)
	go func() {
		for range interval.C {
			if gw.cnt >= config.GetConfig().Benchmark.N-100 {
				totalCrossShardLatency := int64(0)
				for _, latency := range gw.Experiment {
					totalCrossShardLatency += latency
				}
				avgCrossShardLatency := math.Round((float64(totalCrossShardLatency)/1000)/(float64(len(gw.Experiment)))*1000) / 1000
				log.Debugf("[Gateway] avg cross latency: %v, one local: %v, cross: %v, two local: %v, cross: %v, three local: %v, cross: %v, total cross: %v", avgCrossShardLatency, gw.one_local, gw.one_cross, gw.two_local, gw.two_cross, gw.three_local, gw.three_cross, gw.total_cross)
				systemTPS := 0.0
				sumOfTotalTPS := 0.0
				sumOfLocalLatency := 0.0
				sumOfCrossLatency := 0.0
				sumOfConsensusForISC := 0.0
				sumOfISC := 0.0
				sumOfConsensusForCommit := 0.0
				sumOfWaitingTime := 0.0
				sumOfLocalConsensus := 0.0
				sumOfLocalWaitingTime := 0.0
				sumOfLocalConsensusForCommit := 0.0
				for i := 1; i <= config.GetConfig().ShardCount; i++ {
					savedExperimentTransactionResult := gw.savedExperimentTransactionResult[types.Shard(i)]
					savedExperiment := gw.savedExperiment[types.Shard(i)]
					systemTPS += math.Round((float64(savedExperimentTransactionResult.TotalTransaction) / savedExperimentTransactionResult.RunningTime))
					sumOfTotalTPS += math.Round((float64(savedExperimentTransactionResult.TotalTransaction) / savedExperimentTransactionResult.RunningTime))
					sumOfLocalLatency += savedExperiment.LocalLatency
					sumOfCrossLatency += avgCrossShardLatency
					sumOfConsensusForISC += savedExperiment.ConsensusForISC
					sumOfISC += savedExperiment.ISC
					sumOfConsensusForCommit += savedExperiment.ConsensusForCommit
					sumOfWaitingTime += savedExperiment.WaitingTime
					sumOfLocalConsensus += savedExperiment.LocalConsensus
					sumOfLocalWaitingTime += savedExperiment.LocalWaitingTime
					sumOfLocalConsensusForCommit += savedExperiment.LocalConsensusForCommit
				}
				log.ExperimentResultf("%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f,%.3f",
					systemTPS,
					sumOfTotalTPS/float64(config.GetConfig().ShardCount),
					sumOfLocalLatency/float64(config.GetConfig().ShardCount),
					sumOfCrossLatency/float64(config.GetConfig().ShardCount),
					sumOfConsensusForISC/float64(config.GetConfig().ShardCount),
					sumOfISC/float64(config.GetConfig().ShardCount),
					sumOfConsensusForCommit/float64(config.GetConfig().ShardCount),
					sumOfWaitingTime/float64(config.GetConfig().ShardCount),
					sumOfLocalConsensus/float64(config.GetConfig().ShardCount),
					sumOfLocalWaitingTime/float64(config.GetConfig().ShardCount),
					sumOfLocalConsensusForCommit/float64(config.GetConfig().ShardCount),
				)
			}
		}
	}()
	go func() {
		for {
			select {
			case msg := <-gw.GatewayTopic:
				switch v := (msg).(type) {
				case message.CommunicatorRegister:
					gw.CommunicatorList[v.SenderShard] = v.Address
					log.Debugf("[Gateway] Received register request Communicator %v", v.SenderShard)

					// request to every shard communicator
					if _, exist := gw.CommunicatorTransports[v.SenderShard]; !exist {
						t := transport.NewTransport(v.Address)

						if err := t.Dial(); err != nil {
							panic(err)
						}
						gw.CommunicatorTransports[v.SenderShard] = t
					}

					gw.CommunicatorTransports[v.SenderShard].Send(message.ShardSharedVariableRegisterRequest{
						Gateway: gw.GetIP(),
					})
					log.Debugf("[Gateway] request shared variable to %v shard builder", v.SenderShard)
					log.Debugf("[Gateway] CommunicatorList: %v", gw.CommunicatorList)

				case message.ShardSharedVariableRegisterResponse:
					gw.shardSharedVariableList[v.SenderShard] = append(
						gw.shardSharedVariableList[v.SenderShard],
						v.Variables,
					)
					log.Debugf("[Gateway] received builder shard %v shared variable registering %v", v.SenderShard, v.Variables)

					if len(gw.CommunicatorList) == config.GetConfig().ShardCount {
						ShardList := make(message.ShardList, config.GetConfig().ShardCount)
						for shard, ip := range gw.CommunicatorList {
							ShardList[shard-1] = ip
						}
						for shard, tp := range gw.CommunicatorTransports {
							tp.Send(ShardList)
							log.Debugf("[Gateway] Send to %v shard, list %v", shard, ShardList)
						}
					}
				case message.ClientStart:
					gw.clientStart = append(gw.clientStart, v.Shard)
					log.Debugf("[Gateway] Receive ClientStart Message From Shard %v", v.Shard)
					if len(gw.clientStart) == config.GetConfig().ShardCount {
						cmd := exec.Command("sh", "-c", "./client 2> error.log &")
						if err := cmd.Run(); err != nil {
							log.Error("[Gateway] Error executing Client!!!")
						} else {
							log.Debug("[Gateway] Execute Client Success!!!")
						}
					}
				case message.Experiment:
					for _, crossShardLatency := range v.CrossShardTransactionLatency {
						if _, exist := gw.Experiment[crossShardLatency.Hash]; !exist {
							gw.Experiment[crossShardLatency.Hash] = crossShardLatency.Latency
						} else {
							if gw.Experiment[crossShardLatency.Hash] < crossShardLatency.Latency {
								gw.Experiment[crossShardLatency.Hash] = crossShardLatency.Latency
							}
						}
					}

					// fmt.Printf("Shard: %v, Total: %v, Cross: %v, Local: %v\n", v.Shard, v.ExperimentTransactionResult.TotalTransaction, v.ExperimentTransactionResult.CrossTransaction, v.ExperimentTransactionResult.LocalTransaction)
					gw.savedExperimentTransactionResult[v.Shard] = v.ExperimentTransactionResult
					gw.savedExperiment[v.Shard] = v
				case message.TransactionForm:
					switch {
					case len(v.Data) > 0:
						if v.To.String() == "0x0000000000000000000000000000000000000000" {
							// Handle Contract Deployment
						} else {
							// Handle Smart Contract Transaction
							addressList := []common.Address{v.To}
							addressList = append(addressList, v.ExternalAddressList...)
							shardToSend := utils.CalculateShardToSend(addressList)
							isCrossShardTx := len(shardToSend) > 1
							name := hex.EncodeToString(v.Data)
							rwSet := make([]message.RwVariable, 0)
							idx := 0
							rwSet = append(rwSet, message.RwVariable{Address: v.To, Name: name, RwSet: gw.contractRwSet[v.To][name]})
							// Handle Nested External Set
							for _, externalSet := range rwSet[0].ExternalSet {
								externalRwSet, curIdx := gw.getExternalRwSet(idx, v.ExternalAddressList, []types.ExternalElem{externalSet})
								idx = curIdx
								rwSet = append(rwSet, externalRwSet...)
							}
							for i := 0; i < len(rwSet); i++ {
								switch rwSet[i].Name {
								case "5710ddcd":
									// update rwSet for Travel contract bookTrainAndHotel function
									readSet, writeSet := gw.updateSlotForMapping(rwSet[i].RwSet.ReadSet, rwSet[i].RwSet.WriteSet, v.MappingIdx, "0")
									rwSet[i].RwSet.ReadSet = readSet
									rwSet[i].RwSet.WriteSet = writeSet
								}
							}

							tx := message.MakeSmartContractTransaction(v, rwSet, isCrossShardTx)
							gw.cnt++
							if len(shardToSend) == 1 {
								switch shardToSend[0] {
								case 1:
									gw.one_local++
								case 2:
									gw.two_local++
								case 3:
									gw.three_local++
								}
							} else if len(shardToSend) > 1 {
								gw.total_cross++
								for _, shard := range shardToSend {
									switch shard {
									case 1:
										gw.one_cross++
									case 2:
										gw.two_cross++
									case 3:
										gw.three_cross++
									}
								}
							}
							if isCrossShardTx {
								for idx, shard := range shardToSend {
									gw.sendedTransactionList[shard] = append(gw.sendedTransactionList[shard], tx.Hash)
									if idx == 0 {
										log.Debugf("[Gateway] %v Send Cross_Transaction Hash: %v, From: %v, To: %v, Value: %v, to shard %v, Rwset: %v", gw.cnt, tx.Hash, tx.From, tx.To, tx.Value, shardToSend[0], tx.RwSet)
									}
								}
							} else {
								gw.sendedTransactionList[shardToSend[0]] = append(gw.sendedTransactionList[shardToSend[0]], tx.Hash)
								log.Debugf("[Gateway] %v Send Local Transaction Hash: %v, From: %v, To: %v, Value: %v, to shard: %v, RwSet: %v", gw.cnt, tx.Hash, tx.From, tx.To, tx.Value, shardToSend[0], tx.RwSet)

							}
							gw.CommunicatorTransports[shardToSend[0]].Send(tx)
						}
					default:
						// Handle Transfer Transactions
						shardToSend := utils.CalculateShardToSend([]common.Address{v.From, v.To})
						isCrossShardTx := len(shardToSend) > 1
						tx := message.MakeTransferTransaction(v, isCrossShardTx)
						gw.cnt++
						if len(shardToSend) == 1 {
							switch shardToSend[0] {
							case 1:
								gw.one_local++
							case 2:
								gw.two_local++
							case 3:
								gw.three_local++
							}
						} else if len(shardToSend) > 1 {
							gw.total_cross++
							for _, shard := range shardToSend {
								switch shard {
								case 1:
									gw.one_cross++
								case 2:
									gw.two_cross++
								case 3:
									gw.three_cross++
								}
							}
						}
						if isCrossShardTx {
							for idx, shard := range shardToSend {
								gw.sendedTransactionList[shard] = append(gw.sendedTransactionList[shard], tx.Hash)
								if idx == 0 {
									log.Debugf("[Gateway] %v Send Cross_Transaction Hash: %v, From: %v, To: %v, Value: %v, to shard %v, Rwset: %v", gw.cnt, tx.Hash, tx.From, tx.To, tx.Value, shardToSend[0], tx.RwSet)
								}
							}
						} else {
							gw.sendedTransactionList[shardToSend[0]] = append(gw.sendedTransactionList[shardToSend[0]], tx.Hash)
							log.Debugf("[Gateway] %v Send Local Transaction Hash: %v, From: %v, To: %v, Value: %v, to shard %v", gw.cnt, tx.Hash, tx.From, tx.To, tx.Value, shardToSend[0])
						}
						gw.CommunicatorTransports[shardToSend[0]].Send(tx)
					}
				case blockchain.ShardBlock:
					for _, transaction := range v.Transaction {
						for index, sendedTrasnasction := range gw.sendedTransactionList[types.Shard(v.Shard)] {
							if sendedTrasnasction == transaction.Hash {
								gw.sendedTransactionList[types.Shard(v.Shard)] = append(gw.sendedTransactionList[types.Shard(v.Shard)][:index], gw.sendedTransactionList[types.Shard(v.Shard)][index+1:]...)
							}
						}
					}
					block_height := v.Block_header.Block_height
					number_of_transactions := len(v.Transaction)
					remaining_transaction := len(gw.sendedTransactionList[v.Shard])
					log.Debugf("Shard %v BlockHeight %v Number of Transaction %v, received, Remaining transaction number %v", v.Shard, block_height, number_of_transactions, remaining_transaction)
					// if remaining_transaction != 0 {
					// 	for _, txHash := range gw.sendedTransactionList[v.Shard] {
					// 		log.Debugf("Shard %v remaining %v", v.Shard, txHash.String())
					// 	}
					// }
				default:
					log.Debugf("illegal message type : %s", reflect.TypeOf(v).String())
				}
				gw.once.Do(func() {
					log.ExperimentResultf("SystemTPS,TotalTPS,LocalLatency,CrossLatency,ConsensusForISC,ISC,ConsensusForCommit,WaitingTime,LocalConsensus,LocalWaitingTime,LocalConsensusForCommit")
				})

			// drop another topic
			case <-gw.CommunicatorTopic:
				continue
			}
		}
	}()
	wg.Wait()
}

func (gw *Gateway) handleCommunicatorRegister(msg message.CommunicatorRegister) {
	// C -> G
	gw.GatewayTopic <- msg
}

func (gw *Gateway) handleShardShardVariableRegisterResponse(msg message.ShardSharedVariableRegisterResponse) {
	// W -> G
	gw.GatewayTopic <- msg
}

func (gw *Gateway) handleTransactionForm(msg message.TransactionForm) {
	gw.GatewayTopic <- msg
	// log.Debugf("Received TransactionForm Message: %v", msg)
}

func (gw *Gateway) handleShardBlock(msg blockchain.ShardBlock) {
	// log.Debugf("Received ShardBlock: %v", msg)
	gw.GatewayTopic <- msg
}

func (gw *Gateway) handleClientStart(msg message.ClientStart) {
	gw.GatewayTopic <- msg
}

func (gw *Gateway) handleExperiment(msg message.Experiment) {
	gw.GatewayTopic <- msg
}

func (gw *Gateway) getExternalRwSet(idx int, externalAddressList []common.Address, externalSet []types.ExternalElem) ([]message.RwVariable, int) {
	rwSet := make([]message.RwVariable, 0)
	curIdx := idx
	for _, externalElem := range externalSet {
		if externalElem.ElemType == "read" {
			rwSet = append(rwSet, message.RwVariable{
				Address: externalAddressList[idx],
				Name:    externalElem.Name,
				RwSet:   types.RwSet{ReadSet: []string{externalElem.Name}, WriteSet: []string{}},
			})
			curIdx++
		} else if externalElem.ElemType == "execute" {
			rwSet = append(rwSet, message.RwVariable{
				Address: externalAddressList[idx],
				Name:    externalElem.Name,
				RwSet:   gw.contractRwSet[externalAddressList[idx]][externalElem.Name],
			})
			curIdx++
			if len(gw.contractRwSet[externalAddressList[idx]][externalElem.Name].ExternalSet) > 0 {
				externalRwSet, _curIdx := gw.getExternalRwSet(curIdx, externalAddressList, gw.contractRwSet[externalAddressList[idx]][externalElem.Name].ExternalSet)
				rwSet = append(rwSet, externalRwSet...)
				curIdx = _curIdx
			}
		}
	}
	return rwSet, curIdx
}

func (gw *Gateway) updateSlotForMapping(readSet []string, writeSet []string, idx []byte, targetIdx string) ([]string, []string) {
	tempReadSet := []string{}
	tempWriteSet := []string{}

	for _, elem := range readSet {
		if elem == targetIdx {
			tempReadSet = append(tempReadSet, hex.EncodeToString(idx))
		} else {
			tempReadSet = append(tempReadSet, elem)
		}
	}
	for _, elem := range writeSet {
		if elem == targetIdx {
			tempWriteSet = append(tempWriteSet, hex.EncodeToString(idx))
		} else {
			tempWriteSet = append(tempWriteSet, elem)
		}
	}
	return tempReadSet, tempWriteSet
}
