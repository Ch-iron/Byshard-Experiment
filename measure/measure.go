package measure

import (
	"math"
	"paperexperiment/blockchain"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/types"
	"paperexperiment/utils"
	"time"

	"github.com/ethereum/go-ethereum/common"
)

// Transaction Ratio
// 100:0 -> 350, 650, 0, 0
// 90:10 -> 315, 585, 35, 65
// 80:20 -> 280, 520, 70, 130
// 70:30 -> 245, 455, 105, 195

type PBFTMeasure struct {
	ConflictTransaction         map[common.Hash]types.BlockHeight
	StartTime                   time.Time
	TotalCommittedTx            int
	TotalCommittedLocalTx       int
	TotalCommittedCrossTx       int
	TotalLocalLatency           int64
	TotalCrossLatency           int64
	TotalRootVoteConsensusTime  int64
	TotalNetwork1               int64
	TotalRootVoteToDecideTime   int64
	TotalVoteConsensusTime      int64
	TotalNetwork2               int64
	TotalDecideConsensusTime    int64
	TotalNetwork3               int64
	TotalCommitConsensusTime    int64
	TotalExecutionTime          int64
	TotalBlockWaitingtime       int64
	TotalCommitToBlockTime      int64
	TotalCrossProcessTime       int64
	TotalLocalConsensusTime     int64
	TotalLocalBlockWaitingTime  int64
	TotalLocalCommitToBlockTime int64
	TotalLocalProcessTime       int64
}

func NewPBFTMeasure() *PBFTMeasure {
	return &PBFTMeasure{
		ConflictTransaction:         make(map[common.Hash]types.BlockHeight),
		StartTime:                   time.Time{},
		TotalCommittedTx:            0,
		TotalCommittedLocalTx:       0,
		TotalCommittedCrossTx:       0,
		TotalLocalLatency:           0,
		TotalCrossLatency:           0,
		TotalRootVoteConsensusTime:  0,
		TotalNetwork1:               0,
		TotalRootVoteToDecideTime:   0,
		TotalVoteConsensusTime:      0,
		TotalNetwork2:               0,
		TotalDecideConsensusTime:    0,
		TotalNetwork3:               0,
		TotalCommitConsensusTime:    0,
		TotalExecutionTime:          0,
		TotalBlockWaitingtime:       0,
		TotalCommitToBlockTime:      0,
		TotalCrossProcessTime:       0,
		TotalLocalConsensusTime:     0,
		TotalLocalBlockWaitingTime:  0,
		TotalLocalCommitToBlockTime: 0,
		TotalLocalProcessTime:       0,
	}
}

func IsExist(address common.Address, shard types.Shard) bool {
	var addressList []common.Address
	addressList = append(addressList, address)
	if utils.CalculateShardToSend(addressList)[0] == shard {
		return true
	} else {
		return false
	}
}

func (measure *PBFTMeasure) CalculateMeasurements(curShardBlockBlock *blockchain.ShardBlock, shard types.Shard) message.Experiment {
	crossShardTransactionLatency := make([]*message.CrossShardTransactionLatency, 0)

	// calculate local latency and latency dissection
	for _, tx := range curShardBlockBlock.Transaction {
		switch tx.IsCrossShardTx {
		case true:
			tx.LatencyDissection.CommitToBlockTime = time.Now().UnixMilli() - tx.LatencyDissection.CommitToBlockTime
			rootvoteconsensustime, network1, voteconsensustime, network2, decideconsensustime, network3, commitconsensustime, blockwaitingtime, committoblocktime := CalculateLatencyDissection(tx)
			if IsExist(tx.To, shard) {
				measure.TotalCommittedTx++
				measure.TotalCommittedCrossTx++
				measure.TotalRootVoteConsensusTime += rootvoteconsensustime
				measure.TotalNetwork1 += network1
				measure.TotalVoteConsensusTime += voteconsensustime
				measure.TotalNetwork2 += network2
				measure.TotalDecideConsensusTime += decideconsensustime
				measure.TotalNetwork3 += network3
				measure.TotalCommitConsensusTime += commitconsensustime
				measure.TotalBlockWaitingtime += blockwaitingtime
				measure.TotalCommitToBlockTime += committoblocktime
				crosslatency := time.Now().UnixMilli() - tx.Timestamp
				crossShardTransactionLatency = append(crossShardTransactionLatency, &message.CrossShardTransactionLatency{
					Hash:    tx.Hash,
					Latency: crosslatency,
				})
				measure.TotalCrossLatency += crosslatency
				measure.TotalCrossProcessTime += tx.LatencyDissection.ProcessTime
			}
			// log.PerformanceInfof("Network1: %v, Network2: %v, Network3: %v", network1, network2, network3)
		case false:
			tx.LocalLatencyDissection.CommitToBlockTime = time.Now().UnixMilli() - tx.LocalLatencyDissection.CommitToBlockTime
			measure.TotalCommittedTx++
			measure.TotalCommittedLocalTx++
			measure.TotalLocalConsensusTime += tx.LocalLatencyDissection.ConsensusTime
			measure.TotalLocalBlockWaitingTime += tx.LocalLatencyDissection.BlockWaitingTime
			measure.TotalLocalCommitToBlockTime += tx.LocalLatencyDissection.CommitToBlockTime
			locallatency := time.Now().UnixMilli() - tx.Timestamp
			measure.TotalLocalLatency += locallatency
			measure.TotalLocalProcessTime += tx.LocalLatencyDissection.ProcessTime
		}
	}

	// calculate TPS
	totalTPS := math.Round((float64(measure.TotalCommittedTx) / time.Since(measure.StartTime).Seconds()))
	localTPS := math.Round((float64(measure.TotalCommittedLocalTx) / time.Since(measure.StartTime).Seconds()))
	crossTPS := math.Round((float64(measure.TotalCommittedCrossTx) / time.Since(measure.StartTime).Seconds()))
	totalConflict := len(measure.ConflictTransaction)

	// calculate average latency
	avgLocalLatency := CalculateAverageTimeDifference(measure.TotalLocalLatency, measure.TotalCommittedLocalTx)
	avgLocalConsensusTime := CalculateAverageTimeDifference(measure.TotalLocalConsensusTime, measure.TotalCommittedLocalTx)
	avgLocalBlockWaitingTime := CalculateAverageTimeDifference(measure.TotalLocalBlockWaitingTime, measure.TotalCommittedLocalTx)
	avgLocalCommitToBlockTime := CalculateAverageTimeDifference(measure.TotalLocalCommitToBlockTime, measure.TotalCommittedLocalTx)
	avgLocalProcessTime := CalculateAverageTimeDifference(measure.TotalLocalProcessTime, measure.TotalCommittedLocalTx)
	localTotal := avgLocalConsensusTime + avgLocalBlockWaitingTime + avgLocalCommitToBlockTime
	avgCrossLatency := CalculateAverageTimeDifference(measure.TotalCrossLatency, measure.TotalCommittedCrossTx)
	avgRootVoteConsensusTime := CalculateAverageTimeDifference(measure.TotalRootVoteConsensusTime, measure.TotalCommittedCrossTx)
	avgNetwork1 := CalculateAverageTimeDifference(measure.TotalNetwork1, measure.TotalCommittedCrossTx)
	avgVoteConsensusTime := CalculateAverageTimeDifference(measure.TotalVoteConsensusTime, measure.TotalCommittedCrossTx)
	avgNetwork2 := CalculateAverageTimeDifference(measure.TotalNetwork2, measure.TotalCommittedCrossTx)
	avgDecideConsensusTime := CalculateAverageTimeDifference(measure.TotalDecideConsensusTime, measure.TotalCommittedCrossTx)
	avgNetwork3 := CalculateAverageTimeDifference(measure.TotalNetwork3, measure.TotalCommittedCrossTx)
	avgCommitConsensusTime := CalculateAverageTimeDifference(measure.TotalCommitConsensusTime, measure.TotalCommittedCrossTx)
	avgBlockWaitingTime := CalculateAverageTimeDifference(measure.TotalBlockWaitingtime, measure.TotalCommittedCrossTx)
	avgCommitToBlockTime := CalculateAverageTimeDifference(measure.TotalCommitToBlockTime, measure.TotalCommittedCrossTx)
	avgCrossProcessTime := CalculateAverageTimeDifference(measure.TotalCrossProcessTime, measure.TotalCommittedCrossTx)

	consensusForISC := avgRootVoteConsensusTime + avgVoteConsensusTime + avgDecideConsensusTime + avgCommitConsensusTime
	ISC := avgNetwork1 + avgNetwork2 + avgNetwork3
	consensusForCommit := avgCommitToBlockTime
	waitingTime := avgBlockWaitingTime
	total := consensusForISC + ISC + consensusForCommit + waitingTime

	log.ShardStatisticf("[Shard %v][Overview] total TPS: %v, local TPS: %v, cross TPS: %v, local process latency: %v, cross process latency: %v, local commit latency: %v, cross commit latency: %v, total conflict tx: %v, total committed tx: %v, committed tx num: %v", shard, totalTPS, localTPS, crossTPS, avgLocalProcessTime, avgCrossProcessTime, avgLocalLatency, avgCrossLatency, totalConflict, measure.TotalCommittedTx, len(curShardBlockBlock.Transaction))
	log.ShardStatisticf("[Shard %v][Local Dissection] LocalConsensus: %.3f, LocalWaitingTime: %.3f, LocalConsensusForCommit: %.3f, localTotal: %.3f", shard, avgLocalConsensusTime, avgLocalBlockWaitingTime, avgLocalCommitToBlockTime, localTotal)
	log.ShardStatisticf("[Shard %v][Cross Dissection] ConsensusForISC: %.3f, ISC: %.3f, ConsensusForCommit: %.3f, WaitingTime: %.3f, Total: %.3f", shard, consensusForISC, ISC, consensusForCommit, waitingTime, total)

	experiment := message.Experiment{}
	experiment.CrossShardTransactionLatency = crossShardTransactionLatency
	if time.Since(measure.StartTime).Seconds() >= 0 && !measure.StartTime.IsZero() {
		experimentTransactionResult := message.ExperimentTransactionResult{
			TotalTransaction: measure.TotalCommittedTx,
			LocalTransaction: measure.TotalCommittedLocalTx,
			CrossTransaction: measure.TotalCommittedCrossTx,
			RunningTime:      time.Since(measure.StartTime).Seconds(),
		}
		experiment = message.Experiment{
			Shard:                        shard,
			CrossShardTransactionLatency: crossShardTransactionLatency,
			ExperimentTransactionResult:  experimentTransactionResult,
			LocalLatency:                 avgLocalLatency,
			ConsensusForISC:              consensusForISC,
			ISC:                          ISC,
			ConsensusForCommit:           consensusForCommit,
			WaitingTime:                  waitingTime,
			LocalConsensus:               avgLocalConsensusTime,
			LocalWaitingTime:             avgLocalBlockWaitingTime,
			LocalConsensusForCommit:      avgLocalCommitToBlockTime,
			Total:                        total,
		}
	}

	return experiment
}

func CalculateAverageTimeDifference(total int64, num int) float64 {
	return math.Round((float64(total)/1000)/float64(num)*1000) / 1000
}

func CalculateLatencyDissection(tx *message.Transaction) (rootvoteconsensustime, network1, voteconsensustime, network2, decideconsensustime, network3, commitconsensustime, blockwaitingtime, committoblocktime int64) {
	return tx.LatencyDissection.RootVoteConsensusTime,
		tx.LatencyDissection.Network1,
		tx.LatencyDissection.VoteConsensusTime,
		tx.LatencyDissection.Network2,
		tx.LatencyDissection.DecideConsensusTime,
		tx.LatencyDissection.Network3,
		tx.LatencyDissection.CommitConsensusTime,
		tx.LatencyDissection.BlockWaitingTime,
		tx.LatencyDissection.CommitToBlockTime
}
