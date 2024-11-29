package message

import (
	"fmt"

	"paperexperiment/client/db"
	"paperexperiment/crypto"
	"paperexperiment/identity"
	"paperexperiment/types"

	"github.com/ethereum/go-ethereum/common"
)

/***************************
 *  paperexperiment Init Related  *
 ***************************/
type ShardList []string

type CommunicatorRegister struct {
	SenderShard types.Shard
	Address     string
}

type CommunicatorListRequest struct {
	Gateway string
}

type CommunicatorListResponse struct {
	Builders map[types.Shard]string
}

type ShardSharedVariableRegisterRequest struct {
	Gateway string
}

type ShardSharedVariableRegisterResponse struct {
	SenderShard types.Shard
	Variables   string
}

type ConsensusNodeRegister struct {
	ConsensusNodeID identity.NodeID
	IP              string
}

type CreateBlock struct {
	Message string
}

/***************************
 *  paperexperiment RwSet Related *
 ***************************/
type NodeStartMessage struct {
	Message string
}

type NodeStartAck struct {
	ID identity.NodeID
}

type ClientStart struct {
	Shard types.Shard
}

type RwVariable struct {
	Address common.Address
	Name    string
	types.RwSet
}

// Transaction Format
type TransactionForm struct {
	From                common.Address
	To                  common.Address
	Value               int
	Data                []byte
	ExternalAddressList []common.Address
	MappingIdx          []byte
	Timestamp           int64
}

type CrossShardTransactionLatency struct {
	Hash    common.Hash
	Latency int64
}

type Experiment struct {
	types.Shard
	ExperimentTransactionResult  ExperimentTransactionResult
	CrossShardTransactionLatency []*CrossShardTransactionLatency
	LocalLatency                 float64
	ConsensusForISC              float64
	ISC                          float64
	ConsensusForCommit           float64
	WaitingTime                  float64
	LocalConsensus               float64
	LocalWaitingTime             float64
	LocalConsensusForCommit      float64
	Total                        float64
	FlagForExperiment            bool
}

type ExperimentTransactionResult struct {
	TotalTransaction int
	LocalTransaction int
	CrossTransaction int
	RunningTime      float64
}

type CrossShardTransactionLatencyDissection struct {
	RootVoteConsensusTime int64
	Network1              int64
	RootVoteToDecideTime  int64
	VoteConsensusTime     int64
	Network2              int64
	DecideConsensusTime   int64
	Network3              int64
	CommitConsensusTime   int64
	ExecutionTime         int64
	BlockWaitingTime      int64
	CommitToBlockTime     int64
	ProcessTime           int64
}

type AbortAndRetryLatency struct {
	RootVoteConsensusTime int64
	Network1              int64
	RootVoteToDecideTime  int64
	VoteConsensusTime     int64
	Network2              int64
	DecideConsensusTime   int64
	Network3              int64
	CommitConsensusTime   int64
	ExecutionTime         int64
	ConsensusTime         int64
	BlockWaitingTime      int64
	CommitToBlockTime     int64
	ProcessTime           int64
}

type LocalTransactionLatencyDissection struct {
	ConsensusTime     int64
	BlockWaitingTime  int64
	CommitToBlockTime int64
	ProcessTime       int64
}

type Snapshot struct {
	Address    common.Address
	Slot       common.Hash
	Value      string
	IsContract bool
}

type Transaction struct {
	Hash   common.Hash
	TXType types.TransactionType

	From                           common.Address
	To                             common.Address
	Value                          int
	Data                           []byte
	ExternalAddressList            []common.Address
	RwSet                          []RwVariable
	IsCrossShardTx                 bool
	Timestamp                      int64
	IsAbort                        bool
	Nonce                          int
	Snapshots                      []Snapshot
	LatencyDissection              CrossShardTransactionLatencyDissection
	AbortAndRetryLatencyDissection AbortAndRetryLatency
	LocalLatencyDissection         LocalTransactionLatencyDissection
	StartTime                      int64
}

type TransactionHeader struct {
	View     types.View
	Epoch    types.Epoch
	Proposer identity.NodeID
}

type CommunicatorSignedTransaction SignedTransaction

type SignedTransaction struct {
	Transaction
	Committee_sig []crypto.Signature
}

type SignedTransactionWithHeader struct {
	Header      TransactionHeader
	Transaction SignedTransaction
}

type TransactionAccept struct {
	SignedTransactionWithHeader
	IsCommit bool
}

type RootShardVotedTransaction struct {
	Transaction
	IsCommit bool
}

type VoteCommunicatorSignedTransaction SignedTransaction

type VotedTransaction struct {
	RootShardVotedTransaction
	Committee_sig []crypto.Signature
}
type LeadervotedTransaction VotedTransaction
type DecideTransactionAccept VotedTransactionWithHeader
type VotedTransactionWithHeader struct {
	Header      TransactionHeader
	Transaction VotedTransaction
}

type VotedTransactionWithHeaderSigned struct {
	Transaction   VotedTransactionWithHeader
	Committee_sig []crypto.Signature
}

type CommittedTransaction VotedTransaction
type CommitTransactionAccept CommittedTransactionWithHeader
type CommittedTransactionWithHeader struct {
	Header      TransactionHeader
	Transaction CommittedTransaction
}

type CompleteTransaction RootShardVotedTransaction

func MakeTransferTransaction(requestTx TransactionForm, isCrossShardTx bool) *Transaction {
	tx := new(Transaction)
	tx.From = requestTx.From
	tx.To = requestTx.To
	tx.Value = requestTx.Value
	tx.IsCrossShardTx = isCrossShardTx
	tx.Timestamp = requestTx.Timestamp
	tx.TXType = types.TRANSFER
	tx.makeID()
	return tx
}

func MakeDeployTransaction(requestTx TransactionForm, isCrossShardTx bool) *Transaction {
	tx := new(Transaction)
	tx.From = requestTx.From
	tx.To = requestTx.To
	tx.Value = requestTx.Value
	tx.Data = []byte(requestTx.Data)
	tx.IsCrossShardTx = isCrossShardTx
	tx.Timestamp = requestTx.Timestamp
	tx.TXType = types.DEPLOY
	tx.makeID()
	return tx
}

func MakeSmartContractTransaction(requestTx TransactionForm, rwSet []RwVariable, isCrossShardTx bool) *Transaction {
	tx := new(Transaction)
	tx.From = requestTx.From
	tx.To = requestTx.To
	tx.Value = requestTx.Value
	tx.Data = requestTx.Data
	tx.ExternalAddressList = requestTx.ExternalAddressList
	tx.RwSet = rwSet
	tx.IsCrossShardTx = isCrossShardTx
	tx.Timestamp = requestTx.Timestamp
	tx.TXType = types.SMARTCONTRACT
	tx.makeID()
	return tx
}

type rawTransaction struct {
	Command   db.Command
	From      common.Address
	To        common.Address
	Value     int
	Data      []byte
	TXType    types.TransactionType
	Timestamp int64
	// NodeID    identity.NodeID // forward by node
}

func (tx *Transaction) makeID() {
	raw := &rawTransaction{
		From:      tx.From,
		To:        tx.To,
		Value:     tx.Value,
		Data:      tx.Data,
		TXType:    tx.TXType,
		Timestamp: tx.Timestamp,
		// NodeID:    tx.NodeID,
	}

	tx.Hash = crypto.MakeID(raw)
}

// Read can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Read struct {
	CommandID int
	Key       db.Key
}

func (r Read) String() string {
	return fmt.Sprintf("Read {cid=%d, key=%d}", r.CommandID, r.Key)
}

// ReadReply cid and value of reading key
type ReadReply struct {
	CommandID int
	Value     db.Value
}

// Query can be used as a special request that directly read the value of key without go through replication protocol in Replica
type Query struct {
	C chan QueryReply
}

func (r *Query) Reply(reply QueryReply) {
	r.C <- reply
}

// QueryReply cid and value of reading key
type QueryReply struct {
	Info string
}

// Request Leader
type RequestLeader struct {
	C chan RequestLeaderReply
}

func (r *RequestLeader) Reply(reply RequestLeaderReply) {
	r.C <- reply
}

// QueryReply cid and value of reading key
type RequestLeaderReply struct {
	Leader string
}

// Request Leader
type ReportByzantine struct {
	C chan ReportByzantineReply
}

func (r *ReportByzantine) Reply(reply ReportByzantineReply) {
	r.C <- reply
}

// QueryReply cid and value of reading key
type ReportByzantineReply struct {
	Leader string
}
