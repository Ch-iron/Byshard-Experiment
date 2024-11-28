package mempool

import (
	"paperexperiment/config"
	"paperexperiment/message"
)

type Producer struct {
	mempool *MemPool
}

func NewProducer() *Producer {
	return &Producer{
		mempool: NewMemPool(),
	}
}

func (pd *Producer) GeneratePayload(manageTransactions *ManageTransactions) []*message.Transaction {
	return pd.mempool.some(config.Configuration.BSize, manageTransactions)
}

func (pd *Producer) AddTxn(txn *message.Transaction) {
	pd.mempool.addNew(txn)
}

func (pd *Producer) PopTxs(txn *message.TransactionAccept) *message.Transaction {
	return pd.mempool.front()
}

func (pd *Producer) CollectTxn(txn *message.Transaction) {
	pd.mempool.addOld(txn)
}

func (pd *Producer) TotalReceivedTxNo() int64 {
	return pd.mempool.totalReceived
}
