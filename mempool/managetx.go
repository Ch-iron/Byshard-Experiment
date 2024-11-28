package mempool

import (
	"sync"

	"paperexperiment/message"

	"github.com/ethereum/go-ethereum/common"
)

type ManageTransactions struct {
	pendingTransactions   map[common.Hash]*message.Transaction
	processedTransactions map[common.Hash]*message.Transaction

	mu sync.Mutex
}

func NewManageTransactions() *ManageTransactions {
	return &ManageTransactions{
		pendingTransactions:   make(map[common.Hash]*message.Transaction),
		processedTransactions: make(map[common.Hash]*message.Transaction),
	}
}

func (pt *ManageTransactions) AddPendingTransactions(payload []*message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, tx := range payload {
		pt.pendingTransactions[tx.Hash] = tx
	}
}

func (pt *ManageTransactions) CheckPendingTransactions(tx *message.Transaction) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	_, exist := pt.pendingTransactions[tx.Hash]
	return exist
}

func (pt *ManageTransactions) DeletePendingTransactions(payload []*message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, tx := range payload {
		delete(pt.pendingTransactions, tx.Hash)
	}
}

func (pt *ManageTransactions) DeletePendingTransaction(tx *message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	delete(pt.pendingTransactions, tx.Hash)
}

func (pt *ManageTransactions) AddProcessedTransaction(tx *message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.processedTransactions[tx.Hash] = tx
}

func (pt *ManageTransactions) AddProcessedTransactions(payload []*message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, tx := range payload {
		pt.pendingTransactions[tx.Hash] = tx
	}
}

func (pt *ManageTransactions) CheckProcessedTransactions(tx *message.Transaction) bool {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	_, exist := pt.processedTransactions[tx.Hash]
	return exist
}

func (pt *ManageTransactions) DeleteProcessedTransactions(payload []*message.Transaction) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	for _, tx := range payload {
		delete(pt.processedTransactions, tx.Hash)
	}
}
