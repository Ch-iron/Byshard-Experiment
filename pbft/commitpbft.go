package pbft

import (
	"paperexperiment/crypto"
	"paperexperiment/evm"
	"paperexperiment/evm/state/tracing"
	"paperexperiment/evm/vm/runtime"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/quorum"
	"paperexperiment/types"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func (pb *PBFT) InitCommitStep(tx *message.CommittedTransactionWithHeader) *message.CommittedTransactionWithHeader {
	if !tx.Transaction.IsCommit {
		log.CommitDebugf("[%v %v] (InitCommitStep) Init Abort Step for view %v epoch %v Hash: %v Nonce: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Hash, tx.Transaction.Nonce)
		if tx.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.From) { // Root Shard
				pb.deleteAddressLockTable(tx.Transaction.From, tx.Transaction.Hash)
				log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
			} else if pb.IsRootShard(tx.Transaction.To) { // Associate Shard
				pb.deleteAddressLockTable(tx.Transaction.To, tx.Transaction.Hash)
				log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
			}
		} else if tx.Transaction.TXType == types.SMARTCONTRACT {
			pb.deleteLockTable(tx.Transaction.RwSet, tx.Transaction.Hash)
			log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
		}
	} else {
		log.CommitDebugf("[%v %v] (InitCommitStep) Init Commit Step for view %v epoch %v Hash: %v Nonce: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Hash, tx.Transaction.Nonce)
		// Deploy snapshot's other shard value and Excute Transaction
		cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
		pb.executemu.Lock()
		initState := pb.bc.GetStateDB()
		evm_machine := runtime.NewEnv(cfg, initState)
		if tx.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.From) {
				initState.SubBalance(tx.Transaction.From, uint256.NewInt(uint64(tx.Transaction.Value)), tracing.BalanceChangeUnspecified)
			} else if pb.IsRootShard(tx.Transaction.To) {
				initState.AddBalance(tx.Transaction.To, uint256.NewInt(uint64(tx.Transaction.Value)), tracing.BalanceChangeUnspecified)
			}
		} else if tx.Transaction.TXType == types.SMARTCONTRACT {
			if !pb.IsRootShard(tx.Transaction.From) {
				initState.CreateAccount(tx.Transaction.From)
				initState.SetBalance(tx.Transaction.From, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
			}
			// foreign contract code and variable deploy & contract execute
			for _, snapshot := range tx.Transaction.Snapshots {
				if snapshot.IsContract && !pb.IsExist(snapshot.Address) {
					// SetCode
					initState.CreateTemporaryAccount(snapshot.Address)
					initState.SetCode(snapshot.Address, []byte(snapshot.Value))
				}
			}
			for _, snapshot := range tx.Transaction.Snapshots {
				if !snapshot.IsContract && !pb.IsExist(snapshot.Address) {
					// SetState
					initState.SetState(snapshot.Address, snapshot.Slot, common.HexToHash(snapshot.Value))
				}
			}
			_, _, err := evm.Execute(evm_machine, initState, tx.Transaction.Data, tx.Transaction.From, tx.Transaction.To)
			if err != nil {
				log.Errorf("[%v %v] (InitCommitStep) Error Execute Cross Smart Contract: %v Hash: %v Address: %v", pb.ID(), pb.Shard(), err, tx.Transaction.Hash, tx.Transaction.To)
			}
			// remove contract
			for _, snapshot := range tx.Transaction.Snapshots {
				if snapshot.IsContract && !pb.IsExist(snapshot.Address) {
					// removeCode
					initState.RemoveTemporaryAccount(snapshot.Address)
				}
			}
		}
		pb.executemu.Unlock()
	}

	// only for logging
	// for tx_hash, locks := range pb.lockTable_transaction {
	// 	log.LockDebugf("[%v %v] (InitCommitStep) TransactionHash: %v, Lock Table: %v", pb.ID(), pb.Shard(), tx_hash, &locks)
	// }
	// for addr, table := range pb.lockTable_variable {
	// 	log.LockDebugf("[%v %v] (InitCommitStep) Address: %v, Lock Table: %v", pb.ID(), pb.Shard(), addr, &table)
	// }

	pb.commitmu.Lock()
	if pb.committingTransactions[tx.Transaction.Hash] == nil {
		pb.committingTransactions[tx.Transaction.Hash] = make(map[int]*message.CommittedTransactionWithHeader)
	}
	pb.committingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] = tx

	if qc, ok := pb.committransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce]; ok {
		pb.processCertificateCommitVote(pb.committingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce], qc)
		pb.commitmu.Lock()
		pb.committransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce] = nil
		pb.commitmu.Unlock()
		vote := quorum.MakeCommitTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Hash, tx.Transaction.IsCommit, tx.Transaction.Nonce)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
		return nil
	} else {
		pb.commitmu.Unlock()
	}

	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.CommitDebugf("[%v %v] (InitCommitStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
	}

	vote := quorum.MakeCommitTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Hash, tx.Transaction.IsCommit, tx.Transaction.Nonce)

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
	pb.ProcessCommitTransactionVote(vote)

	log.CommitDebugf("[%v %v] (InitCommitStep) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Hash)

	return tx
}

func (pb *PBFT) ProcessCommitTransactionVote(vote *quorum.CommitTransactionVote) {
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) processing vote for view %v epoch %v Hash %v Nonce %v from %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Nonce, vote.Voter)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessCommitTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessCommitTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.committransactionvoteQuorum.Add(vote)
	if !isBuilt {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) vote qc is created Hash %v nonce %v", pb.ID(), pb.Shard(), vote.TransactionHash, vote.Nonce)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.commitmu.Lock()
	if pb.committingTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.committransactionbufferedQCs[qc.TransactionHash] == nil {
			pb.committransactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
		}
		pb.committransactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.commitmu.Unlock()
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) bufferedQC Hash %v nonce %v", pb.ID(), pb.Shard(), vote.TransactionHash, vote.Nonce)
		return
	}

	pb.processCertificateCommitVote(pb.committingTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) finished processing vote for view %v epoch %v Hash %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash)
}

func (pb *PBFT) ProcessCommitTransactionCommit(commit *quorum.CommitTransactionCommit) {
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) processing commit for view %v epoch %v Hash %v Nonce %v from %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash, commit.Nonce, commit.Voter)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessCommitTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessCommitTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.committransactioncommitQuorum.Add(commit)
	if !isBuilt {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) commit cqc is created Hash %v nonce %v", pb.ID(), pb.Shard(), commit.TransactionHash, commit.Nonce)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.commitmu.Lock()
	if pb.committingTransactions[cqc.TransactionHash][cqc.Nonce] == nil || pb.committingVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.committransactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.committransactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
		}
		pb.committransactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.commitmu.Unlock()
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) bufferedCQC Hash %v nonce %v", pb.ID(), pb.Shard(), commit.TransactionHash, commit.Nonce)
		return
	}

	pb.processCertificateCommitCqc(pb.committingTransactions[cqc.TransactionHash][cqc.Nonce], cqc)
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessCommitTransactionAccept(accept *message.CommitTransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	if !accept.Transaction.IsCommit {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Init Abort Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Hash)
		if len(pb.lockTable_transaction[accept.Transaction.Hash]) > 0 {
			if accept.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(accept.Transaction.From) { // Root Shard
					pb.deleteAddressLockTable(accept.Transaction.From, accept.Transaction.Hash)
					log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Hash)
				} else if pb.IsRootShard(accept.Transaction.To) { // Associate Shard
					pb.deleteAddressLockTable(accept.Transaction.To, accept.Transaction.Hash)
					log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Hash)
				}
			} else if accept.Transaction.TXType == types.SMARTCONTRACT {
				pb.deleteLockTable(accept.Transaction.RwSet, accept.Transaction.Hash)
				log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Hash)
				// if pb.IsRootShard(accept.Transaction.To) {
				// 	log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Hash)
				// }
			}
		}
	} else {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Init Commit Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Hash)
		// Execute
		cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
		evm_machine := runtime.NewEnv(cfg, pb.bc.GetStateDB())
		if accept.Transaction.TXType == types.TRANSFER {
			pb.executemu.Lock()
			if pb.IsRootShard(accept.Transaction.From) {
				pb.bc.GetStateDB().SubBalance(accept.Transaction.From, uint256.NewInt(uint64(accept.Transaction.Value)), tracing.BalanceChangeUnspecified)
			} else if pb.IsRootShard(accept.Transaction.To) {
				pb.bc.GetStateDB().AddBalance(accept.Transaction.To, uint256.NewInt(uint64(accept.Transaction.Value)), tracing.BalanceChangeUnspecified)
			}
			pb.executemu.Unlock()
		} else if accept.Transaction.TXType == types.SMARTCONTRACT {
			for _, rwset := range accept.Transaction.RwSet {
				if pb.IsRootShard(rwset.Address) {
					fdcm := 0
					if !pb.IsRootShard(accept.Transaction.From) {
						pb.executemu.Lock()
						pb.bc.GetStateDB().CreateAccount(accept.Transaction.From)
						pb.bc.GetStateDB().SetBalance(accept.Transaction.From, uint256.NewInt(1e18), tracing.BalanceChangeUnspecified)
						pb.executemu.Unlock()
						fdcm++
					}
					pb.executemu.Lock()
					_, _, err := evm.Execute(evm_machine, pb.bc.GetStateDB(), accept.Transaction.Data, accept.Transaction.From, rwset.Address)
					pb.executemu.Unlock()
					if err != nil {
						log.Errorf("[%v %v] (ProcessCommitTransactionAccept) Error Execute Cross Smart Contract: %v", pb.ID(), pb.Shard(), err)
						continue
					}
					if fdcm != 0 {
						pb.executemu.Lock()
						pb.bc.GetStateDB().SelfDestruct(accept.Transaction.From)
						pb.executemu.Unlock()
					}
				}
			}
			pb.deleteLockTable(accept.Transaction.RwSet, accept.Transaction.Hash)
			// log.StateInfof("[%v %v] (ProcessCommitTransactionAccept) Execute Smart Contract address %v, %v", pb.ID(), pb.Shard(), accept.Transaction.Hash, accept.Transaction.To)
		}
	}
}

func (pb *PBFT) processCertificateCommitVote(committingTransaction *message.CommittedTransactionWithHeader, qc *quorum.CommitTransactionQC) {
	log.CommitDebugf("[%v %v] (processCertificateCommitVote) Start processCertificateCommitVote hash %v nonce %v", pb.ID(), pb.Shard(), committingTransaction.Transaction.Hash, committingTransaction.Transaction.Nonce)
	if pb.committingVoteTransactions[qc.TransactionHash] == nil {
		pb.committingVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
	}
	pb.committingVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.committransactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateCommitCqc(committingTransaction, cqc)
		pb.commitmu.Lock()
		pb.committransactionbufferedCQCs[qc.TransactionHash][qc.Nonce] = nil
		pb.commitmu.Unlock()
	} else {
		pb.commitmu.Unlock()
	}

	commit := quorum.MakeCommitTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessCommitTransactionCommit(commit)
}

func (pb *PBFT) processCertificateCommitCqc(committingTransaction *message.CommittedTransactionWithHeader, cqc *quorum.CommitTransactionQC) {
	log.CommitDebugf("[%v %v] (processCertificateCommitCqc) Start processCertificateCommitCqc hash %v nonce %v", pb.ID(), pb.Shard(), committingTransaction.Transaction.Hash, committingTransaction.Transaction.Nonce)
	if committingTransaction.Transaction.TXType == types.TRANSFER {
		if pb.IsRootShard(committingTransaction.Transaction.From) {
			pb.deleteAddressLockTable(committingTransaction.Transaction.From, committingTransaction.Transaction.Hash)
		} else if pb.IsRootShard(committingTransaction.Transaction.To) {
			pb.deleteAddressLockTable(committingTransaction.Transaction.To, committingTransaction.Transaction.Hash)
		}
	} else if committingTransaction.Transaction.TXType == types.SMARTCONTRACT {
		pb.deleteLockTable(committingTransaction.Transaction.RwSet, committingTransaction.Transaction.Hash)
	}

	// If Leader, Accept Message Broadcast To Validator
	if cqc.Leader == pb.ID() {
		accept := &message.CommitTransactionAccept{
			Header:      pb.committingTransactions[cqc.TransactionHash][cqc.Nonce].Header,
			Transaction: pb.committingTransactions[cqc.TransactionHash][cqc.Nonce].Transaction,
		}
		pb.commitmu.Unlock()
		validators := pb.FindValidatorsFor(cqc.Epoch)
		pb.BroadcastToSome(validators, accept)
		var vote_complete_transaction message.CompleteTransaction
		vote_complete_transaction.Transaction = accept.Transaction.Transaction
		vote_complete_transaction.IsCommit = accept.Transaction.IsCommit
		vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime = time.Now().UnixMilli() - vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime
		vote_complete_transaction.LatencyDissection.CommitConsensusTime = vote_complete_transaction.LatencyDissection.CommitConsensusTime + vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime
		vote_complete_transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
		if vote_complete_transaction.IsCommit {
			vote_complete_transaction.LatencyDissection.ProcessTime = time.Now().UnixMilli() - vote_complete_transaction.StartTime
			pb.committed_transactions.AddTxn(&vote_complete_transaction.Transaction)
			log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) Add completed consensus Transaction Tx Hash: %v, Nonce: %v IsCommit: %v", pb.ID(), pb.Shard(), cqc.TransactionHash, cqc.Nonce, cqc.IsCommit)
		} else {
			if vote_complete_transaction.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(vote_complete_transaction.Transaction.From) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
				}
			} else if vote_complete_transaction.Transaction.TXType == types.SMARTCONTRACT {
				if pb.IsRootShard(vote_complete_transaction.Transaction.To) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
				}
			}
		}
		pb.SubConsensusTx(cqc.TransactionHash)
	} else {
		pb.commitmu.Unlock()
	}
}
