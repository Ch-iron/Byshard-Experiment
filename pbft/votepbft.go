package pbft

import (
	"paperexperiment/crypto"
	"paperexperiment/evm/core"
	"paperexperiment/log"
	"paperexperiment/message"
	"paperexperiment/quorum"
	"paperexperiment/types"
	"paperexperiment/utils"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func (pb *PBFT) IsRootShard(to common.Address) bool {
	var address []common.Address
	address = append(address, to)
	if utils.CalculateShardToSend(address)[0] == pb.Shard() {
		return true
	} else {
		return false
	}
}

func (pb *PBFT) InitVoteStep(tx *message.SignedTransactionWithHeader) *message.SignedTransactionWithHeader {
	// Setting Metric
	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		pb.AddConsensusTx(tx.Transaction.Hash)
		if tx.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.From) {
				tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.LatencyDissection.BlockWaitingTime = tx.Transaction.LatencyDissection.BlockWaitingTime + tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli()
			}
		} else if tx.Transaction.TXType == types.SMARTCONTRACT {
			if pb.IsRootShard(tx.Transaction.To) {
				tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.LatencyDissection.BlockWaitingTime = tx.Transaction.LatencyDissection.BlockWaitingTime + tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli()
			}
		}
	}
	log.VoteDebugf("[%v %v] (InitVoteStep) Init Vote Step for view %v epoch %v Hash: %v, Nonce: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Hash, tx.Transaction.Nonce)

	// Lock Table Inspect
	if tx.Transaction.TXType == types.TRANSFER {
		if pb.IsRootShard(tx.Transaction.From) { // Root Shard
			pb.executemu.Lock()
			isCommit := core.CanTransfer(pb.bc.GetStateDB(), tx.Transaction.From, uint256.NewInt(uint64(tx.Transaction.Value)))
			pb.executemu.Unlock()
			if !isCommit {
				// Abort ignore
				tx.Transaction.IsAbort = true
			} else {
				if !pb.addAddressLockTable(tx.Transaction.From, tx.Transaction.Hash) {
					tx.Transaction.IsAbort = true
				} else {
					tx.Transaction.IsAbort = false
				}
			}
		} else if pb.IsRootShard(tx.Transaction.To) { // Associate Shard
			if !pb.addAddressLockTable(tx.Transaction.To, tx.Transaction.Hash) {
				tx.Transaction.IsAbort = true
			} else {
				tx.Transaction.IsAbort = false
			}
		}
	} else if tx.Transaction.TXType == types.SMARTCONTRACT {
		if !pb.addLockTable(tx.Transaction.RwSet, tx.Transaction.Hash) {
			tx.Transaction.IsAbort = true
		} else {
			tx.Transaction.IsAbort = false
		}
	}
	if !tx.Transaction.IsAbort {
		log.VoteDebugf("[%v %v] (InitVoteStep) Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
	} else {
		log.VoteDebugf("[%v %v] (InitVoteStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
	}

	// only for logging
	// for tx_hash, locks := range pb.lockTable_transaction {
	// 	log.LockDebugf("[%v %v] (InitVoteStep) TransactionHash: %v, Lock Table: %v", pb.Shard(), pb.ID(), tx_hash, &locks)
	// }
	// for addr, table := range pb.lockTable_variable {
	// 	log.LockDebugf("[%v %v] (InitVoteStep) Address: %v, Lock Table: %v", pb.Shard(), pb.ID(), addr, &table)
	// }

	pb.agreemu.Lock()
	if pb.agreeingTransactions[tx.Transaction.Hash] == nil {
		pb.agreeingTransactions[tx.Transaction.Hash] = make(map[int]*message.SignedTransactionWithHeader)
	}
	pb.agreeingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] = tx

	if qc, ok := pb.transactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce]; ok {
		pb.processCertificateTransactionVote(pb.agreeingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce], qc)
		pb.agreemu.Lock()
		pb.transactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce] = nil
		pb.agreemu.Unlock()
		vote := quorum.MakeTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Hash, !tx.Transaction.IsAbort, tx.Transaction.Nonce)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
		return nil
	} else {
		pb.agreemu.Unlock()
	}

	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.VoteDebugf("[%v %v] (InitVoteStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
	}

	vote := quorum.MakeTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Hash, !tx.Transaction.IsAbort, tx.Transaction.Nonce)

	pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
	pb.ProcessTransactionVote(vote)

	log.VoteDebugf("[%v %v] (InitVoteStep) Finished preprepared step for view %v epoch %v Hash: %v, vote Iscommit: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Hash, vote.IsCommit)

	return tx
}

func (pb *PBFT) ProcessTransactionVote(vote *quorum.TransactionVote) {
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) processing vote for view %v epoch %v Hash %v from %v, iscommit: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Voter, vote.IsCommit)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.transactionvoteQuorum.Add(vote)
	if !isBuilt {
		// log.VoteDebugf("[%v %v] (ProcessTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) vote qc is created Hash %v Nonce %v", pb.ID(), pb.Shard(), vote.TransactionHash, vote.Nonce)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.agreemu.Lock()
	if pb.agreeingTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.transactionbufferedQCs[qc.TransactionHash] == nil {
			pb.transactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.TransactionQC)
		}
		pb.transactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.agreemu.Unlock()
		return
	}

	pb.processCertificateTransactionVote(pb.agreeingTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.VoteDebugf("[%v %v] (ProcessTransactionVote) finished processing vote for view %v epoch %v Hash %v, isCommit: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.IsCommit)
}

func (pb *PBFT) ProcessTransactionCommit(commit *quorum.TransactionCommit) {
	log.VoteDebugf("[%v %v] (ProcessTransactionCommit) processing commit for view %v epoch %v Hash %v from %v iscommit: %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash, commit.Voter, commit.IsCommit)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.transactioncommitQuorum.Add(commit)
	if !isBuilt {
		// log.VoteDebugf("[%v %v] (ProcessTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) commit cqc is created Hash %v Nonce %v", pb.ID(), pb.Shard(), commit.TransactionHash, commit.Nonce)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.agreemu.Lock()
	if pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce] == nil || pb.agreeingVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.transactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.transactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.TransactionQC)
		}
		pb.transactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.agreemu.Unlock()
		return
	}

	pb.processCertificateTransactionCqc(pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce], cqc)

	log.VoteDebugf("[%v %v] (ProcessTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessTransactionAccept(accept *message.TransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(accept.Transaction.Committee_sig[1], crypto.IDToByte(accept.Transaction.Hash))
	if !transactionIsVerified {
		// log.Errorf("[%v %v] (ProcessTransactionAccept) Received a transaction accept from leader with an invalid signature", pb.Shard(), pb.ID())
		return
	}

	if accept.Transaction.TXType == types.TRANSFER {
		if pb.IsRootShard(accept.Transaction.From) { // Root Shard
			pb.executemu.Lock()
			isCommit := core.CanTransfer(pb.bc.GetStateDB(), accept.Transaction.From, uint256.NewInt(uint64(accept.Transaction.Value)))
			pb.executemu.Unlock()
			if !isCommit {
				// Abort ignore
				return
			}
			if !pb.addAddressLockTable(accept.Transaction.From, accept.Transaction.Hash) {
				return
			}
		} else { // Associate Shard
			if !pb.addAddressLockTable(accept.Transaction.To, accept.Transaction.Hash) {
				return
			}
		}
	} else if accept.Transaction.TXType == types.SMARTCONTRACT {
		if !pb.addLockTable(accept.Transaction.RwSet, accept.Transaction.Hash) {
			if pb.IsRootShard(accept.Transaction.To) {
				return
			} else {
				return
			}
		}
	}

	var associate_shards []types.Shard
	associate_addresses := []common.Address{}
	for _, rwset := range accept.Transaction.RwSet {
		associate_addresses = append(associate_addresses, rwset.Address)
	}
	associate_shards = utils.CalculateShardToSend(associate_addresses)
	pb.AddDecidingTransaction(accept.Transaction.Hash, len(associate_shards)-1)
}

func (pb *PBFT) processCertificateTransactionVote(votingTransaction *message.SignedTransactionWithHeader, qc *quorum.TransactionQC) {
	log.VoteDebugf("[%v %v] (processCertificateTransactionVote) Start processCertificateTransactionVote hash %v", pb.ID(), pb.Shard(), votingTransaction.Transaction.Hash)
	if pb.agreeingVoteTransactions[qc.TransactionHash] == nil {
		pb.agreeingVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.TransactionQC)
	}
	pb.agreeingVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.transactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateTransactionCqc(votingTransaction, cqc)
		pb.agreemu.Lock()
		pb.transactionbufferedCQCs[qc.TransactionHash][qc.Nonce] = nil
		pb.agreemu.Unlock()
	} else {
		pb.agreemu.Unlock()
	}

	commit := quorum.MakeTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessTransactionCommit(commit)
}

func (pb *PBFT) processCertificateTransactionCqc(votingTransaction *message.SignedTransactionWithHeader, cqc *quorum.TransactionQC) {
	log.VoteDebugf("[%v %v] (processCertificateTransactionCqc) Start processCertificateTransactionCqc hash %v", pb.ID(), pb.Shard(), votingTransaction.Transaction.Hash)
	var associate_shards []types.Shard
	associate_addresses := []common.Address{}
	if votingTransaction.Transaction.TXType == types.TRANSFER {
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.To)
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.From)
	} else if votingTransaction.Transaction.TXType == types.SMARTCONTRACT {
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.ExternalAddressList...)
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.To)
	}
	associate_shards = utils.CalculateShardToSend(associate_addresses)
	pb.AddDecidingTransaction(pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce].Transaction.Hash, len(associate_shards)-1)

	if !cqc.IsCommit {
		if votingTransaction.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(votingTransaction.Transaction.From) { // Root Shard
				pb.deleteAddressLockTable(votingTransaction.Transaction.From, votingTransaction.Transaction.Hash)
			} else if pb.IsRootShard(votingTransaction.Transaction.To) { // Associate Shard
				pb.deleteAddressLockTable(votingTransaction.Transaction.To, votingTransaction.Transaction.Hash)
			}
		} else if votingTransaction.Transaction.TXType == types.SMARTCONTRACT {
			pb.deleteLockTable(votingTransaction.Transaction.RwSet, votingTransaction.Transaction.Hash)
		}
	}

	// If Leader, Accept Message Broadcast To Validator
	// Todo: If commit and associate shard, send to root shard with rwset snapshot
	if cqc.Leader == pb.ID() {
		accept := &message.TransactionAccept{
			SignedTransactionWithHeader: message.SignedTransactionWithHeader{
				Header:      votingTransaction.Header,
				Transaction: votingTransaction.Transaction,
			},
		}
		pb.agreemu.Unlock()
		if cqc.IsCommit {
			accept.IsCommit = true
		} else {
			accept.IsCommit = false
		}
		validators := pb.FindValidatorsFor(cqc.Epoch)
		pb.BroadcastToSome(validators, accept)
		var vote_complete_transaction message.RootShardVotedTransaction
		vote_complete_transaction.Transaction = accept.Transaction.Transaction
		if cqc.IsCommit {
			vote_complete_transaction.IsCommit = true
		} else {
			vote_complete_transaction.IsCommit = false
		}

		if vote_complete_transaction.IsCommit {
			if vote_complete_transaction.TXType == types.SMARTCONTRACT {
				if !pb.IsRootShard(vote_complete_transaction.Transaction.To) {
					pb.executemu.Lock()
					stateDB := pb.bc.GetStateDB()
					// Create Code Snapshot
					for _, externalAddress := range vote_complete_transaction.ExternalAddressList {
						if pb.IsExist(externalAddress) {
							codeSnapshot := message.Snapshot{
								Address:    externalAddress,
								Value:      string(stateDB.GetCode(externalAddress)),
								IsContract: true,
							}
							vote_complete_transaction.Snapshots = append(vote_complete_transaction.Snapshots, codeSnapshot)
						}
					}
					// Create Snapshot
					for _, rwset := range vote_complete_transaction.RwSet {
						if pb.IsExist(rwset.Address) {
							for _, readset := range rwset.ReadSet {
								valueSnapshot := message.Snapshot{
									Address:    rwset.Address,
									Slot:       common.HexToHash(readset),
									Value:      stateDB.GetState(rwset.Address, common.HexToHash(readset)).String(),
									IsContract: false,
								}
								vote_complete_transaction.Snapshots = append(vote_complete_transaction.Snapshots, valueSnapshot)
							}
							for _, writeset := range rwset.WriteSet {
								valueSnapshot := message.Snapshot{
									Address:    rwset.Address,
									Slot:       common.HexToHash(writeset),
									Value:      stateDB.GetState(rwset.Address, common.HexToHash(writeset)).String(),
									IsContract: false,
								}
								vote_complete_transaction.Snapshots = append(vote_complete_transaction.Snapshots, valueSnapshot)
							}
						}
					}
					pb.executemu.Unlock()
				}
			}
		}

		pb.SendToBlockBuilder(vote_complete_transaction)
		if !vote_complete_transaction.IsCommit {
			vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli() - vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime
			vote_complete_transaction.LatencyDissection.RootVoteConsensusTime = vote_complete_transaction.LatencyDissection.RootVoteConsensusTime + vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime
			vote_complete_transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
			if vote_complete_transaction.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(vote_complete_transaction.Transaction.From) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
					pb.SubConsensusTx(vote_complete_transaction.Transaction.Hash)
				}
			} else if vote_complete_transaction.Transaction.TXType == types.SMARTCONTRACT {
				if pb.IsRootShard(vote_complete_transaction.Transaction.To) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
					pb.SubConsensusTx(vote_complete_transaction.Transaction.Hash)
				}
			}
		}
		log.VoteDebugf("[%v %v] (processCertificateTransactionCqc) Send Transaction completed consensus To BlockBuilder Tx Hash: %v, Nonce: %v, IsCommit: %v", pb.ID(), pb.Shard(), cqc.TransactionHash, cqc.Nonce, cqc.IsCommit)
	} else {
		pb.agreemu.Unlock()
	}
}
