// Copyright 2015 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package raft

import (
	"log"

	pb "github.com/pingcap-incubator/tinykv/proto/pkg/eraftpb"
)

// RaftLog manage the log entries, its struct look like:
//
//  snapshot/first.....applied....committed....stabled.....last
//  --------|------------------------------------------------|
//                            log entries
//
// for simplify the RaftLog implement should manage all log entries
// that not truncated
type RaftLog struct {
	// storage contains all stable entries since the last snapshot.
	storage Storage

	// committed is the highest log position that is known to be in
	// stable storage on a quorum of nodes.
	committed uint64

	// applied is the highest log position that the application has
	// been instructed to apply to its state machine.
	// Invariant: applied <= committed
	applied uint64

	// log entries with index <= stabled are persisted to storage.
	// It is used to record the logs that are not persisted by storage yet.
	// Everytime handling `Ready`, the unstabled logs will be included.
	stabled uint64

	// all entries that have not yet compact.
	entries []pb.Entry

	// the incoming unstable snapshot, if any.
	// (Used in 2C)
	pendingSnapshot *pb.Snapshot

	// Your Data Here (2A).
}

// newLog returns log using the given storage. It recovers the log
// to the state that it just commits and applies the latest snapshot.
func newLog(storage Storage) *RaftLog {
	l := &RaftLog{
		storage: storage,
	}
	firstIndex, _ := storage.FirstIndex()
	lastIndex, _ := storage.LastIndex()

	l.applied = firstIndex - 1
	l.committed = firstIndex - 1
	l.stabled = lastIndex
	return l
}

// We need to compact the log entries in some point of time like
// storage compact stabled log entries prevent the log entries
// grow unlimitedly in memory
func (l *RaftLog) maybeCompact() {
	// Your Code Here (2C).
}

// unstableEntries return all the unstable entries
func (l *RaftLog) unstableEntries() []pb.Entry {
	return l.entries
}

// nextEnts returns all the committed but not applied entries
func (l *RaftLog) nextEnts() (ents []pb.Entry) {
	ents, err := l.slice(l.applied+1, l.committed+1)
	if err != nil {
		return nil
	}
	return
}

// LastIndex return the last index of the log entries
func (l *RaftLog) LastIndex() uint64 {
	if li := len(l.entries); li != 0 {
		return l.stabled + uint64(li)
	}
	li, _ := l.storage.LastIndex()
	return li
}

// Term return the term of the entry in the given index
func (l *RaftLog) Term(i uint64) (uint64, error) {
	// Your Code Here (2A).
	return 0, nil
}

func (l *RaftLog) firstIndex() uint64 {
	fi, _ := l.storage.FirstIndex()
	return fi
}

func (l *RaftLog) slice(lo, hi uint64) ([]pb.Entry, error) {
	err := l.checkOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	var ents []pb.Entry
	if lo <= l.stabled {
		storedEnts, err := l.storage.Entries(lo, min(hi, l.stabled+1))
		if err == ErrCompacted {
			return nil, err
		} else if err == ErrUnavailable {
			log.Panicf("entries[%d:%d) is unavailable from storage", lo, min(hi, l.stabled+1))
		} else if err != nil {
			panic(err) // TODO(bdarnell)
		}
		ents = storedEnts
	}
	if hi > l.stabled+1 {
		unstableEnts := l.entries[max(l.stabled+1, lo):hi]
		if len(ents) > 0 {
			combined := make([]pb.Entry, len(ents)+len(unstableEnts))
			n := copy(combined, ents)
			copy(combined[n:], unstableEnts)
			ents = combined
		} else {
			ents = unstableEnts
		}
	}
	return ents, nil
}

func (l *RaftLog) checkOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		log.Panicf("invalid slice %d > %d", lo, hi)
	}
	if lo < l.firstIndex() {
		return ErrCompacted
	}
	if hi > l.LastIndex()+1 {
		log.Panicf("slice[%d,%d) out of bound [%d,%d]", lo, hi, l.firstIndex(), l.LastIndex())
	}
	return nil
}
