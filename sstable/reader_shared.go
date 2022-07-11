// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "github.com/cockroachdb/pebble/internal/base"

const (
	seqNumL5PointKey = 2
	seqNumL5RangeDel = 1
	seqNumL6All      = 0
)

type tableIterator struct {
	iter     Iterator
	twoLevel bool
}

// NOTE: The physical layout of user keys follows the descending order of freshness
//       (e.g., newer versions precede older versions)

func (i *tableIterator) getReader() *Reader {
	var r *Reader
	if i.twoLevel {
		r = i.iter.(*twoLevelIterator).reader
	} else {
		r = i.iter.(*singleLevelIterator).reader
	}
	return r
}

func (i *tableIterator) getCmp() Compare {
	var cmp Compare
	if i.twoLevel {
		cmp = i.iter.(*twoLevelIterator).cmp
	} else {
		cmp = i.iter.(*singleLevelIterator).cmp
	}
	return cmp
}

func (i *tableIterator) isShared() bool {
	r := i.getReader()
	if r.meta != nil && r.meta.IsShared {
		return true
	}
	return false
}

// cmpSharedBound returns -1 if key < smallest, 1 if key > largest,
// or 0 otherwise
func (i *tableIterator) cmpSharedBound(key []byte) int {
	if key == nil {
		return 0
	}
	r, cmp := i.getReader(), i.getCmp()
	lower := r.meta.Smallest.UserKey
	upper := r.meta.Largest.UserKey
	if cmp(key, lower) < 0 {
		return -1
	} else if cmp(key, upper) > 0 {
		return 1
	}
	return 0
}

func (i *tableIterator) isLocallyCreated() bool {
	var r *Reader
	if i.twoLevel {
		r = i.iter.(*twoLevelIterator).reader
	} else {
		r = i.iter.(*singleLevelIterator).reader
	}
	if i.isShared() && r.meta.CreatorUniqueID == DBUniqueID {
		return true
	}
	return false
}

func (i *tableIterator) setExhaustedBounds(e int8) {
	if i.twoLevel {
		i.iter.(*twoLevelIterator).exhaustedBounds = e
	} else {
		i.iter.(*singleLevelIterator).exhaustedBounds = e
	}
}

func (i *tableIterator) getCurrUserKey() *[]byte {
	var k *[]byte
	if i.twoLevel {
		k = &i.iter.(*twoLevelIterator).data.key
	} else {
		k = &i.iter.(*singleLevelIterator).data.key
	}
	return k
}

func (i *tableIterator) seekGEShared(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	r := i.getReader()
	ib := i.cmpSharedBound(key)
	if ib > 0 {
		// The search key overflows
		i.setExhaustedBounds(+1)
		return nil, nil
	} else if ib < 0 {
		// The search key underflows, substitute it with the lower shared bound
		key = r.meta.SmallestPointKey.UserKey
	}
	var k *InternalKey
	var v []byte
	if prefix == nil {
		k, v = i.iter.SeekGE(key, trySeekUsingNext)
	} else {
		k, v = i.iter.SeekPrefixGE(prefix, key, trySeekUsingNext)
	}
	if k == nil {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	// If the table is not locally created (i.e., purely foreign table), update
	// the SeqNum accordingly. Note that we don't need to perform any extra movement
	// here because if k != nil then we are guaranteed to be positioned at the first
	// user key that satisfies the condition, which is the latest version.
	if !i.isLocallyCreated() {
		if r.meta.Level == 5 {
			k.SetSeqNum(seqNumL5PointKey)
		} else if r.meta.Level == 6 {
			k.SetSeqNum(seqNumL6All)
		} else {
			panic("sharedTableIterator: a table with shared flag must have its level at 5 or 6")
		}
	}
	// finally, check upper bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	// shared path
	if i.isShared() {
		return i.seekGEShared(nil, key, trySeekUsingNext)
	}
	// non-shared path
	return i.iter.SeekGE(key, trySeekUsingNext)
}

func (i *tableIterator) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	if i.isShared() {
		return i.seekGEShared(prefix, key, trySeekUsingNext)
	}
	// non-shared path
	return i.iter.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func (i *tableIterator) seekLTShared(key []byte) (*InternalKey, []byte) {
	r, cmp := i.getReader(), i.getCmp()
	ib := i.cmpSharedBound(key)
	if ib < 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	} else if ib > 0 {
		key = r.meta.Largest.UserKey
	}
	k, v := i.iter.SeekLT(key)
	if k == nil {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	// SeekLT is different from SeekGE as we are at the oldest version for the user key
	// and we need to move to the newest version
	if !i.isLocallyCreated() {
		ik := *i.getCurrUserKey()
		k, _ = i.iter.Prev()
		for k != nil && cmp(k.UserKey, ik) == 0 {
			k, _ = i.iter.Prev()
		}
		// now, either k == nil or k < ik, so k is just one slot over
		k, v = i.iter.Next()
		if r.meta.Level == 5 {
			k.SetSeqNum(seqNumL5PointKey)
		} else if r.meta.Level == 6 {
			k.SetSeqNum(seqNumL6All)
		} else {
			panic("sharedTableIterator: a table with shared flag must have its level at 5 or 6")
		}

	}
	// check lower bound
	if i.cmpSharedBound(k.UserKey) < 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) SeekLT(key []byte) (*InternalKey, []byte) {
	// shared path
	if i.isShared() {
		return i.seekLTShared(key)
	}
	return i.iter.SeekLT(key)
}

// First() and Last() are just two synonyms of SeekGE and SeekLT

func (i *tableIterator) First() (*InternalKey, []byte) {
	r := i.getReader()
	k, v := i.iter.First()
	if i.isShared() {
		// check lower bound
		if i.cmpSharedBound(k.UserKey) < 0 {
			k, v = i.SeekGE(r.meta.Smallest.UserKey, true)
		}
	}
	return k, v
}

func (i *tableIterator) Last() (*InternalKey, []byte) {
	r := i.getReader()
	k, v := i.iter.Last()
	if i.isShared() {
		// check upper bound
		if i.cmpSharedBound(k.UserKey) > 0 {
			k, v = i.SeekLT(r.meta.Largest.UserKey)
		}
	}
	return k, v
}

func (i *tableIterator) nextShared() (*InternalKey, []byte) {
	r, cmp := i.getReader(), i.getCmp()
	// Next() is not a simple case, as a valid position of an iterator
	// for a purely foreign table always points to the latest version of a user key,
	// and all the other versions are not exposed. Therefore, when we move forward,
	// it is highly possible that we encounter these history versions which we should omit,
	// and we can not easily determine when we crossed the key boundaries.
	// To this end, we let tmpIter go first.
	ik := *i.getCurrUserKey()
	k, v := i.iter.Next()
	if k == nil {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	if !i.isLocallyCreated() {
		// k is not nil, so it might position to a different key or a invisible history version
		for k != nil && cmp(k.UserKey, ik) == 0 {
			k, _ = i.iter.Next()
		}
		// now one of the following conditions stands:
		//   k == nil, we just return nil, or
		//   k > ik, we let iter step back once
		if k == nil {
			i.setExhaustedBounds(+1)
			return nil, nil
		}
		k, v = i.iter.Prev()
		if r.meta.Level == 5 {
			k.SetSeqNum(seqNumL5PointKey)
		} else if r.meta.Level == 6 {
			k.SetSeqNum(seqNumL6All)
		} else {
			panic("sharedTableIterator: a table with shared flag must have its level at 5 or 6")
		}
	}
	// check upper bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(+1)
		return nil, nil
	}
	return k, v
}

func (i *tableIterator) Next() (*InternalKey, []byte) {
	if i.isShared() {
		return i.nextShared()
	}
	return i.iter.Next()
}

func (i *tableIterator) prevShared() (*InternalKey, []byte) {
	r, cmp := i.getReader(), i.getCmp()
	// First move to the previous position, as we must move at least once.
	// Note that if the iterator operates correctly, this Prev() must set the position
	// of the iterator to a different key, as we were exposing the latest point version
	// of a user key, i.e., the first slot.
	k, v := i.iter.Prev()
	if k == nil {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	// if the table is not locally created (i.e., purely foreign table), make sure exactly
	// one version (the latest) of a user key is exposed. The SeqNum needs to be updated accordingly.
	if !i.isLocallyCreated() {
		ik := *i.getCurrUserKey()
		// find duplicated keys, or nil, whichever comes first
		for k != nil && cmp(k.UserKey, ik) == 0 {
			k, _ = i.iter.Prev()
		}
		// At the current moment, either k < ik, or k == nil. So we rewind iter once.
		k, v = i.iter.Next()
		if r.meta.Level == 5 {
			k.SetSeqNum(seqNumL5PointKey)
		} else if r.meta.Level == 6 {
			k.SetSeqNum(seqNumL6All)
		} else {
			panic("sharedTableIterator: a table with shared flag must have its level at 5 or 6")
		}
	}
	// check lower bound
	if k == nil || i.cmpSharedBound(k.UserKey) > 0 {
		i.setExhaustedBounds(-1)
		return nil, nil
	}
	return k, v

}

func (i *tableIterator) Prev() (*InternalKey, []byte) {
	if i.isShared() {
		return i.prevShared()
	}
	return i.iter.Prev()
}

func (i *tableIterator) Error() error {
	return i.iter.Error()
}

func (i *tableIterator) Close() error {
	return i.iter.Close()
}

func (i *tableIterator) SetBounds(lower, upper []byte) {
	i.iter.SetBounds(lower, upper)
}

func (i *tableIterator) String() string {
	return i.iter.String()
}

func (i *tableIterator) SetCloseHook(fn func(i Iterator) error) {
	i.iter.SetCloseHook(fn)
}

func (i tableIterator) Stats() base.InternalIteratorStats {
	var stats base.InternalIteratorStats
	if i.twoLevel {
		stats = i.iter.(*twoLevelIterator).stats
	} else {
		stats = i.iter.(*singleLevelIterator).stats
	}
	return stats
}

// ResetStats implements InternalIteratorWithStats.
func (i *tableIterator) ResetStats() {
	if i.twoLevel {
		i.iter.(*twoLevelIterator).stats = base.InternalIteratorStats{}
	} else {
		i.iter.(*singleLevelIterator).stats = base.InternalIteratorStats{}
	}
}

var _ base.InternalIterator = (*tableIterator)(nil)
var _ base.InternalIteratorWithStats = (*tableIterator)(nil)
var _ Iterator = (*tableIterator)(nil)
