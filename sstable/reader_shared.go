// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import "github.com/cockroachdb/pebble/internal/base"

type singleLevelIteratorS struct {
	*singleLevelIterator
}

func (i *singleLevelIteratorS) isShared() bool {
	if i.reader.meta != nil && i.reader.meta.IsShared {
		return true
	}
	return false
}

func (i *singleLevelIteratorS) cmpSharedBound(key []byte) int {
	if key == nil {
		return 0
	}
	lower := i.reader.meta.Smallest.UserKey
	upper := i.reader.meta.Largest.UserKey
	if i.cmp(key, lower) < 0 {
		return -1
	} else if i.cmp(key, upper) > 0 {
		return 1
	}
	return 0
}

func (i *singleLevelIteratorS) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		} else if ib < 0 {
			return i.singleLevelIterator.SeekGE(i.reader.meta.Smallest.UserKey, trySeekUsingNext)
		}
	}
	return i.singleLevelIterator.SeekGE(key, trySeekUsingNext)
}

func (i *singleLevelIteratorS) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		} else if ib < 0 {
			return i.singleLevelIterator.SeekPrefixGE(prefix, i.reader.meta.Smallest.UserKey, trySeekUsingNext)
		}
	}
	return i.singleLevelIterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func (i *singleLevelIteratorS) SeekLT(key []byte) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			return i.singleLevelIterator.SeekLT(i.reader.meta.Largest.UserKey)
		} else if ib < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
	}
	return i.singleLevelIterator.SeekLT(key)
}

func (i *singleLevelIteratorS) First() (*InternalKey, []byte) {
	k, v := i.singleLevelIterator.First()
	if i.isShared() {
		// check lower bound
		if i.cmpSharedBound(k.UserKey) < 0 {
			k, v = i.singleLevelIterator.SeekGE(i.reader.meta.Smallest.UserKey, true)
		}
	}
	return k, v
}

func (i *singleLevelIteratorS) Last() (*InternalKey, []byte) {
	k, v := i.singleLevelIterator.Last()
	if i.isShared() {
		// check upper bound
		if i.cmpSharedBound(k.UserKey) > 0 {
			k, v = i.singleLevelIterator.SeekLT(i.reader.meta.Largest.UserKey)
		}
	}
	return k, v
}

func (i *singleLevelIteratorS) Next() (*InternalKey, []byte) {
	k, v := i.singleLevelIterator.Next()
	if i.isShared() {
		if i.cmpSharedBound(k.UserKey) > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		}
	}
	return k, v
}

func (i *singleLevelIteratorS) Prev() (*InternalKey, []byte) {
	k, v := i.singleLevelIterator.Prev()
	if i.isShared() {
		if i.cmpSharedBound(k.UserKey) < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
	}
	return k, v
}

func (i *singleLevelIteratorS) Error() error {
	return i.singleLevelIterator.Error()
}

func (i *singleLevelIteratorS) Close() error {
	return i.singleLevelIterator.Close()
}

func (i *singleLevelIteratorS) SetBounds(lower, upper []byte) {
	i.singleLevelIterator.SetBounds(lower, upper)
}

func (i *singleLevelIteratorS) String() string {
	return i.singleLevelIterator.String()
}

func (i *singleLevelIteratorS) SetCloseHook(fn func(i Iterator) error) {
	i.singleLevelIterator.SetCloseHook(fn)
}

// Both InternalIterator and Iterator interfaces are implemented
var _ base.InternalIterator = (*singleLevelIteratorS)(nil)
var _ Iterator = (*singleLevelIteratorS)(nil)

type twoLevelIteratorS struct {
	*twoLevelIterator
}

// mirror of singleLevelIterator
func (i *twoLevelIteratorS) isShared() bool {
	if i.reader.meta != nil && i.reader.meta.IsShared {
		return true
	}
	return false
}

// mirror of singleLevelIterator
func (i *twoLevelIteratorS) cmpSharedBound(key []byte) int {
	if key == nil {
		return 0
	}
	lower := i.reader.meta.Smallest.UserKey
	upper := i.reader.meta.Largest.UserKey
	if i.cmp(key, lower) < 0 {
		return -1
	} else if i.cmp(key, upper) > 0 {
		return 1
	}
	return 0
}

func (i *twoLevelIteratorS) SeekGE(key []byte, trySeekUsingNext bool) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		} else if ib < 0 {
			return i.twoLevelIterator.SeekGE(i.reader.meta.Smallest.UserKey, trySeekUsingNext)
		}
	}
	return i.twoLevelIterator.SeekGE(key, trySeekUsingNext)
}

func (i *twoLevelIteratorS) SeekPrefixGE(
	prefix, key []byte, trySeekUsingNext bool,
) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		} else if ib < 0 {
			return i.twoLevelIterator.SeekPrefixGE(prefix, i.reader.meta.Smallest.UserKey, trySeekUsingNext)
		}
	}
	return i.twoLevelIterator.SeekPrefixGE(prefix, key, trySeekUsingNext)
}

func (i *twoLevelIteratorS) SeekLT(key []byte) (*InternalKey, []byte) {
	if i.isShared() {
		ib := i.cmpSharedBound(key)
		if ib > 0 {
			return i.twoLevelIterator.SeekLT(i.reader.meta.Largest.UserKey)
		} else if ib < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
	}
	return i.twoLevelIterator.SeekLT(key)
}

func (i *twoLevelIteratorS) First() (*InternalKey, []byte) {
	k, v := i.twoLevelIterator.First()
	if i.isShared() {
		// check lower bound
		if i.cmpSharedBound(k.UserKey) < 0 {
			k, v = i.twoLevelIterator.SeekGE(i.reader.meta.Smallest.UserKey, true)
		}
	}
	return k, v
}

func (i *twoLevelIteratorS) Last() (*InternalKey, []byte) {
	k, v := i.twoLevelIterator.Last()
	if i.isShared() {
		// check upper bound
		if i.cmpSharedBound(k.UserKey) > 0 {
			k, v = i.twoLevelIterator.SeekLT(i.reader.meta.Largest.UserKey)
		}
	}
	return k, v
}

func (i *twoLevelIteratorS) Next() (*InternalKey, []byte) {
	k, v := i.twoLevelIterator.Next()
	if i.isShared() {
		if i.cmpSharedBound(k.UserKey) > 0 {
			i.exhaustedBounds = +1
			return nil, nil
		}
	}
	return k, v
}

func (i *twoLevelIteratorS) Prev() (*InternalKey, []byte) {
	k, v := i.twoLevelIterator.Prev()
	if i.isShared() {
		if i.cmpSharedBound(k.UserKey) < 0 {
			i.exhaustedBounds = -1
			return nil, nil
		}
	}
	return k, v
}

func (i *twoLevelIteratorS) Error() error {
	return i.twoLevelIterator.Error()
}

func (i *twoLevelIteratorS) Close() error {
	return i.twoLevelIterator.Close()
}

func (i *twoLevelIteratorS) SetBounds(lower, upper []byte) {
	i.twoLevelIterator.SetBounds(lower, upper)
}

func (i *twoLevelIteratorS) String() string {
	return i.twoLevelIterator.String()
}

func (i *twoLevelIteratorS) SetCloseHook(fn func(i Iterator) error) {
	i.twoLevelIterator.SetCloseHook(fn)
}

// Both InternalIterator and Iterator interfaces are implemented
var _ base.InternalIterator = (*twoLevelIteratorS)(nil)
var _ Iterator = (*twoLevelIteratorS)(nil)
