// Copyright 2022 The LevelDB-Go and Pebble Authors. All rights reserved. Use
// of this source code is governed by a BSD-style license that can be found in
// the LICENSE file.

package sstable

import (
	"os"
	"testing"

	"github.com/cockroachdb/pebble/internal/base"
	"github.com/cockroachdb/pebble/internal/cache"
	"github.com/cockroachdb/pebble/internal/manifest"
	"github.com/cockroachdb/pebble/vfs"
	"github.com/stretchr/testify/require"
)

func TestMain(m *testing.M) {
	// check DBUniqueID == 0, otherwise the testing will fail
	if DBUniqueID != 0 {
		panic("shared_test.go: DBUniqueID != 0. It might be injected by a db.Open()")
	}
	code := m.Run()
	os.Exit(code)
}

func TestSharedSST(t *testing.T) {
	t.Logf("Start TestSharedSST")
	mem := vfs.NewMem()
	f0, err := mem.Create("test")
	require.NoError(t, err)

	w := NewWriter(f0, WriterOptions{})

	// Insert the following kv:
	//   a#1,DEL
	//   a#0,SET - foo
	//   b#0,SET - foo
	//   c#1,SET - foo
	//   c#0,SET - bar
	//   d#0,SET - foo
	//   e#0,SET - foo
	//   e-f#1,RANGEDEL
	//   If the reader treats this table as purely foreign, it can only see keys
	//   b, c and d with value "foo" (only latest version, and also considering rangedels)
	//   For the creator, it can see all the keys
	kvPairs := []struct {
		k      []byte
		v      []byte
		seqNum uint64
		kind   InternalKeyKind
	}{
		{[]byte("a"), []byte{}, 1, InternalKeyKindDelete},
		{[]byte("a"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("b"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("c"), []byte("foo"), 1, InternalKeyKindSet},
		{[]byte("c"), []byte("bar"), 0, InternalKeyKindSet},
		{[]byte("d"), []byte("foo"), 0, InternalKeyKindSet},
		{[]byte("e"), []byte("foo"), 0, InternalKeyKindSet},
	}
	for i := range kvPairs {
		w.addPoint(base.MakeInternalKey(kvPairs[i].k, kvPairs[i].seqNum, kvPairs[i].kind), kvPairs[i].v)
	}
	w.addTombstone(base.MakeInternalKey([]byte("e"), 1, InternalKeyKindRangeDelete), []byte("f"))

	require.NoError(t, w.Close())
	t.Logf("Table writing finished")

	f1, err := mem.Open("test")
	require.NoError(t, err)

	c := cache.New(128 << 10)
	defer c.Unref()
	r, err := NewReader(f1, ReaderOptions{
		Cache: c,
	}, FileReopenOpt{
		FS:       mem,
		Filename: "test",
	})
	require.NoError(t, err)
	// local table
	meta := &manifest.FileMetadata{
		IsShared:        true,
		CreatorUniqueID: 0,
		Smallest:        InternalKey{UserKey: []byte("a"), Trailer: 0},
		Largest:         InternalKey{UserKey: []byte("e"), Trailer: 0},
	}
	r.meta = meta
	require.Equal(t, DBUniqueID, uint32(0))

	iter, err := r.NewIter(nil, nil)
	require.NoError(t, err)
	require.NotEqual(t, iter, nil)
	iter.SetLevel(5)

	t.Logf("Read as locally created shared table")
	i := 0
	for k, v := iter.First(); k != nil; k, v = iter.Next() {
		t.Logf("  - %s %s", k, v)
		require.Equal(t, *k, base.MakeInternalKey(kvPairs[i].k, kvPairs[i].seqNum, kvPairs[i].kind))
		require.Equal(t, v, kvPairs[i].v)
		i++
	}
	require.NoError(t, iter.Close())

	t.Logf("Read as remotely created shared table")
	r.meta.CreatorUniqueID = 1
	iter, err = r.NewIter(nil, nil)
	require.NoError(t, err)
	require.NotEqual(t, iter, nil)
	require.NotEqual(t, iter.(*tableIterator).rangeDelIter, nil)
	iter.SetLevel(5)

	// b#2,SET
	k, v := iter.First()
	t.Logf("  - %s %s", k, v)
	require.Equal(t, *k, base.MakeInternalKey(kvPairs[2].k, seqNumL5PointKey, InternalKeyKindSet))
	require.Equal(t, v, kvPairs[2].v)
	// c#2,SET
	k, v = iter.Next()
	t.Logf("  - %s %s", k, v)
	require.Equal(t, *k, base.MakeInternalKey(kvPairs[3].k, seqNumL5PointKey, InternalKeyKindSet))
	require.Equal(t, v, kvPairs[3].v)
	// d#2,SET
	k, v = iter.Next()
	t.Logf("  - %s %s", k, v)
	require.Equal(t, *k, base.MakeInternalKey(kvPairs[5].k, seqNumL5PointKey, InternalKeyKindSet))
	require.Equal(t, v, kvPairs[5].v)
	// nil
	k, v = iter.Next()
	if k != nil || v != nil {
		t.Fatalf("shared iter should have reached end")
	}

	require.NoError(t, iter.Close())

	require.NoError(t, r.Close())
}
