package gelf

import (
	"bytes"
	"sync"
	"testing"
	"time"
)

func TestChunkInfo_Bytes(t *testing.T) {
	info := &chunkInfo{
		id: 1,
		cs: [][]byte{
			{0x00, 0x01},
			nil,
			{0x02},
			{0x03, 0x04},
		},
	}
	ref := []byte{0x00, 0x01, 0x02, 0x03, 0x04}
	out := info.Bytes()
	if !bytes.Equal(ref, out) {
		t.Fatal("bad bytes")
	}
	if cap(out) != len(ref) {
		t.Fatal("memory wasted")
	}
}

func TestChunkInfo_IsAllArrived(t *testing.T) {
	info := &chunkInfo{
		id: 1,
		cs: [][]byte{
			{0x00, 0x01},
			nil,
			{0x02},
			{0x03, 0x04},
		},
	}
	if info.IsAllArrived() {
		t.Fatal("bad 1")
	}
	info = &chunkInfo{
		id: 1,
		cs: [][]byte{
			{0x00, 0x01},
			{0x02},
			{0x03, 0x04},
		},
	}
	if !info.IsAllArrived() {
		t.Fatal("bad 2")
	}
}

func TestChunkPool_GC(t *testing.T) {
	now := time.Now().Unix()
	p := &chunkPool{
		max: 1000,
		store: map[uint64]*chunkInfo{
			0x01: {
				id: 0x01,
				ts: now - 2,
				cs: [][]byte{
					{0x01, 0x02},
					nil,
					{0x04},
				},
			},
			0x02: {
				id: 0x02,
				ts: now - 4,
				cs: [][]byte{
					{0x01, 0x02},
					nil,
					{0x04},
				},
			},
		},
		lock:  &sync.Mutex{},
		exp:   1,
		idExp: 2,
	}
	p.gc(now)
	if len(p.store) != 1 {
		t.Fatal("failed message id expires")
	}
	if !p.store[0x01].ex {
		t.Fatal("failed message expires")
	}
	p.gc(now)
	if len(p.store) != 1 {
		t.Fatal("things changed on re-gc with same time")
	}
	now += 2
	p.gc(now)
	if len(p.store) != 0 {
		t.Fatal("failed message id expires")
	}
}

func TestChunkPool_AssembleInternal(t *testing.T) {
	now := time.Now().Unix()
	p := &chunkPool{
		max: 1000,
		store: map[uint64]*chunkInfo{
			0x01: {
				id: 0x01,
				ts: now,
				cs: [][]byte{
					{0x01, 0x02},
					nil,
					{0x04},
				},
			},
			0x02: {
				id: 0x02,
				ts: now - 4,
				cs: [][]byte{
					{0x01, 0x02},
					nil,
					{0x04},
				},
			},
		},
		lock:  &sync.Mutex{},
		exp:   1,
		idExp: 2,
	}
	out := p.assemble(0x01, 1, 3, []byte{0x03})
	if !bytes.Equal(out, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Logf("out %v", out)
		t.Fatal("failed 1")
	}
	if len(p.store) != 0 {
		t.Fatal("failed 2")
	}
}

func TestChunkPool_Assemble(t *testing.T) {
	var ret []byte
	p := NewChunkPool(3, 1, 2).(*chunkPool)
	// add 0x01 1/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x03, 0x01})
	if ret != nil {
		t.Fatal("failed 1")
	}
	// add 0x01 3/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x02, 0x03, 0x03, 0x04})
	if ret != nil {
		t.Fatal("failed 2")
	}
	// add 0x01 2/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x01, 0x03, 0x02})
	if ret == nil {
		t.Fatal("failed 3")
	}
	if !bytes.Equal(ret, []byte{0x01, 0x02, 0x03, 0x04}) {
		t.Fatal("failed 4")
	}
	if len(p.store) != 0 {
		t.Fatal("failed 5")
	}

	// add 0x02 1/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x03, 0x01})
	if ret != nil {
		t.Fatal("failed 6")
	}
	// add 0x02 3/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x02, 0x03, 0x03})
	if ret != nil {
		t.Fatal("failed 7")
	}
	// wait two seconds, should be enough for chunkInfo expiration
	time.Sleep(2 * time.Second)
	// add 0x02 2/3
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x01, 0x03, 0x02})
	if ret != nil {
		t.Fatal("failed 8")
	}
	if len(p.store) != 1 || p.store[0x02].cs != nil || !p.store[0x02].ex {
		t.Fatal("failed 9")
	}
	// wait two more seconds, should be enough for chunkInfo id expiration
	time.Sleep(2 * time.Second)
	// add 0x03 1/1
	ret = p.Assemble([]byte{0x1e, 0x0f, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x01, 0x01, 0x02})
	if ret == nil {
		t.Fatal("failed 10")
	}
	if !bytes.Equal(ret, []byte{0x01, 0x02}) {
		t.Fatal("failed 11")
	}
	if len(p.store) != 0 {
		t.Fatal("failed 12")
	}
}
