package gelf

import (
	"encoding/binary"
	"sync"
	"time"
)

// IsChunkedPayload check payload is a chunked payload
func IsChunkedPayload(data []byte) bool {
	return len(data) >= 12 && data[0] == 0x1E && data[1] == 0x0F
}

// ChunkPool tool to manage and assemble chunked payloads
type ChunkPool interface {
	// Assemble accepts a chunked payload, returns a assembled payload if all chunks arrived
	Assemble([]byte) []byte
}

// NewChunkPool create a new chunk pool, with max allowed count, expires time in seconds and id expires time in seconds
func NewChunkPool(max int, exp int64, idExp int64) ChunkPool {
	return &chunkPool{
		store: map[uint64]*chunkInfo{},
		lock:  &sync.Mutex{},
		max:   max,
		exp:   exp,
		idExp: idExp,
	}
}

type chunkInfo struct {
	id uint64
	ts int64    // last timestamp
	cs [][]byte // chunks
	ex bool     // this chunkInfo is expired, cs == nil and new arriving chunks should be also discarded
}

func (i *chunkInfo) IsAllArrived() bool {
	// if chunks has a nil pointer, means some chunks is missing
	for _, c := range i.cs {
		if c == nil {
			return false
		}
	}
	return true
}

func (i *chunkInfo) Bytes() []byte {
	// calculate total length
	l := 0
	for _, c := range i.cs {
		l += len(c)
	}
	// make a whole buf
	buf := make([]byte, l, l)
	// copy chunk payloads
	cur := buf
	for _, c := range i.cs {
		// copy
		copy(cur, c)
		// move cursor
		cur = cur[len(c):]
	}
	return buf
}

type chunkPool struct {
	max   int
	store map[uint64]*chunkInfo
	lock  *sync.Mutex
	exp   int64
	idExp int64
}

func (p *chunkPool) gc(now int64) {
	if p.exp == 0 {
		return
	}
	// check expires
	for _, c := range p.store {
		// if expired
		if c.ts+p.exp < now {
			// remove all chunks, free memory
			c.cs = nil
			// marked as expired
			c.ex = true
		}
	}
	if p.idExp == 0 {
		return
	}
	// check message id expired
	tm := []uint64{}
	for _, c := range p.store {
		// if chunkInfo expired and chunkInfo message id is also expired
		if c.ex && c.ts+p.exp+p.idExp < now {
			// mark to delete
			tm = append(tm, c.id)
		}
	}
	for _, id := range tm {
		delete(p.store, id)
	}
}

func (p *chunkPool) assemble(id uint64, n int, c int, pl []byte) (out []byte) {
	now := time.Now().Unix()
	// check expired chunkInfo and remove chunkInfo with message id expired
	p.gc(now)
	// find existing chunkInfo
	info := p.store[id]
	// create new chunkInfo
	if info == nil {
		// discard if store exceeded max
		if p.max > 0 && len(p.store) > p.max {
			return
		}
		// create new chunkInfo, allocate chunks
		info = &chunkInfo{id: id, cs: make([][]byte, c, c)}
		// save to store
		p.store[id] = info
	}
	// if chunkInfo is expired, ignore; this chunkInfo will later be removed by gc()
	if info.ex {
		return
	}
	// ignore if sequence count mismatch
	if c != len(info.cs) {
		return
	}
	// clone payload and set to chunks, and update ts
	buf := make([]byte, len(pl), len(pl))
	copy(buf, pl)
	info.cs[n] = buf
	info.ts = now
	// check if all chunks arrived
	if info.IsAllArrived() {
		// remove chunkInfo from store
		delete(p.store, id)
		// return combined payloads
		return info.Bytes()
	}
	return
}

func (p *chunkPool) Assemble(data []byte) []byte {
	// ensure it's a chunked payload
	if !IsChunkedPayload(data) {
		return nil
	}
	// extra variables
	id := binary.BigEndian.Uint64(data[2:10])
	i, n, pl := data[10], data[11], data[12:]
	// ignore invalid sequence number / sequence count
	if n == 0 || i >= n {
		return nil
	}
	// locking
	p.lock.Lock()
	defer p.lock.Unlock()
	return p.assemble(id, int(i), int(n), pl)
}
