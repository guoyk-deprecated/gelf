package gelf

import "net"

const (
	PacketMaxSize = 8192
)

type Handler interface {
	// HandleGELFPayload handles a uncompressed and reassembled GELF message (basically a JSON)
	// the variable 'data' is reused, REMEMBER to make a copy if you want to actually process the data in a separated goroutine
	HandleGELFPayload(addr net.Addr, data []byte)
}

// Serve serve a net.PacketConn, you run multiple Serve() on same net.PacketConn with the same ChunkPool
func Serve(l net.PacketConn, cp ChunkPool, h Handler) (err error) {
	buf := make([]byte, PacketMaxSize, PacketMaxSize)
	for {
		// read a payload
		var n int
		var addr net.Addr
		if n, addr, err = l.ReadFrom(buf); err != nil {
			return
		}
		if n == 0 {
			continue
		}
		// decompress payload
		var pl []byte
		if pl, err = Decompress(buf[:n]); err != nil {
			return
		}
		// assemble payload
		if IsChunkedPayload(pl) {
			pl = cp.Assemble(pl)
		}
		// skip if payload is empty (or chunks no yet assembled)
		if len(pl) > 0 {
			h.HandleGELFPayload(addr, pl)
		}
	}
	return
}
