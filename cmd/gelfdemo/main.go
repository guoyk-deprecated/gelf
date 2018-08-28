package main

import (
	"encoding/json"
	"github.com/yankeguo/gelf"
	"log"
	"net"
)

type demoHandler int

func (demoHandler) HandleGELFPayload(addr net.Addr, data []byte) {
	var err error
	out := map[string]interface{}{}
	if err = json.Unmarshal(data, &out); err != nil {
		log.Println("failed to unmarshal JSON:", err)
		return
	}
	data, _ = json.MarshalIndent(&out, "", "  ")
	log.Println("event from", addr.String(), ":\n", string(data))
}

func main() {
	var err error
	var addr *net.UDPAddr
	if addr, err = net.ResolveUDPAddr("udp", ":12201"); err != nil {
		log.Println(err)
		return
	}
	var conn *net.UDPConn
	if conn, err = net.ListenUDP("udp", addr); err != nil {
		log.Println(err)
		return
	}
	defer conn.Close()
	gelf.Serve(conn, gelf.NewChunkPool(1000, 5, 5), demoHandler(0))
}
