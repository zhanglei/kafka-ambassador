package kafka

import (
	"fmt"
	"net"
)

type P struct {
	FromAddr string
	ToAddr   string
	srcLn    net.Listener
	dstConn  net.Conn
	lastID   uint
	conns    map[uint]net.Conn
	closed   bool
}

func (p *P) GetNextID() uint {
	if p.lastID >= ^uint(0) {
		p.lastID = 0
	}
	p.lastID++
	return p.lastID
}

func (p *P) Run() error {
	p.closed = false
	p.lastID = 0
	p.conns = map[uint]net.Conn{}
	ln, err := net.Listen("tcp", p.FromAddr)
	if err != nil {
		return err
	}
	p.srcLn = ln

	dstConn, err := net.Dial("tcp", p.ToAddr)
	if err != nil {
		return err
	}
	p.dstConn = dstConn

	go func() {
		for {
			if p.closed {
				break
			}
			conn, err := ln.Accept()
			if err != nil {
				fmt.Printf("Could not accept connection: %v", err)
			}
			p.conns[p.GetNextID()] = conn
			go p.handleConnection(conn)
		}
	}()
	return nil
}

func (p *P) handleConnection(c net.Conn) {
	for {
		buf := make([]byte, 1024)
		n, err := c.Read(buf)
		if err != nil {
			fmt.Printf("Error reading from client connection: %v\n", err)
			return
		} else {
			_, err := p.dstConn.Write(buf[:n])
			if err != nil {
				fmt.Println("Error writing to dst connection")
				return
			}
		}
	}
}

func (p *P) Close() {
	p.closed = true
	for _, c := range p.conns {
		c.Close()
	}
}
