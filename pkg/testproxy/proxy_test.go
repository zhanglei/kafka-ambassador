package testproxy

import (
	"github.com/stretchr/testify/assert"
	"net"
	"testing"
)

func TestProxyWithBounce(t *testing.T) {
	assert := assert.New(t)
	ln, err := net.Listen("tcp", "localhost:23456")
	assert.NoError(err, "Could not create bouncer listener")

	go func() {
		conn, err := ln.Accept()
		assert.NoErrorf(err, "Could not accept connection: %v", err)
		for {
			data := make([]byte, 1024)
			n, err := conn.Read(data)
			if n > 0 {
				_, err = conn.Write(data[:n])
			}
			if err != nil {
				return
			}
		}
	}()
	proxy := T{FromAddr: "localhost:12345", ToAddr: "localhost:23456"}
	proxy.Run()

	c, err := net.Dial("tcp", "localhost:12345")
	assert.NoErrorf(err, "Error connecting to proxy: %v", err)

	b := []byte("This is a test string")
	n, err := c.Write(b)
	assert.NoErrorf(err, "Error writing to proxy conn: %v", err)
	data := make([]byte, 1024)
	n, err = c.Read(data)
	assert.NoErrorf(err, "Error reading from proxy conn: %v", err)
	assert.Equal(b, data[:n], "Did not receive exact copy of the sent data")
	proxy.Close()
	n, err = c.Read(b)
	assert.Errorf(err, "Read from closed proxy should raise and error: %v", err)

}
