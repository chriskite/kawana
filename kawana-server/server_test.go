package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/chriskite/kawana/datastore"

	. "github.com/chriskite/kawana/kawana-server/Godeps/_workspace/src/gopkg.in/check.v1"
)

// Hook up gocheck into the "go test" runner.
func Test(t *testing.T) { TestingT(t) }

type ServerS struct{}

var _ = Suite(&ServerS{})

type faker struct {
	io.ReadWriter
}

func (f faker) Close() error                     { return nil }
func (f faker) LocalAddr() net.Addr              { return nil }
func (f faker) RemoteAddr() net.Addr             { return nil }
func (f faker) SetDeadline(time.Time) error      { return nil }
func (f faker) SetReadDeadline(time.Time) error  { return nil }
func (f faker) SetWriteDeadline(time.Time) error { return nil }

func (s *ServerS) TestBlackWhiteIP(c *C) {
	var ip uint32 = 1
	impact := datastore.ImpactAmount(0)
	bw := byte(datastore.BWWhitelist)

	var cmdBuf [5]byte
	binary.LittleEndian.PutUint32(cmdBuf[0:4], ip)
	cmdBuf[4] = bw

	expected := datastore.IPData{
		MaxImpacts: datastore.ImpactAmounts{
			FiveMin: impact,
			Hour:    impact,
			Day:     impact,
		},
		Forgiven:   datastore.ForgivenNum(0),
		BlackWhite: bw,
	}
	helpTestCommand(c, cmdBuf[0:], expected, func(s *Server, f faker) {
		s.handleBlackWhiteIP(f)
	})
}

func (s *ServerS) TestLogIP(c *C) {
	var ip uint32 = 1
	impact := datastore.ImpactAmount(2)

	var cmdBuf [8]byte
	binary.LittleEndian.PutUint32(cmdBuf[0:4], ip)
	binary.LittleEndian.PutUint32(cmdBuf[4:8], uint32(impact))

	expected := datastore.IPData{
		MaxImpacts: datastore.ImpactAmounts{
			FiveMin: impact,
			Hour:    impact,
			Day:     impact,
		},
		Forgiven:   datastore.ForgivenNum(0),
		BlackWhite: byte(0),
	}
	helpTestCommand(c, cmdBuf[0:], expected, func(s *Server, f faker) {
		s.handleLogIP(f)
	})
}

func (s *ServerS) TestForgiveIP(c *C) {
	var ip uint32 = 1
	impact := datastore.ImpactAmount(2)
	exp := datastore.ImpactAmount(0)

	var cmdBuf [16]byte
	binary.LittleEndian.PutUint32(cmdBuf[0:4], ip)
	binary.LittleEndian.PutUint32(cmdBuf[4:8], uint32(impact))
	binary.LittleEndian.PutUint32(cmdBuf[8:12], uint32(impact))
	binary.LittleEndian.PutUint32(cmdBuf[12:16], uint32(impact))

	expected := datastore.IPData{
		MaxImpacts: datastore.ImpactAmounts{
			FiveMin: exp,
			Hour:    exp,
			Day:     exp,
		},
		Forgiven:   datastore.ForgivenNum(0),
		BlackWhite: byte(0),
	}
	helpTestCommand(c, cmdBuf[0:], expected, func(s *Server, f faker) {
		s.handleForgiveIP(f)
	})
}

func helpTestCommand(c *C, cmdBuf []byte, expected datastore.IPData, cmd func(s *Server, f faker)) {
	var respBuf bytes.Buffer
	bRespBuf := bufio.NewWriter(&respBuf)
	var fake faker
	fake.ReadWriter = bufio.NewReadWriter(bufio.NewReader(bytes.NewReader(cmdBuf[0:])), bRespBuf)

	server := New(9291, "/tmp", 0, 0, "")

	cmd(server, fake)

	bRespBuf.Flush()
	checkResponse(&respBuf, expected, c)
}

func checkResponse(respBuf *bytes.Buffer, expected datastore.IPData, c *C) {
	var buf [15]byte
	io.ReadFull(bufio.NewReader(respBuf), buf[0:])

	fiveMinImpact := datastore.ImpactAmount(binary.LittleEndian.Uint32(buf[0:4]))
	hourImpact := datastore.ImpactAmount(binary.LittleEndian.Uint32(buf[4:8]))
	dayImpact := datastore.ImpactAmount(binary.LittleEndian.Uint32(buf[8:12]))
	forgiven := datastore.ForgivenNum(binary.LittleEndian.Uint16(buf[12:14]))
	bw := buf[14]

	c.Check(fiveMinImpact, Equals, expected.MaxImpacts.FiveMin)
	c.Check(hourImpact, Equals, expected.MaxImpacts.Hour)
	c.Check(dayImpact, Equals, expected.MaxImpacts.Day)
	c.Check(forgiven, Equals, expected.Forgiven)
	c.Check(bw, Equals, expected.BlackWhite)
}
