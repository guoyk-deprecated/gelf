package gelf

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"testing"
)

func TestCompressionNone(t *testing.T) {
	var err error
	buf := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}
	t.Logf("raw: % 02x", buf)
	if DetectCompression(buf) != CompressionNone {
		t.Fatal("failed to detect gzip compression")
	}
	var res []byte
	if res, err = Decompress(buf); err != nil {
		t.Fatal("failed to resolve none compression")
	}
	if !bytes.Equal(res, buf) {
		t.Fatal("bad decompression")
	}
}

func TestCompressionGzip(t *testing.T) {
	var err error
	buf := []byte{0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a}
	out := &bytes.Buffer{}
	w := gzip.NewWriter(out)
	if _, err = w.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("compressed: % 02x", out.Bytes())
	if DetectCompression(out.Bytes()) != CompressionGzip {
		t.Fatal("failed to detect gzip compression")
	}
	var res []byte
	if res, err = Decompress(out.Bytes()); err != nil {
		t.Fatal("failed to resolve gzip compression")
	}
	if !bytes.Equal(res, buf) {
		t.Fatal("bad decompression")
	}
}

func TestCompressionZlib(t *testing.T) {
	var err error
	buf := []byte{0x01, 0x02, 0x03, 0x06, 0x05, 0x06, 0x07, 0x04, 0x09, 0x0a}
	out := &bytes.Buffer{}
	w := zlib.NewWriter(out)
	if _, err = w.Write(buf); err != nil {
		t.Fatal(err)
	}
	if err = w.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("compressed: % 02x", out.Bytes())
	if DetectCompression(out.Bytes()) != CompressionZlib {
		t.Fatal("failed to detect zlib compression")
	}
	var res []byte
	if res, err = Decompress(out.Bytes()); err != nil {
		t.Fatal("failed to resolve gzip compression")
	}
	if !bytes.Equal(res, buf) {
		t.Fatal("bad decompression")
	}
}
