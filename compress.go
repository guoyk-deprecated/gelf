package gelf

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"io"
	"io/ioutil"
)

const (
	CompressionNone = CompressionType(iota)
	CompressionGzip
	CompressionZlib
)

type CompressionType uint

// IsGzip detect if buf is a gzip compressed data
// see https://tools.ietf.org/html/rfc1952
func IsGzip(buf []byte) bool {
	return len(buf) > 2 && (buf[0] == 0x1F) && (buf[1] == 0x8B) && (buf[2] == 0x08)
}

// IsZlib detect if buf is a zlib compressed data
// see https://tools.ietf.org/html/rfc1950
func IsZlib(buf []byte) bool {
	return len(buf) > 2 && (buf[0]&0xF8 == buf[0]) && ((uint(buf[0])*256+uint(buf[1]))%31 == 0)
}

func DetectCompression(data []byte) CompressionType {
	if IsGzip(data) {
		return CompressionGzip
	}
	if IsZlib(data) {
		return CompressionZlib
	}
	return CompressionNone
}

func Decompress(data []byte) (out []byte, err error) {
	switch DetectCompression(data) {
	case CompressionNone:
		out = data
	case CompressionGzip:
		var r io.ReadCloser
		if r, err = gzip.NewReader(bytes.NewReader(data)); err != nil {
			return
		}
		defer r.Close()
		return ioutil.ReadAll(r)
	case CompressionZlib:
		var r io.ReadCloser
		if r, err = zlib.NewReader(bytes.NewReader(data)); err != nil {
			return
		}
		defer r.Close()
		return ioutil.ReadAll(r)
	}
	return
}
