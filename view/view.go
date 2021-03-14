package view

import (
	"io"
	"time"
)

type View interface {
	Len() int64
	Expire() time.Duration
	Reader() (io.ReadCloser, error)
	Type() int
}

const (
	ByteType = iota
	ReaderType
)
