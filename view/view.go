package view

import (
	"io"
	"time"
)

type View interface {
	Len() int64
	Expire() time.Time
	Reader() (io.ReadCloser, error)
	Type() int
}

const (
	FileType = iota
	ByteType
	ReaderType
)
