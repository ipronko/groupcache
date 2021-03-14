package view

import (
	"io"
	"time"
)

func NewReaderView(rc io.ReadCloser, size int64, ttl time.Duration) *ReaderView {
	return &ReaderView{
		rc:   rc,
		size: size,
		ttl:  ttl,
	}
}

type ReaderView struct {
	rc   io.ReadCloser
	size int64
	ttl  time.Duration
}

func (f *ReaderView) Reader() (io.ReadCloser, error) {
	return f.rc, nil
}

// Returns the expire time associated with this view
func (f *ReaderView) Expire() time.Duration {
	return f.ttl
}

func (f *ReaderView) Len() int64 {
	return f.size
}

func (f *ReaderView) Type() int {
	return ReaderType
}

func (f *ReaderView) SwapReader(r io.ReadCloser) {
	f.rc = r
}
