package view

import (
	"io"
	"time"
)

func NewReaderView(rc io.ReadCloser, size int64, exp time.Time) *ReaderView {
	return &ReaderView{
		rc:   rc,
		size: size,
		e:    exp,
	}
}

type ReaderView struct {
	rc   io.ReadCloser
	size int64
	e    time.Time
}

func (f *ReaderView) Reader() (io.ReadCloser, error) {
	return f.rc, nil
}

// Returns the expire time associated with this view
func (f *ReaderView) Expire() time.Time {
	return f.e
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
