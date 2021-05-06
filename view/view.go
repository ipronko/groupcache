package view

import (
	"bytes"
	"io"
	"time"
)

func NewView(r io.Reader, ttl time.Duration) *View {
	return &View{
		r:   r,
		ttl: ttl,
	}
}

type View struct {
	r   io.Reader
	ttl time.Duration
}

func (f *View) Read(b []byte) (int, error) {
	return f.r.Read(b)
}

func (f *View) Close() error {
	if rc, ok := f.r.(io.ReadCloser); ok {
		return rc.Close()
	}
	return nil
}

func (f *View) Expire() time.Duration {
	return f.ttl
}

func (f *View) SwapReader(r io.Reader) io.Reader {
	oldRC := f.r
	f.r = r
	return oldRC
}

func (f *View) BytesBuffer() (*bytes.Buffer, bool) {
	if buf, ok := f.r.(*bytes.Buffer); ok {
		return buf, true
	}
	return nil, false
}
