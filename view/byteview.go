/*
Copyright 2012 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package view

import (
	"bytes"
	"io"
	"io/ioutil"
	"time"
)

func NewByteView(bytes []byte, ttl time.Duration) ByteView {
	return ByteView{
		b:   bytes,
		ttl: ttl,
	}
}

// A ByteView holds an immutable view of bytes.
// Internally it wraps either a []byte or a string,
// but that detail is invisible to callers.
type ByteView struct {
	// If b is non-nil, b is used, else s is used.
	b   []byte
	ttl time.Duration
}

// Returns the expire time associated with this view
func (v ByteView) Expire() time.Duration {
	return v.ttl
}

// Len returns the view's length.
func (v ByteView) Len() int64 {
	return int64(len(v.b))
}

func (v ByteView) Type() int {
	return ByteType
}

// Reader returns an io.ReadSeeker for the bytes in v.
func (v ByteView) Reader() (io.ReadCloser, error) {
	return ioutil.NopCloser(bytes.NewReader(v.b)), nil
}

// WriteTo implements io.WriterTo on the bytes in v.
func (v ByteView) WriteTo(w io.Writer) (n int64, err error) {
	i, err := w.Write(v.b)
	return int64(i), err
}
