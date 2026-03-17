package writer

import "sync"

// bufPool is a pool of byte slices used by encoders to avoid allocations.
var bufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 512)
		return &b
	},
}

// getBuf retrieves a buffer from the pool.
func getBuf() *[]byte {
	b := bufPool.Get().(*[]byte)
	*b = (*b)[:0]
	return b
}

// putBuf returns a buffer to the pool.
func putBuf(b *[]byte) {
	if cap(*b) > 64*1024 {
		// Don't pool very large buffers.
		return
	}
	bufPool.Put(b)
}
