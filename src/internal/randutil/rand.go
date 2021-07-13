package randutil

import (
	"io"
	"math/rand"

	"modernc.org/mathutil"
)

var letters = []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// Bytes generates random bytes (n is number of bytes)
func Bytes(random *rand.Rand, n int) []byte {
	bs := make([]byte, n)
	for i := range bs {
		bs[i] = letters[random.Intn(len(letters))]
	}
	return bs
}

type bytesReader struct {
	random *rand.Rand
	n      int64
}

// NewBytesReader creates a reader that generates random bytes (n is number of bytes)
func NewBytesReader(random *rand.Rand, n int64) *bytesReader {
	return &bytesReader{
		random: random,
		n:      n,
	}
}

func (br *bytesReader) Read(data []byte) (int, error) {
	size := int(mathutil.MinInt64(br.n, int64(len(data))))
	for i := 0; i < size; i++ {
		data[i] = letters[br.random.Intn(len(letters))]
	}
	br.n -= int64(size)
	if br.n == 0 {
		return size, io.EOF
	}
	return size, nil
}
