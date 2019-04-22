package chunk

import (
	"bytes"
	"context"
	"io"
	"math/rand"
	"testing"

	"github.com/pachyderm/pachyderm/src/client/pkg/require"
)

var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

// generates random sequence of data (n is number of bytes)
func randSeq(n int) []byte {
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return []byte(string(b))
}

func TestWriteThenRead(t *testing.T) {
	objC, chunks := LocalStorage(t)
	defer func() {
		require.NoError(t, chunks.Clear(context.Background()))
		require.NoError(t, objC.Delete(context.Background(), Prefix))
	}()
	var finalDataRefs []*DataRef
	w := chunks.NewWriter(context.Background())
	cb := func(dataRefs []*DataRef) error {
		finalDataRefs = append(finalDataRefs, dataRefs...)
		return nil
	}
	seq := randSeq(100 * MB)
	for i := 0; i < 100; i++ {
		w.RangeStart(cb)
		_, err := w.Write(seq[i*MB : (i+1)*MB])
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	buf := &bytes.Buffer{}
	r := chunks.NewReader(context.Background(), finalDataRefs)
	_, err := io.Copy(buf, r)
	require.NoError(t, err)
	require.Equal(t, bytes.Compare(buf.Bytes(), seq), 0)
}

func BenchmarkWriter(b *testing.B) {
	objC, chunks := LocalStorage(b)
	defer func() {
		require.NoError(b, chunks.Clear(context.Background()))
		require.NoError(b, objC.Delete(context.Background(), Prefix))
	}()
	seq := randSeq(100 * MB)
	b.SetBytes(100 * MB)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w := chunks.NewWriter(context.Background())
		cb := func(dataRefs []*DataRef) error { return nil }
		for i := 0; i < 100; i++ {
			w.RangeStart(cb)
			_, err := w.Write(seq[i*MB : (i+1)*MB])
			require.NoError(b, err)
		}
		require.NoError(b, w.Close())
	}
}
