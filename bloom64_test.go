package bloom

import (
	"strconv"
	"testing"
)

func TestFilter64(t *testing.T) {
	f := New(3000, 0.01)
	f.Add(foo)
	f.Add(bar)
	if !f.Test(foo) {
		t.Error("foo not in bloom filter")
	}
	if !f.Test(bar) {
		t.Error("bar not in bloom filter")
	}
	if f.Test(baz) {
		t.Error("baz in bloom filter")
	}
}

func TestCountingFilter64(t *testing.T) {
	f := NewCounting(3000, 0.01)
	f.Add(foo)
	f.Add(foo)
	f.Remove(foo)
	if !f.Test(foo) {
		t.Error("foo not in bloom filter")
	}
	f.Remove(foo)
	if f.Test(foo) {
		t.Error("foo still in bloom filter")
	}
}

func TestLayeredFilter64(t *testing.T) {
	layers := 5
	f := NewLayered(3000, 0.01)
	for i := 0; i < layers; i++ {
		if n := f.Add(foo); n != i+1 {
			t.Errorf("add %d (layer %d): n %d", i, i+1, n)
		}
		if n, ok := f.Test(foo); n != i+1 || !ok {
			t.Errorf("test %d (layer %d): n %d, ok %v", i, i+1, n, ok)
		}
	}
}

func BenchmarkFilterAdd64(b *testing.B) {
	b.StopTimer()
	f := New64(int64(b.N), 0.01)
	datas := make([][]byte, b.N)
	for i := range datas {
		datas[i] = []byte(strconv.Itoa(i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.Add(datas[i])
	}
}

func BenchmarkFilterAddExisting64(b *testing.B) {
	b.StopTimer()
	f := New64(int64(b.N), 0.01)
	f.Add(foo)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.Add(foo)
	}
}
