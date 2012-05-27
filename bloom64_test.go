package bloom

import (
	"github.com/pmylund/go-bitset"

	"encoding/binary"
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

func TestBasicUint64(t *testing.T) {
	f := New(1000, 0.0001)
	n1 := make([]byte, 8)
	n2 := make([]byte, 8)
	n3 := make([]byte, 8)
	binary.BigEndian.PutUint64(n1, 100)
	binary.BigEndian.PutUint64(n2, 101)
	binary.BigEndian.PutUint64(n3, 102)
	f.Add(n1)
	n1b := f.Test(n1)
	n2b := f.Test(n2)
	f.Test(n3)
	if !n1b {
		t.Errorf("%v should be in.", n1)
	}
	if n2b {
		t.Errorf("%v should not be in.", n2)
	}
}

// Note: This resets the bloom filter.
func estimateP64(f *Filter64, n uint64) float64 {
	f.Reset()
	defer f.Reset()
	n1 := make([]byte, 8)
	for i := uint64(0); i < n; i++ {
		binary.BigEndian.PutUint64(n1, i)
		f.Add(n1)
	}
	fp := 0
	for i := uint64(0); i < 10000; i++ {
		binary.BigEndian.PutUint64(n1, i+uint64(n)+1)
		if f.Test(n1) {
			fp++
		}
	}
	return float64(fp) / float64(100)
}

func TestDirect64_20_5(t *testing.T) {
	n := uint64(10000)
	k := uint64(5)
	load := uint64(20)
	m := n * load
	f := &Filter64{
		newFilter64(m, k),
		bitset.New64(m),
	}
	p := estimateP64(f, n)
	if p > 0.0001 {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestDirect64_15_10(t *testing.T) {
	n := uint64(10000)
	k := uint64(10)
	load := uint64(15)
	m := n * load
	f := &Filter64{
		newFilter64(m, k),
		bitset.New64(m),
	}
	p := estimateP64(f, n)
	if p > 0.0001 {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestEstimated64_10_0001(t *testing.T) {
	n := int64(10000)
	fp := 0.0001
	f := New64(n, fp)
	p := estimateP64(f, uint64(n))
	if p > fp {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestEstimated64_10_001(t *testing.T) {
	n := int64(10000)
	fp := 0.001
	f := New64(n, fp)
	p := estimateP64(f, uint64(n))
	if p > fp {
		t.Errorf("False positive rate too high: %f", p)
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
