package bloom

import (
	"github.com/pmylund/go-bitset"

	"encoding/binary"
	"strconv"
	"testing"
)

// TODO: Add reset tests

var (
	foo = []byte("foo")
	bar = []byte("bar")
	baz = []byte("baz")
)

func TestFilter(t *testing.T) {
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

func TestBasicUint32(t *testing.T) {
	f := New(1000, 0.0001)
	n1 := make([]byte, 4)
	n2 := make([]byte, 4)
	n3 := make([]byte, 4)
	binary.BigEndian.PutUint32(n1, 100)
	binary.BigEndian.PutUint32(n2, 101)
	binary.BigEndian.PutUint32(n3, 102)
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
func estimateP(f *Filter, n uint32) float64 {
	f.Reset()
	defer f.Reset()
	n1 := make([]byte, 4)
	for i := uint32(0); i < n; i++ {
		binary.BigEndian.PutUint32(n1, i)
		f.Add(n1)
	}
	fp := 0
	for i := uint32(0); i < 10000; i++ {
		binary.BigEndian.PutUint32(n1, i+uint32(n)+1)
		if f.Test(n1) {
			fp++
		}
	}
	return float64(fp) / float64(100)
}

func TestDirect20_5(t *testing.T) {
	n := uint32(10000)
	k := uint32(5)
	load := uint32(20)
	m := n * load
	f := &Filter{
		newFilter(m, k),
		bitset.New32(m),
	}
	p := estimateP(f, n)
	if p > 0.0001 {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestDirect15_10(t *testing.T) {
	n := uint32(10000)
	k := uint32(10)
	load := uint32(15)
	m := n * load
	f := &Filter{
		newFilter(m, k),
		bitset.New32(m),
	}
	p := estimateP(f, n)
	if p > 0.0001 {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestEstimated10_0001(t *testing.T) {
	n := 10000
	fp := 0.0001
	f := New(n, fp)
	p := estimateP(f, uint32(n))
	if p > fp {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestEstimated10_001(t *testing.T) {
	n := 10000
	fp := 0.001
	f := New(n, fp)
	p := estimateP(f, uint32(n))
	if p > fp {
		t.Errorf("False positive rate too high: %f", p)
	}
}

func TestCountingFilter(t *testing.T) {
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

func TestLayeredFilter(t *testing.T) {
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

const (
	million = 1000000
	billion = 1000 * million
)

func TestSizePanic(t *testing.T) {
	// Trying to create a bloom filter that requires a bitset slice with
	// > MaxInt32 should cause a panic rather than silently overflow.
	defer func() {
		if x := recover(); x == nil {
			t.Errorf("MaxInt32 word requirement should have caused a panic")
		}
	}()
	New(2*billion, 0.01)
}

func BenchmarkFilterAdd(b *testing.B) {
	b.StopTimer()
	f := New(b.N, 0.01)
	datas := make([][]byte, b.N)
	for i := range datas {
		datas[i] = []byte(strconv.Itoa(i))
	}
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.Add(datas[i])
	}
}

func BenchmarkFilterAddExisting(b *testing.B) {
	b.StopTimer()
	f := New(b.N, 0.01)
	f.Add(foo)
	b.StartTimer()
	for i := 0; i < b.N; i++ {
		f.Add(foo)
	}
}
