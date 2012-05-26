package bloom

import (
	"github.com/pmylund/go-bitset"

	"encoding/binary"
	"hash"
	"hash/fnv"
	"math"
)

type filter struct {
	n uint32
	k uint32
	h hash.Hash64
}

func (f *filter) indices(data []byte) []uint32 {
	f.h.Reset()
	f.h.Write(data)
	d := f.h.Sum(nil)
	a := binary.BigEndian.Uint32(d[0:4])
	b := binary.BigEndian.Uint32(d[4:8])
	is := make([]uint32, f.k)
	for i := uint32(0); i < f.k; i++ {
		is[i] = (a + b*i) % f.n
	}
	return is
}

func new(n, k uint32) *filter {
	return &filter{
		n: n,
		k: k,
		h: fnv.New64a(),
	}
}

func estimates(num int, fpRate float64) (uint32, uint32) {
	n := uint32(-1 * float64(num) * math.Log(fpRate) / math.Pow(math.Log(2), 2))
	k := uint32(math.Ceil(math.Log(2) * float64(n) / float64(num)))
	return n, k
}

// A standard bloom filter using the 64-bit FNV-1a hash function.
type Filter struct {
	*filter
	b *bitset.Bitset
}

// Check whether data was previously added to the filter. Returns true if
// yes, with a false positive chance near the ratio specified upon creation
// of the filter. The result cannot be falsely negative.
func (f *Filter) Test(data []byte) bool {
	for _, i := range f.indices(data) {
		if !f.b.Test(i) {
			return false
		}
	}
	return true
}

// Add data to the filter.
func (f *Filter) Add(data []byte) {
	for _, i := range f.indices(data) {
		f.b.Set(i)
	}
}

// Create a bloom filter with an expected num number of items, and an acceptable
// false positive rate of fpRate, e.g. 0.01.
func New(num int, fpRate float64) *Filter {
	n, k := estimates(num, fpRate)
	f := &Filter{
		new(n, k),
		bitset.New(n),
	}
	return f
}

// A counting bloom filter using the 64-bit FNV-1a hash function. Supports
// removing items from the filter.
type CountingFilter struct {
	*filter
	b []*bitset.Bitset
}

// Checks whether data was previously added to the filter. Returns true if
// yes, with a false positive chance near the ratio specified upon creation
// of the filter. The result cannot cannot be falsely negative (unless one
// has removed an item that wasn't actually added to the filter previously.)
func (f *CountingFilter) Test(data []byte) bool {
	b := f.b[0]
	for _, v := range f.indices(data) {
		if !b.Test(v) {
			return false
		}
	}
	return true
}

// Adds data to the filter.
func (f *CountingFilter) Add(data []byte) {
	for _, v := range f.indices(data) {
		done := false
		for _, ov := range f.b {
			if !ov.Test(v) {
				done = true
				ov.Set(v)
				break
			}
		}
		if !done {
			nb := bitset.New(f.b[0].Len())
			f.b = append(f.b, nb)
			nb.Set(v)
		}
	}
}

// Removes data from the filter. This exact data must have been previously added
// to the filter, or future results will be inconsistent.
func (f *CountingFilter) Remove(data []byte) {
	last := len(f.b) - 1
	for _, v := range f.indices(data) {
		for oi := last; oi >= 0; oi-- {
			ov := f.b[oi]
			if ov.Test(v) {
				ov.Clear(v)
				break
			}
		}
	}
}

// Create a counting bloom filter with an expected num number of items, and an
// acceptable false positive rate of fpRate. Counting bloom filters support
// the removal of items from the filter.
func NewCounting(num int, fpRate float64) *CountingFilter {
	n, k := estimates(num, fpRate)
	f := &CountingFilter{
		new(n, k),
		[]*bitset.Bitset{bitset.New(n)},
	}
	return f
}

// A layered bloom filter using the 64-bit FNV-1a hash function.
type LayeredFilter struct {
	*filter
	b []*bitset.Bitset
}

// Checks whether data was previously added to the filter. Returns the number of
// the last layer where the data was added, e.g. 1 for the first layer, and a
// boolean indicating whether the data was added to the filter at all. The check
// has a false positive chance near the ratio specified upon creation of the
// filter. The result cannot be falsely negative.
func (f *LayeredFilter) Test(data []byte) (int, bool) {
	is := f.indices(data)
	for i := len(f.b) - 1; i >= 0; i-- {
		v := f.b[i]
		last := len(is) - 1
		for oi, ov := range is {
			if !v.Test(ov) {
				break
			}
			if oi == last {
				// Every test was positive at this layer
				return i + 1, true
			}
		}
	}
	return 0, false
}

// Adds data to the filter. Returns the number of the layer where the data
// was added, e.g. 1 for the first layer.
func (f *LayeredFilter) Add(data []byte) int {
	is := f.indices(data)
	var (
		i int
		v *bitset.Bitset
	)
	for i, v = range f.b {
		here := false
		for _, ov := range is {
			if here {
				v.Set(ov)
			} else if !v.Test(ov) {
				here = true
				v.Set(ov)
			}
		}
		if here {
			return i + 1
		}
	}
	nb := bitset.New(f.b[0].Len())
	f.b = append(f.b, nb)
	for _, v := range is {
		nb.Set(v)
	}
	return i + 2
}

// Create a layered bloom filter with an expected num number of items, and an
// acceptable false positive rate of fpRate. Layered bloom filters can be used
// to keep track of a certain, arbitrary count of items, e.g. to check if some
// given data was added to the filter 10 times or less.
func NewLayered(num int, fpRate float64) *LayeredFilter {
	n, k := estimates(num, fpRate)
	f := &LayeredFilter{
		new(n, k),
		[]*bitset.Bitset{bitset.New(n)},
	}
	return f
}
