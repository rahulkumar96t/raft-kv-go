package lincheck

import (
	"fmt"
	"testing"
	"time"
)

func ms(n int) time.Duration { return time.Duration(n) * time.Millisecond }

func TestSequentialHistory(t *testing.T) {
	h := []Op{
		{Kind: Get, Key: "k", Found: false, Call: ms(0), Return: ms(1)},
		{Kind: Put, Key: "k", Value: "a", Call: ms(2), Return: ms(3)},
		{Kind: Get, Key: "k", Value: "a", Found: true, Call: ms(4), Return: ms(5)},
		{Kind: Delete, Key: "k", Found: true, Call: ms(6), Return: ms(7)},
		{Kind: Delete, Key: "k", Found: false, Call: ms(8), Return: ms(9)},
		{Kind: Get, Key: "k", Found: false, Call: ms(10), Return: ms(11)},
	}
	if err := Check(h); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentWritesEitherOrder(t *testing.T) {
	// Two overlapping puts; later reads may see either, but must agree.
	base := []Op{
		{Client: 1, Kind: Put, Key: "k", Value: "a", Call: ms(0), Return: ms(10)},
		{Client: 2, Kind: Put, Key: "k", Value: "b", Call: ms(1), Return: ms(9)},
	}
	for _, v := range []string{"a", "b"} {
		h := append(append([]Op{}, base...),
			Op{Client: 3, Kind: Get, Key: "k", Value: v, Found: true, Call: ms(11), Return: ms(12)},
			Op{Client: 3, Kind: Get, Key: "k", Value: v, Found: true, Call: ms(13), Return: ms(14)},
		)
		if err := Check(h); err != nil {
			t.Errorf("final value %q: %v", v, err)
		}
	}
	flipFlop := append(append([]Op{}, base...),
		Op{Client: 3, Kind: Get, Key: "k", Value: "a", Found: true, Call: ms(11), Return: ms(12)},
		Op{Client: 3, Kind: Get, Key: "k", Value: "b", Found: true, Call: ms(13), Return: ms(14)},
	)
	if Check(flipFlop) == nil {
		t.Error("reads after both writes completed changed value: must not be linearizable")
	}
}

func TestReadConcurrentWithWriteMaySeeEither(t *testing.T) {
	for _, found := range []bool{false, true} {
		h := []Op{
			{Client: 1, Kind: Put, Key: "k", Value: "a", Call: ms(0), Return: ms(10)},
			{Client: 2, Kind: Get, Key: "k", Value: map[bool]string{true: "a"}[found], Found: found, Call: ms(2), Return: ms(5)},
		}
		if err := Check(h); err != nil {
			t.Errorf("found=%v: %v", found, err)
		}
	}
}

func TestStaleReadIsRejected(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: Put, Key: "k", Value: "a", Call: ms(0), Return: ms(1)},
		{Client: 1, Kind: Put, Key: "k", Value: "b", Call: ms(2), Return: ms(3)},
		// Starts after "b" was acknowledged but returns the old value.
		{Client: 2, Kind: Get, Key: "k", Value: "a", Found: true, Call: ms(4), Return: ms(5)},
	}
	if Check(h) == nil {
		t.Fatal("stale read accepted")
	}
}

func TestLostWriteIsRejected(t *testing.T) {
	h := []Op{
		{Client: 1, Kind: Put, Key: "k", Value: "a", Call: ms(0), Return: ms(1)},
		{Client: 2, Kind: Get, Key: "k", Found: false, Call: ms(2), Return: ms(3)},
	}
	if Check(h) == nil {
		t.Fatal("read missing an acknowledged write accepted")
	}
}

func TestPhantomValueIsRejected(t *testing.T) {
	h := []Op{{Kind: Get, Key: "k", Value: "never written", Found: true, Call: ms(0), Return: ms(1)}}
	if Check(h) == nil {
		t.Fatal("read of a value nobody wrote accepted")
	}
}

func TestKeysAreIndependent(t *testing.T) {
	h := []Op{
		{Kind: Put, Key: "x", Value: "1", Call: ms(0), Return: ms(1)},
		{Kind: Get, Key: "y", Found: false, Call: ms(2), Return: ms(3)},
		{Kind: Get, Key: "x", Value: "1", Found: true, Call: ms(2), Return: ms(3)},
	}
	if err := Check(h); err != nil {
		t.Fatal(err)
	}
}

func TestManyConcurrentOpsStayTractable(t *testing.T) {
	// 5 clients x 12 ops on one key, heavily overlapping, all writes distinct
	// and every read returning the latest write in a valid order.
	var h []Op
	clock := 0
	last := ""
	for round := 0; round < 12; round++ {
		start := clock
		for c := 0; c < 5; c++ {
			v := fmt.Sprintf("c%d-r%d", c, round)
			if c == 4 {
				h = append(h, Op{Client: c, Kind: Get, Key: "k", Value: last, Found: last != "", Call: ms(start), Return: ms(start + 20)})
				continue
			}
			h = append(h, Op{Client: c, Kind: Put, Key: "k", Value: v, Call: ms(start + c), Return: ms(start + 20)})
		}
		last = fmt.Sprintf("c%d-r%d", 3, round)
		clock += 21
	}
	start := time.Now()
	if err := Check(h); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d > 5*time.Second {
		t.Fatalf("checking 60 ops took %v", d)
	}
}

func TestTooManyOpsPerKey(t *testing.T) {
	h := make([]Op, MaxOpsPerKey+1)
	for i := range h {
		h[i] = Op{Kind: Put, Key: "k", Value: "v", Call: ms(i), Return: ms(i)}
	}
	if Check(h) == nil {
		t.Fatal("expected a limit error")
	}
}
