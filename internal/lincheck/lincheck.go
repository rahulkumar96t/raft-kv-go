// Package lincheck checks recorded histories of a key-value store for
// linearizability.
//
// Linearizability is compositional, so each key is checked on its own as a
// register that supports put, get and delete. The search is the classic
// Wing & Gong backtracking: repeatedly pick an operation that may take effect
// next (one that was invoked before every other pending operation returned),
// apply it to the model, and backtrack when a read disagrees. Visited
// (set-of-applied-ops, model state) pairs are memoised, which keeps histories
// of a few dozen operations per key cheap to check.
package lincheck

import (
	"fmt"
	"math"
	"sort"
	"time"
)

// Kind is the operation type.
type Kind uint8

const (
	Put Kind = iota + 1
	Get
	Delete
)

func (k Kind) String() string {
	switch k {
	case Put:
		return "put"
	case Get:
		return "get"
	case Delete:
		return "delete"
	}
	return "?"
}

// Op is one completed operation in a history.
type Op struct {
	Client int
	Kind   Kind
	Key    string
	// Value is the value written by Put or observed by Get.
	Value string
	// Found is the observed existence of the key for Get, and whether the
	// key existed for Delete. Ignored for Put.
	Found bool
	// Call and Return are the invocation and response times.
	Call, Return time.Duration
}

func (o Op) String() string {
	return fmt.Sprintf("client %d %s(%s)=%q/%v [%v, %v]", o.Client, o.Kind, o.Key, o.Value, o.Found, o.Call, o.Return)
}

// MaxOpsPerKey is the largest per-key history the checker accepts.
const MaxOpsPerKey = 64

// Check returns nil if the history is linearizable, or an error naming the
// first key whose sub-history is not.
func Check(history []Op) error {
	byKey := map[string][]Op{}
	for _, op := range history {
		byKey[op.Key] = append(byKey[op.Key], op)
	}
	keys := make([]string, 0, len(byKey))
	for k := range byKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		ops := byKey[k]
		if len(ops) > MaxOpsPerKey {
			return fmt.Errorf("lincheck: key %q has %d operations, limit is %d", k, len(ops), MaxOpsPerKey)
		}
		if !checkRegister(ops) {
			sort.Slice(ops, func(i, j int) bool { return ops[i].Call < ops[j].Call })
			return fmt.Errorf("lincheck: history of key %q is not linearizable: %v", k, ops)
		}
	}
	return nil
}

type state struct {
	present bool
	value   string
}

func checkRegister(ops []Op) bool {
	n := len(ops)
	var all uint64
	if n == 64 {
		all = math.MaxUint64
	} else {
		all = 1<<uint(n) - 1
	}
	type memoKey struct {
		done uint64
		st   state
	}
	failed := map[memoKey]bool{}

	var search func(done uint64, st state) bool
	search = func(done uint64, st state) bool {
		if done == all {
			return true
		}
		if failed[memoKey{done, st}] {
			return false
		}
		// An op may go next only if no other pending op finished before it
		// started (real-time order).
		earliestReturn := time.Duration(math.MaxInt64)
		for i := 0; i < n; i++ {
			if done&(1<<uint(i)) == 0 && ops[i].Return < earliestReturn {
				earliestReturn = ops[i].Return
			}
		}
		for i := 0; i < n; i++ {
			op := ops[i]
			if done&(1<<uint(i)) != 0 || op.Call > earliestReturn {
				continue
			}
			next := st
			switch op.Kind {
			case Put:
				next = state{present: true, value: op.Value}
			case Delete:
				if op.Found != st.present {
					continue
				}
				next = state{}
			case Get:
				if op.Found != st.present || (st.present && op.Value != st.value) {
					continue
				}
			}
			if search(done|1<<uint(i), next) {
				return true
			}
		}
		failed[memoKey{done, st}] = true
		return false
	}
	return search(0, state{})
}
