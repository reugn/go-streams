//go:build challenge

package flow_test

import (
	"errors"
	"math"
	"reflect"
	"sort"
	"testing"

	"github.com/reugn/go-streams/flow"
)

type joinEvent = flow.JoinEvent[string, int, int]
type joinResult = flow.JoinResult[string, int, int]
type joinFlow = flow.IntervalJoin[string, int, int]

func left(key string, at int64, value int) joinEvent {
	return joinEvent{Side: flow.JoinLeft, Kind: flow.JoinData, Key: key, Timestamp: at, Left: value}
}
func right(key string, at int64, value int) joinEvent {
	return joinEvent{Side: flow.JoinRight, Kind: flow.JoinData, Key: key, Timestamp: at, Right: value}
}
func mark(side flow.JoinSide, at int64) joinEvent {
	return joinEvent{Side: side, Kind: flow.JoinWatermark, Timestamp: at}
}
func end(side flow.JoinSide) joinEvent { return joinEvent{Side: side, Kind: flow.JoinEnd} }

func joinNew(t *testing.T, lower, upper int64, capacity int, mode flow.JoinMode) *joinFlow {
	t.Helper()
	j, e := flow.NewIntervalJoin[string, int, int](lower, upper, capacity, mode)
	if e != nil {
		t.Fatal(e)
	}
	return j
}

func joinFinish(t *testing.T, j *joinFlow, events ...any) ([]joinResult, flow.JoinStats) {
	t.Helper()
	stopStats, statsDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(statsDone)
		for {
			select {
			case <-stopStats:
				return
			default:
				_ = j.Stats()
			}
		}
	}()
	defer func() { close(stopStats); <-statsDone }()
	go func() {
		for _, e := range events {
			j.In() <- e
		}
		close(j.In())
	}()
	var results []joinResult
	for v := range j.Out() {
		r, ok := v.(joinResult)
		if !ok {
			t.Fatalf("wrong output type %T", v)
		}
		results = append(results, r)
	}
	return results, j.Stats()
}

func joinRun(t *testing.T, lower, upper int64, capacity int, mode flow.JoinMode, events ...any) ([]joinResult, flow.JoinStats) {
	t.Helper()
	return joinFinish(t, joinNew(t, lower, upper, capacity, mode), events...)
}

func pairs(rs []joinResult) [][2]int {
	var p [][2]int
	for _, r := range rs {
		if r.HasLeft && r.HasRight {
			p = append(p, [2]int{r.Left, r.Right})
		}
	}
	return p
}

func unpaired(rs []joinResult) [][2]int {
	var p [][2]int
	for _, r := range rs {
		if r.HasLeft && !r.HasRight {
			p = append(p, [2]int{0, r.Left})
		}
		if r.HasRight && !r.HasLeft {
			p = append(p, [2]int{1, r.Right})
		}
	}
	sort.Slice(p, func(i, j int) bool {
		if p[i][0] != p[j][0] {
			return p[i][0] < p[j][0]
		}
		return p[i][1] < p[j][1]
	})
	return p
}

func wantPairs(t *testing.T, rs []joinResult, expected ...[2]int) {
	t.Helper()
	if !reflect.DeepEqual(pairs(rs), expected) {
		t.Fatalf("pairs %v, want %v", pairs(rs), expected)
	}
}
func noErrors(t *testing.T, rs []joinResult) {
	t.Helper()
	for _, r := range rs {
		if r.Err != nil {
			t.Fatal(r.Err)
		}
	}
}

func TestIntervalJoinBoundaries(t *testing.T) {
	rs, s := joinRun(t, -2, 3, 20, flow.JoinInner, left("k", 10, 1), right("k", 7, 7), right("k", 8, 8), right("k", 13, 13), right("k", 14, 14))
	noErrors(t, rs)
	wantPairs(t, rs, [2]int{1, 8}, [2]int{1, 13})
	for i, r := range rs {
		if r.Key != "k" || r.LeftTime != 10 || r.RightTime != []int64{8, 13}[i] {
			t.Fatal("pair lost input metadata", r)
		}
	}
	if s.Pairs != 2 {
		t.Fatal(s)
	}
	rs, _ = joinRun(t, 0, 0, 3, flow.JoinInner, left("k", 0, 1), right("k", 0, 2))
	wantPairs(t, rs, [2]int{1, 2})
}

func TestIntervalJoinOverflow(t *testing.T) {
	for _, f := range []struct {
		l, r, lo, hi int64
		match        bool
	}{
		{math.MinInt64, math.MaxInt64, 0, math.MaxInt64, false},
		{math.MaxInt64, math.MinInt64, math.MinInt64, 0, false},
		{math.MinInt64, math.MaxInt64, -1, -1, false},
		{math.MaxInt64, math.MinInt64, 1, 1, false},
		{0, math.MinInt64, math.MinInt64, math.MinInt64, true},
		{math.MinInt64, -1, math.MaxInt64, math.MaxInt64, true},
	} {
		rs, _ := joinRun(t, f.lo, f.hi, 3, flow.JoinInner, left("k", f.l, 1), right("k", f.r, 2))
		if (len(pairs(rs)) == 1) != f.match {
			t.Fatalf("overflow interval %+v: %v", f, pairs(rs))
		}
	}
}

func TestIntervalJoinOccurrences(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 8, flow.JoinInner, left("a", 5, 1), left("a", 5, 1), right("b", 5, 9), right("a", 5, 2), right("a", 5, 2))
	wantPairs(t, rs, [2]int{1, 2}, [2]int{1, 2}, [2]int{1, 2}, [2]int{1, 2})
	if s.AcceptedLeft != 2 || s.AcceptedRight != 3 || s.Pairs != 4 {
		t.Fatal(s)
	}
}

func TestIntervalJoinArrivalOrder(t *testing.T) {
	rs, _ := joinRun(t, -5, 5, 8, flow.JoinInner, right("k", 12, 3), right("k", 8, 1), right("k", 10, 2), left("k", 10, 4), left("k", 9, 5))
	wantPairs(t, rs, [2]int{4, 3}, [2]int{4, 1}, [2]int{4, 2}, [2]int{5, 3}, [2]int{5, 1}, [2]int{5, 2})
}

func TestIntervalJoinLateBoundary(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 9, flow.JoinFullOuter, mark(flow.JoinLeft, 4), left("k", 3, 1), left("k", 4, 2), mark(flow.JoinRight, 4), right("k", 3, 3), right("k", 4, 4))
	noErrors(t, rs)
	wantPairs(t, rs, [2]int{2, 4})
	if s.LateLeft != 1 || s.LateRight != 1 || s.AcceptedLeft != 1 || s.AcceptedRight != 1 || s.Rejected != 0 {
		t.Fatal(s)
	}
}

func TestIntervalJoinWatermarkRegression(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 9, flow.JoinInner, mark(flow.JoinLeft, 5), mark(flow.JoinLeft, 5), mark(flow.JoinLeft, 2), left("k", 3, 1), left("k", 5, 2), right("k", 5, 3))
	wantPairs(t, rs, [2]int{2, 3})
	if s.Rejected != 1 || s.LateLeft != 1 || s.AcceptedLeft != 1 {
		t.Fatal(s)
	}
}

func TestIntervalJoinLeftRetention(t *testing.T) {
	rs, s := joinRun(t, -2, 3, 1, flow.JoinFullOuter, left("k", 10, 1), mark(flow.JoinRight, 13), left("x", 20, 2), mark(flow.JoinRight, 14), left("x", 20, 3))
	if s.Rejected != 1 || s.AcceptedLeft != 2 || s.UnmatchedLeft != 2 {
		t.Fatal(s)
	}
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{0, 1}, {0, 3}}) {
		t.Fatal(unpaired(rs))
	}
}

func TestIntervalJoinRightRetention(t *testing.T) {
	rs, s := joinRun(t, -2, 3, 1, flow.JoinFullOuter, right("k", 10, 1), mark(flow.JoinLeft, 12), right("x", 20, 2), mark(flow.JoinLeft, 13), right("x", 20, 3))
	if s.Rejected != 1 || s.AcceptedRight != 2 || s.UnmatchedRight != 2 {
		t.Fatal(s)
	}
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{1, 1}, {1, 3}}) {
		t.Fatal(unpaired(rs))
	}
	rs, s = joinRun(t, math.MinInt64, 0, 1, flow.JoinInner, right("k", 0, 1), mark(flow.JoinLeft, math.MaxInt64), right("x", 0, 2))
	if s.Rejected != 1 {
		t.Fatal("overflow retired live record", s)
	}
	_, s = joinRun(t, 0, 1, 1, flow.JoinInner, left("k", math.MaxInt64, 1), mark(flow.JoinRight, math.MaxInt64), left("x", math.MaxInt64, 2))
	if s.Rejected != 1 {
		t.Fatal("overflow retired live left record", s)
	}
}

func TestIntervalJoinMatchedRetention(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 3, flow.JoinFullOuter, left("k", 5, 1), right("k", 5, 2), right("k", 5, 3), right("k", 5, 4))
	wantPairs(t, rs, [2]int{1, 2}, [2]int{1, 3})
	if s.Rejected != 1 || len(unpaired(rs)) != 0 {
		t.Fatal(s, unpaired(rs))
	}
}

func TestIntervalJoinOuterModes(t *testing.T) {
	for _, mode := range []flow.JoinMode{flow.JoinInner, flow.JoinLeftOuter, flow.JoinFullOuter} {
		rs, s := joinRun(t, 0, 0, 8, mode, left("a", 1, 0), right("b", 1, 0), left("c", 2, 3), right("c", 2, 4))
		wantPairs(t, rs, [2]int{3, 4})
		wanted := int(mode)
		if len(unpaired(rs)) != wanted {
			t.Fatal(mode, unpaired(rs))
		}
		if s.UnmatchedLeft+s.UnmatchedRight != uint64(wanted) {
			t.Fatal(s)
		}
		for _, r := range rs {
			if r.HasLeft && !r.HasRight && (r.Key != "a" || r.LeftTime != 1) {
				t.Fatal("left output lost metadata", r)
			}
			if r.HasRight && !r.HasLeft && (r.Key != "b" || r.RightTime != 1) {
				t.Fatal("right output lost metadata", r)
			}
		}
	}
	j, e := flow.NewIntervalJoin[string, *int, *int](0, 0, 2, flow.JoinFullOuter)
	if e != nil {
		t.Fatal(e)
	}
	go func() { j.In() <- flow.JoinEvent[string, *int, *int]{Side: flow.JoinLeft}; close(j.In()) }()
	v := <-j.Out()
	r := v.(flow.JoinResult[string, *int, *int])
	if !r.HasLeft || r.HasRight || r.Left != nil {
		t.Fatal(r)
	}
	for range j.Out() {
	}
}

func TestIntervalJoinSideEnd(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 2, flow.JoinFullOuter, right("orphan", 5, 1), left("k", 5, 2), end(flow.JoinLeft), right("k", 5, 3), right("k", 5, 4))
	noErrors(t, rs)
	wantPairs(t, rs, [2]int{2, 3}, [2]int{2, 4})
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{1, 1}}) || s.AcceptedRight != 3 {
		t.Fatal(s, unpaired(rs))
	}
	rs, _ = joinRun(t, 0, 0, 2, flow.JoinFullOuter, right("k", 5, 1), end(flow.JoinRight), left("k", 5, 2))
	wantPairs(t, rs, [2]int{2, 1})
}

func TestIntervalJoinClosedSide(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 1, flow.JoinFullOuter, end(flow.JoinLeft), end(flow.JoinLeft), left("k", 0, 1), mark(flow.JoinLeft, 0), right("k", 0, 2), end(flow.JoinRight), end(flow.JoinRight))
	if s.Rejected != 2 || s.AcceptedRight != 1 || s.AcceptedLeft != 0 || s.UnmatchedRight != 1 {
		t.Fatal(s)
	}
	if len(rs) != 3 {
		t.Fatal(rs)
	}
}

func TestIntervalJoinCapacityAtomic(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 1, flow.JoinFullOuter, left("k", 1, 1), right("k", 1, 2))
	if s.Rejected != 1 || s.Pairs != 0 || s.AcceptedRight != 0 || s.UnmatchedLeft != 1 {
		t.Fatal(s)
	}
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{0, 1}}) {
		t.Fatal("rejection marked partner matched", rs)
	}
}

func TestIntervalJoinZeroCapacity(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 0, flow.JoinFullOuter, left("k", 0, 1), end(flow.JoinLeft), right("k", 0, 2))
	if s.Rejected != 1 || s.AcceptedRight != 1 || s.UnmatchedRight != 1 {
		t.Fatal(s)
	}
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{1, 2}}) {
		t.Fatal(rs)
	}
}

func TestIntervalJoinCapacityRecovery(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 2, flow.JoinFullOuter, left("old", 1, 1), right("old", 1, 2), left("new", 10, 3), mark(flow.JoinRight, 2), mark(flow.JoinLeft, 2), left("new", 10, 4), right("new", 10, 5))
	wantPairs(t, rs, [2]int{1, 2}, [2]int{4, 5})
	if s.Rejected != 1 || s.AcceptedLeft != 2 || s.AcceptedRight != 2 || s.BufferedLeft != 0 || s.BufferedRight != 0 {
		t.Fatal(s)
	}
}

func TestIntervalJoinErrorAccounting(t *testing.T) {
	rs, s := joinRun(t, 0, 0, 2, flow.JoinInner, "wrong", joinEvent{Side: 9}, joinEvent{Kind: 9}, mark(flow.JoinLeft, 0), mark(flow.JoinLeft, -1), left("k", -1, 1), left("k", 0, 2), right("k", 0, 3), right("k", 0, 4), end(flow.JoinRight), right("k", 0, 5))
	wantPairs(t, rs, [2]int{2, 3})
	want := flow.JoinStats{AcceptedLeft: 1, AcceptedRight: 1, LateLeft: 1, Rejected: 6, Pairs: 1}
	if s != want {
		t.Fatalf("stats %+v, want %+v", s, want)
	}
	errors := 0
	for _, r := range rs {
		if r.Err != nil {
			errors++
		}
	}
	if errors != 6 {
		t.Fatal(errors)
	}
}

func TestIntervalJoinRejectionRouting(t *testing.T) {
	events := []any{"bad", joinEvent{Side: 9}, joinEvent{Kind: 9}, mark(flow.JoinLeft, 0), mark(flow.JoinLeft, -1), left("k", 0, 1), right("k", 0, 2), end(flow.JoinLeft), left("k", 0, 3)}
	rs, _ := joinRun(t, 0, 0, 1, flow.JoinInner, events...)
	wantKinds := []flow.JoinErrorKind{flow.JoinInvalidInput, flow.JoinInvalidInput, flow.JoinInvalidInput, flow.JoinWatermarkRegression, flow.JoinCapacityExceeded, flow.JoinClosedInput}
	wantInputs := []any{events[0], events[1], events[2], events[4], events[6], events[8]}
	index := 0
	for _, r := range rs {
		if r.Err == nil {
			continue
		}
		var e *flow.JoinError
		if !errors.As(r.Err, &e) || index >= len(wantKinds) || e.Kind != wantKinds[index] || !reflect.DeepEqual(e.Input, wantInputs[index]) {
			t.Fatalf("incorrect rejection routing: %v", r.Err)
		}
		index++
	}
	if index != len(wantKinds) {
		t.Fatal("missing rejection")
	}
}

type joinSink struct {
	in     chan any
	done   chan struct{}
	values []any
}

func (s *joinSink) In() chan<- any   { return s.in }
func (s *joinSink) AwaitCompletion() { <-s.done }

func TestIntervalJoinPipeline(t *testing.T) {
	j := joinNew(t, 0, 0, 2, flow.JoinInner)
	s := &joinSink{in: make(chan any), done: make(chan struct{})}
	go func() {
		for v := range s.in {
			s.values = append(s.values, v)
		}
		close(s.done)
	}()
	go func() { j.In() <- left("k", 1, 2); j.In() <- right("k", 1, 3); close(j.In()) }()
	j.Via(flow.NewMap[joinResult, int](func(r joinResult) int { return r.Left + r.Right }, 1)).To(s)
	if !reflect.DeepEqual(s.values, []any{5}) {
		t.Fatal(s.values)
	}
	j = joinNew(t, 0, 0, 0, flow.JoinInner)
	s = &joinSink{in: make(chan any), done: make(chan struct{})}
	go func() {
		for range s.in {
		}
		close(s.done)
	}()
	close(j.In())
	j.To(s)
}

func joinCheckpoint(t *testing.T, events ...any) (*flow.JoinCheckpoint[string, int, int], []joinResult) {
	t.Helper()
	j := joinNew(t, -2, 3, 5, flow.JoinFullOuter)
	go func() {
		for _, e := range events {
			j.In() <- e
		}
		j.In() <- joinEvent{Kind: flow.JoinBarrier, Side: 99}
	}()
	var prefix []joinResult
	for value := range j.Out() {
		r := value.(joinResult)
		if r.Checkpoint != nil {
			if r.Err != nil || r.HasLeft || r.HasRight {
				t.Fatal("invalid barrier result")
			}
			checkpoint := r.Checkpoint
			joinFinish(t, j)
			return checkpoint, prefix
		}
		prefix = append(prefix, r)
	}
	t.Fatal("no checkpoint")
	return nil, nil
}

func restoreJoin(t *testing.T, c *flow.JoinCheckpoint[string, int, int]) *joinFlow {
	t.Helper()
	j, e := flow.RestoreIntervalJoin(c, nil)
	if e != nil {
		t.Fatal(e)
	}
	return j
}

func TestIntervalJoinCheckpointReplay(t *testing.T) {
	c, prefix := joinCheckpoint(t, left("k", 10, 1), right("k", 11, 2))
	wantPairs(t, prefix, [2]int{1, 2})
	rs, s := joinFinish(t, restoreJoin(t, c), right("k", 12, 3))
	wantPairs(t, rs, [2]int{1, 3})
	if len(unpaired(rs)) != 0 || s.Pairs != 2 || s.AcceptedLeft != 1 || s.AcceptedRight != 2 {
		t.Fatal(s, rs)
	}
}

func TestIntervalJoinCheckpointProgress(t *testing.T) {
	c, _ := joinCheckpoint(t, left("k", 10, 1), mark(flow.JoinRight, 12), end(flow.JoinLeft), "bad")
	rs, s := joinFinish(t, restoreJoin(t, c), left("k", 10, 2), right("k", 11, 3), right("k", 12, 4))
	wantPairs(t, rs, [2]int{1, 4})
	if s.Rejected != 2 || s.LateRight != 1 || s.AcceptedRight != 1 || s.Pairs != 1 {
		t.Fatal(s)
	}
	c, _ = joinCheckpoint(t, left("a", 10, 1), left("b", 10, 2), left("c", 10, 3), left("d", 10, 4), left("e", 10, 5))
	_, s = joinFinish(t, restoreJoin(t, c), right("a", 10, 6))
	if s.Rejected != 1 || s.Pairs != 0 {
		t.Fatal("capacity lost on restore", s)
	}
}

func TestIntervalJoinCheckpointForks(t *testing.T) {
	c, _ := joinCheckpoint(t, left("k", 10, 1))
	first := restoreJoin(t, c)
	second := restoreJoin(t, c)
	rs, _ := joinFinish(t, first, right("k", 10, 2))
	wantPairs(t, rs, [2]int{1, 2})
	rs, s := joinFinish(t, second)
	if !reflect.DeepEqual(unpaired(rs), [][2]int{{0, 1}}) || s.Pairs != 0 {
		t.Fatal("fork state leaked", s, rs)
	}
	rs, s = joinFinish(t, restoreJoin(t, c))
	if len(unpaired(rs)) != 1 || s.Pairs != 0 {
		t.Fatal("checkpoint consumed", s, rs)
	}
}

func TestIntervalJoinInvalidConfiguration(t *testing.T) {
	for _, c := range []struct {
		lo, hi int64
		cap    int
		mode   flow.JoinMode
	}{{2, 1, 1, flow.JoinInner}, {0, 0, -1, flow.JoinInner}, {0, 0, 1, 99}} {
		if j, e := flow.NewIntervalJoin[string, int, int](c.lo, c.hi, c.cap, c.mode); e == nil || j != nil {
			t.Fatal("invalid configuration accepted")
		}
	}
	for _, c := range []*flow.JoinCheckpoint[string, int, int]{nil, {}} {
		if _, e := flow.RestoreIntervalJoin(c, nil); e == nil {
			t.Fatal("invalid checkpoint accepted")
		}
	}
	rs, s := joinRun(t, 0, 0, 0, flow.JoinInner)
	if len(rs) != 0 || s != (flow.JoinStats{}) {
		t.Fatal(rs, s)
	}
}

func TestIntervalJoinRestoreCapacity(t *testing.T) {
	c, _ := joinCheckpoint(t, left("a", 1, 1), right("b", 1, 2))
	for _, limit := range []int{-1, 0, 1} {
		if _, e := flow.RestoreIntervalJoin(c, &limit); e == nil {
			t.Fatal("undersized restore accepted")
		}
	}
	limit := 2
	j, e := flow.RestoreIntervalJoin(c, &limit)
	if e != nil {
		t.Fatal(e)
	}
	limit = 100
	_, s := joinFinish(t, j, left("x", 1, 3))
	if s.Rejected != 1 {
		t.Fatal("restored limit was not copied", s)
	}
	limit = 3
	j, e = flow.RestoreIntervalJoin(c, &limit)
	if e != nil {
		t.Fatal(e)
	}
	_, s = joinFinish(t, j, left("x", 1, 3), left("y", 1, 4))
	if s.Rejected != 1 || s.AcceptedLeft != 2 {
		t.Fatal(s)
	}
	_, s = joinFinish(t, restoreJoin(t, c), left("x", 1, 3), left("y", 1, 4))
	if s.Rejected != 0 {
		t.Fatal("restore changed checkpoint capacity", s)
	}
	empty, _ := joinCheckpoint(t)
	limit = 0
	j, e = flow.RestoreIntervalJoin(empty, &limit)
	if e != nil {
		t.Fatal(e)
	}
	_, s = joinFinish(t, j, left("x", 1, 1))
	if s.Rejected != 1 {
		t.Fatal(s)
	}
}
