package flow

import (
	"errors"
	"math"
	"sync"

	"github.com/reugn/go-streams"
)

// JoinSide identifies an input of an interval join.
type JoinSide uint8

const (
	JoinLeft JoinSide = iota
	JoinRight
)

// JoinKind distinguishes data from event-time progress signals.
type JoinKind uint8

const (
	JoinData JoinKind = iota
	JoinWatermark
	JoinEnd
	JoinBarrier
)

// JoinMode controls which unpaired occurrences are emitted on finalization.
type JoinMode uint8

const (
	JoinInner JoinMode = iota
	JoinLeftOuter
	JoinFullOuter
)

// JoinErrorKind identifies a recoverable event rejection.
type JoinErrorKind uint8

const (
	JoinInvalidInput JoinErrorKind = iota
	JoinClosedInput
	JoinWatermarkRegression
	JoinCapacityExceeded
)

// JoinError retains the rejected input so consumers can route or retry it
// without parsing diagnostic text. Input has its original dynamic type.
type JoinError struct {
	Kind  JoinErrorKind
	Input any
}

func (e *JoinError) Error() string {
	switch e.Kind {
	case JoinInvalidInput:
		return "flow: invalid interval join event"
	case JoinClosedInput:
		return "flow: input after join side ended"
	case JoinWatermarkRegression:
		return "flow: regressing join watermark"
	case JoinCapacityExceeded:
		return "flow: join capacity exceeded"
	default:
		return "flow: interval join input rejected"
	}
}

// JoinEvent is one ordered input event. Only the payload selected by Side
// is used for data; controls use Side, Kind, and (for watermarks) Timestamp.
type JoinEvent[K comparable, L, R any] struct {
	Side      JoinSide
	Kind      JoinKind
	Key       K
	Timestamp int64
	Left      L
	Right     R
}

// JoinResult is a pair, a finalized unmatched occurrence, or a recoverable
// input error. Presence is independent of the payload's zero value.
type JoinResult[K comparable, L, R any] struct {
	Key                 K
	Left                L
	Right               R
	LeftTime, RightTime int64
	HasLeft, HasRight   bool
	Err                 error
	Checkpoint          *JoinCheckpoint[K, L, R]
}

// JoinStats describes all events processed so far, including buffered records
// which have already matched. Output may still be waiting for a consumer.
type JoinStats struct {
	AcceptedLeft, AcceptedRight   uint64
	LateLeft, LateRight           uint64
	Rejected, Pairs               uint64
	UnmatchedLeft, UnmatchedRight uint64
	BufferedLeft, BufferedRight   uint64
}

type intervalRecord[K comparable, L, R any] struct {
	event   JoinEvent[K, L, R]
	matched bool
}

type intervalProgress struct {
	watermark    int64
	hasWatermark bool
	ended        bool
}

// JoinCheckpoint is an opaque, reusable in-process resume point. Record
// bookkeeping is detached; user payloads retain their ordinary Go value
// semantics. A zero-value checkpoint is invalid.
type JoinCheckpoint[K comparable, L, R any] struct {
	valid        bool
	lower, upper int64
	capacity     uint64
	mode         JoinMode
	stats        JoinStats
	progress     [2]intervalProgress
	left         map[K][]*intervalRecord[K, L, R]
	right        map[K][]*intervalRecord[K, L, R]
}

// RestoreIntervalJoin starts a new independent join at a barrier. Its input
// is open, with the same per-side progress, matching history, and counters.
// A nil capacity preserves the checkpoint's limit. A replacement limit must
// be nonnegative and accommodate all retained occurrences.
// The checkpoint may be restored repeatedly and concurrently.
func RestoreIntervalJoin[K comparable, L, R any](
	checkpoint *JoinCheckpoint[K, L, R], capacity *int,
) (*IntervalJoin[K, L, R], error) {
	if checkpoint == nil || !checkpoint.valid {
		return nil, errors.New("flow: invalid join checkpoint")
	}
	limit := checkpoint.capacity
	if capacity != nil {
		replacement := *capacity
		if replacement < 0 {
			return nil, errors.New("flow: negative restored join capacity")
		}
		limit = uint64(replacement)
		retained := checkpoint.stats.BufferedLeft + checkpoint.stats.BufferedRight
		if limit < retained {
			return nil, errors.New("flow: restored join capacity is below retained record count")
		}
	}
	j := &IntervalJoin[K, L, R]{
		lower:    checkpoint.lower,
		upper:    checkpoint.upper,
		capacity: limit,
		mode:     checkpoint.mode,
		stats:    checkpoint.stats,
		progress: checkpoint.progress,
		left:     cloneIntervalRecords(checkpoint.left),
		right:    cloneIntervalRecords(checkpoint.right),
		in:       make(chan any),
		out:      make(chan any),
	}
	go j.run()
	return j, nil
}

func cloneIntervalRecords[K comparable, L, R any](
	source map[K][]*intervalRecord[K, L, R],
) map[K][]*intervalRecord[K, L, R] {
	result := make(map[K][]*intervalRecord[K, L, R], len(source))
	for key, records := range source {
		cloned := make([]*intervalRecord[K, L, R], len(records))
		for i, record := range records {
			value := *record
			cloned[i] = &value
		}
		result[key] = cloned
	}
	return result
}

func (j *IntervalJoin[K, L, R]) checkpoint() *JoinCheckpoint[K, L, R] {
	return &JoinCheckpoint[K, L, R]{
		valid:    true,
		lower:    j.lower,
		upper:    j.upper,
		capacity: j.capacity,
		mode:     j.mode,
		stats:    j.stats,
		progress: j.progress,
		left:     cloneIntervalRecords(j.left),
		right:    cloneIntervalRecords(j.right),
	}
}

// IntervalJoin joins occurrences by key and an inclusive event-time interval.
// Inputs are processed in channel order. Watermarks and end markers, rather
// than timers, determine when an occurrence can no longer receive a match.
type IntervalJoin[K comparable, L, R any] struct {
	lower, upper int64
	capacity     uint64
	mode         JoinMode
	in           chan any
	out          chan any
	mu           sync.Mutex
	stats        JoinStats
	progress     [2]intervalProgress
	left         map[K][]*intervalRecord[K, L, R]
	right        map[K][]*intervalRecord[K, L, R]
}

var _ streams.Flow = (*IntervalJoin[string, any, any])(nil)

// NewIntervalJoin constructs a join. Capacity bounds the total number of
// retained occurrences; zero forbids retention. Lower may be negative.
func NewIntervalJoin[K comparable, L, R any](
	lower, upper int64, capacity int, mode JoinMode,
) (*IntervalJoin[K, L, R], error) {
	if lower > upper {
		return nil, errors.New("flow: interval lower bound exceeds upper bound")
	}
	if capacity < 0 {
		return nil, errors.New("flow: negative join capacity")
	}
	if mode > JoinFullOuter {
		return nil, errors.New("flow: invalid join mode")
	}
	j := &IntervalJoin[K, L, R]{
		lower:    lower,
		upper:    upper,
		capacity: uint64(capacity),
		mode:     mode,
		in:       make(chan any),
		out:      make(chan any),
		left:     make(map[K][]*intervalRecord[K, L, R]),
		right:    make(map[K][]*intervalRecord[K, L, R]),
	}
	go j.run()
	return j, nil
}

// In is the ordered stream of JoinEvent values.
func (j *IntervalJoin[K, L, R]) In() chan<- any { return j.in }

// Out yields JoinResult values and closes after input finalization.
func (j *IntervalJoin[K, L, R]) Out() <-chan any { return j.out }

// Via asynchronously connects this join to another flow.
func (j *IntervalJoin[K, L, R]) Via(next streams.Flow) streams.Flow {
	go j.transmit(next)
	return next
}

// To drains this join into a sink and waits for the sink to complete.
func (j *IntervalJoin[K, L, R]) To(sink streams.Sink) {
	j.transmit(sink)
	sink.AwaitCompletion()
}

func (j *IntervalJoin[K, L, R]) transmit(next streams.Inlet) {
	for result := range j.out {
		next.In() <- result
	}
	close(next.In())
}

// Stats returns a coherent snapshot. It does not wait for output consumption.
func (j *IntervalJoin[K, L, R]) Stats() JoinStats {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.stats
}

func (j *IntervalJoin[K, L, R]) run() {
	defer close(j.out)
	for value := range j.in {
		j.mu.Lock()
		results := j.apply(value)
		j.mu.Unlock()
		for _, result := range results {
			j.out <- result
		}
	}
	j.mu.Lock()
	j.progress[JoinLeft].ended = true
	j.progress[JoinRight].ended = true
	results := j.finalize()
	j.mu.Unlock()
	for _, result := range results {
		j.out <- result
	}
}

func (j *IntervalJoin[K, L, R]) reject(kind JoinErrorKind, input any) []JoinResult[K, L, R] {
	j.stats.Rejected++
	return []JoinResult[K, L, R]{{Err: &JoinError{Kind: kind, Input: input}}}
}

func (j *IntervalJoin[K, L, R]) apply(value any) []JoinResult[K, L, R] {
	event, ok := value.(JoinEvent[K, L, R])
	if !ok {
		return j.reject(JoinInvalidInput, value)
	}
	if event.Kind == JoinBarrier {
		return []JoinResult[K, L, R]{{Checkpoint: j.checkpoint()}}
	}
	if event.Side > JoinRight || event.Kind > JoinBarrier {
		return j.reject(JoinInvalidInput, value)
	}
	progress := &j.progress[event.Side]
	if event.Kind == JoinEnd {
		progress.ended = true
		return j.finalize()
	}
	if progress.ended {
		return j.reject(JoinClosedInput, value)
	}
	if event.Kind == JoinWatermark {
		if progress.hasWatermark && event.Timestamp < progress.watermark {
			return j.reject(JoinWatermarkRegression, value)
		}
		progress.watermark = event.Timestamp
		progress.hasWatermark = true
		return j.finalize()
	}
	if progress.hasWatermark && event.Timestamp < progress.watermark {
		if event.Side == JoinLeft {
			j.stats.LateLeft++
		} else {
			j.stats.LateRight++
		}
		return nil
	}
	return j.accept(event)
}

// compareDifference compares a-b with bound in mathematical integers.
// No addition or subtraction is evaluated until its int64 range is known.
func compareDifference(a, b, bound int64) int {
	if bound > 0 && b > math.MaxInt64-bound {
		return -1
	}
	if bound < 0 && b < math.MinInt64-bound {
		return 1
	}
	sum := b + bound
	if a < sum {
		return -1
	}
	if a > sum {
		return 1
	}
	return 0
}

func (j *IntervalJoin[K, L, R]) matches(left, right int64) bool {
	return compareDifference(right, left, j.lower) >= 0 && compareDifference(right, left, j.upper) <= 0
}

func (j *IntervalJoin[K, L, R]) finished(event JoinEvent[K, L, R]) bool {
	other := j.progress[1-event.Side]
	if other.ended {
		return true
	}
	if !other.hasWatermark {
		return false
	}
	if event.Side == JoinLeft {
		return compareDifference(other.watermark, event.Timestamp, j.upper) > 0
	}
	return compareDifference(event.Timestamp, other.watermark, j.lower) < 0
}

func (j *IntervalJoin[K, L, R]) accept(event JoinEvent[K, L, R]) []JoinResult[K, L, R] {
	retain := !j.finished(event)
	if retain && j.stats.BufferedLeft+j.stats.BufferedRight >= j.capacity {
		return j.reject(JoinCapacityExceeded, event)
	}
	record := &intervalRecord[K, L, R]{event: event}
	var results []JoinResult[K, L, R]
	if event.Side == JoinLeft {
		j.stats.AcceptedLeft++
		for _, other := range j.right[event.Key] {
			if j.matches(event.Timestamp, other.event.Timestamp) {
				record.matched = true
				other.matched = true
				results = append(results, j.pair(record, other))
			}
		}
		if retain {
			j.left[event.Key] = append(j.left[event.Key], record)
			j.stats.BufferedLeft++
		}
	} else {
		j.stats.AcceptedRight++
		for _, other := range j.left[event.Key] {
			if j.matches(other.event.Timestamp, event.Timestamp) {
				record.matched = true
				other.matched = true
				results = append(results, j.pair(other, record))
			}
		}
		if retain {
			j.right[event.Key] = append(j.right[event.Key], record)
			j.stats.BufferedRight++
		}
	}
	if !retain {
		if result, ok := j.unmatched(record); ok {
			results = append(results, result)
		}
	}
	return results
}

func (j *IntervalJoin[K, L, R]) pair(left, right *intervalRecord[K, L, R]) JoinResult[K, L, R] {
	j.stats.Pairs++
	return JoinResult[K, L, R]{
		Key:       left.event.Key,
		Left:      left.event.Left,
		Right:     right.event.Right,
		LeftTime:  left.event.Timestamp,
		RightTime: right.event.Timestamp,
		HasLeft:   true,
		HasRight:  true,
	}
}

func (j *IntervalJoin[K, L, R]) unmatched(record *intervalRecord[K, L, R]) (JoinResult[K, L, R], bool) {
	var result JoinResult[K, L, R]
	if record.matched || j.mode == JoinInner {
		return result, false
	}
	event := record.event
	result.Key = event.Key
	if event.Side == JoinLeft {
		result.Left = event.Left
		result.LeftTime = event.Timestamp
		result.HasLeft = true
		j.stats.UnmatchedLeft++
		return result, true
	}
	if j.mode == JoinFullOuter {
		result.Right = event.Right
		result.RightTime = event.Timestamp
		result.HasRight = true
		j.stats.UnmatchedRight++
		return result, true
	}
	return result, false
}

func (j *IntervalJoin[K, L, R]) finalize() []JoinResult[K, L, R] {
	results := j.finalizeSide(j.left, &j.stats.BufferedLeft)
	return append(results, j.finalizeSide(j.right, &j.stats.BufferedRight)...)
}

func (j *IntervalJoin[K, L, R]) finalizeSide(
	side map[K][]*intervalRecord[K, L, R], buffered *uint64,
) []JoinResult[K, L, R] {
	var results []JoinResult[K, L, R]
	for key, records := range side {
		kept := records[:0]
		for _, record := range records {
			if !j.finished(record.event) {
				kept = append(kept, record)
				continue
			}
			*buffered--
			if result, ok := j.unmatched(record); ok {
				results = append(results, result)
			}
		}
		for i := len(kept); i < len(records); i++ {
			records[i] = nil
		}
		if len(kept) == 0 {
			delete(side, key)
		} else {
			side[key] = kept
		}
	}
	return results
}
