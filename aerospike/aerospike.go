package aerospike

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	aero "github.com/aerospike/aerospike-client-go/v7"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// PollingConfig contains the configuration for polling Aerospike cluster events.
type PollingConfig struct {
	// PollingInterval specifies the interval at which the database should be
	// polled for changes. If not specified, the entire dataset will be queried
	// once.
	PollingInterval time.Duration
	// QueryPolicy encapsulates parameters for policy attributes used in query
	// operations (optional).
	QueryPolicy *aero.QueryPolicy
	// SecondaryIndexFilter specifies a query filter definition (optional).
	SecondaryIndexFilter *aero.Filter
	// Namespace determines query namespace.
	Namespace string
	// SetName determines query set name (optional).
	SetName string
	// BinNames determines which bins to retrieve (optional).
	BinNames []string

	filterExpression *aero.Expression
}

// PollingSource is an Aerospike source connector that regularly checks the
// database and transmits any recently updated records downstream.
type PollingSource struct {
	client      *aero.Client
	config      PollingConfig
	statement   *aero.Statement
	recordsChan chan *aero.Result
	out         chan any

	logger *slog.Logger
}

var _ streams.Source = (*PollingSource)(nil)

// NewPollingSource returns a new [PollingSource] connector.
func NewPollingSource(ctx context.Context, client *aero.Client,
	config PollingConfig, logger *slog.Logger) *PollingSource {
	if config.QueryPolicy == nil {
		config.QueryPolicy = aero.NewQueryPolicy()
	} else {
		config.filterExpression = config.QueryPolicy.FilterExpression
	}
	statement := &aero.Statement{
		Namespace: config.Namespace,
		SetName:   config.SetName,
		Filter:    config.SecondaryIndexFilter,
		BinNames:  config.BinNames,
	}

	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "aerospike"),
		slog.String("type", "source")))

	source := &PollingSource{
		client:      client,
		config:      config,
		statement:   statement,
		recordsChan: make(chan *aero.Result),
		out:         make(chan any),
		logger:      logger,
	}

	// periodically monitor the database for changes
	go source.pollChanges(ctx)

	// send new or updated records downstream
	go source.streamRecords(ctx)

	return source
}

func (ps *PollingSource) pollChanges(ctx context.Context) {
	if ps.config.PollingInterval == 0 {
		// retrieve the entire namespace/set once
		ps.query()
		close(ps.recordsChan)
		return
	}

	// obtain updates about data changes through scheduled queries
	ticker := time.NewTicker(ps.config.PollingInterval)
	defer ticker.Stop()
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case t := <-ticker.C:
			lastUpdate := t.Add(-ps.config.PollingInterval)
			// filter records by the time they were last updated
			lastUpdatedExp := aero.ExpGreater(
				aero.ExpLastUpdate(),
				aero.ExpIntVal(lastUpdate.UnixNano()),
			)
			if ps.config.filterExpression == nil {
				ps.config.QueryPolicy.FilterExpression = lastUpdatedExp
			} else {
				ps.config.QueryPolicy.FilterExpression = aero.ExpAnd(
					lastUpdatedExp,
					ps.config.filterExpression,
				)
			}
			ps.logger.Debug("Polling records", slog.Any("from", lastUpdate))
			// execute the query command
			ps.query()
		}
	}
}

func (ps *PollingSource) query() {
	recordSet, err := ps.client.Query(ps.config.QueryPolicy, ps.statement)
	if err != nil {
		ps.logger.Error("Polling query failed", slog.Any("error", err))
		return
	}
	for result := range recordSet.Results() {
		ps.recordsChan <- result
	}
}

func (ps *PollingSource) streamRecords(ctx context.Context) {
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case result, ok := <-ps.recordsChan:
			if !ok {
				break loop
			}
			if result.Err == nil {
				ps.out <- result.Record // send the record downstream
			} else {
				ps.logger.Error("Read record error",
					slog.Any("error", result.Err))
			}
		}
	}
	ps.logger.Info("Closing connector")
	close(ps.out)
}

// Via asynchronously streams data to the given Flow and returns it.
func (ps *PollingSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(ps, operator)
	return operator
}

// Out returns the output channel of the PollingSource connector.
func (ps *PollingSource) Out() <-chan any {
	return ps.out
}

// A Record encapsulates an Aerospike Key and BinMap container.
// It is intended to be used to stream records to the Aerospike sink connector.
type Record struct {
	Key  *aero.Key
	Bins aero.BinMap
}

// batchWrite creates and returns a batch write operation for the record.
func (r *Record) batchWrite(policy *aero.BatchWritePolicy) *aero.BatchWrite {
	ops := make([]*aero.Operation, 0, len(r.Bins))
	for k, v := range r.Bins {
		ops = append(ops, aero.PutOp(aero.NewBin(k, v)))
	}
	return aero.NewBatchWrite(policy, r.Key, ops...)
}

// SinkConfig contains the configuration for the Aerospike sink connector.
type SinkConfig struct {
	// WritePolicy encapsulates parameters for policy attributes used in
	// write operations. Used in single write operations and ignored if
	// BatchSize is larger than one (optional).
	WritePolicy *aero.WritePolicy
	// BatchSize controls the size of the batch when writing records. If not
	// specified or set to a value less than two, a single write operation
	// will be used for each record (optional).
	BatchSize int
	// BufferFlushInterval defines the maximum duration records can be buffered
	// before being flushed. Used with BatchSize larger than one (optional).
	BufferFlushInterval time.Duration
	// BatchPolicy encapsulates parameters for policy attributes used in
	// write operations. Used with BatchSize larger than one (optional).
	BatchPolicy *aero.BatchPolicy
	// BatchWritePolicy attributes used in batch write commands. Used with
	// BatchSize larger than one (optional).
	BatchWritePolicy *aero.BatchWritePolicy
	// Namespace determines the target namespace.
	Namespace string
	// SetName determines the target set name (optional).
	SetName string
}

// Sink represents an Aerospike sink connector.
type Sink struct {
	client *aero.Client
	config SinkConfig
	buf    []*Record
	in     chan any

	done   chan struct{}
	logger *slog.Logger
}

var _ streams.Sink = (*Sink)(nil)

// NewSink returns a new [Sink] connector.
func NewSink(client *aero.Client, config SinkConfig, logger *slog.Logger) *Sink {
	if logger == nil {
		logger = slog.Default()
	}
	logger = logger.With(slog.Group("connector",
		slog.String("name", "aerospike"),
		slog.String("type", "sink")))

	sink := &Sink{
		client: client,
		config: config,
		in:     make(chan any),
		done:   make(chan struct{}),
		logger: logger,
	}

	// initialize the buffer for batch writes
	if config.BatchSize > 1 {
		sink.buf = make([]*Record, 0, config.BatchSize)
	}

	// begin processing upstream records
	go sink.processStream()

	return sink
}

func (as *Sink) processStream() {
	defer close(as.done) // signal data processing completion

	var flushTickerChan <-chan time.Time
	if as.config.BatchSize > 1 && as.config.BufferFlushInterval > 0 {
		ticker := time.NewTicker(as.config.BufferFlushInterval)
		defer ticker.Stop()
		flushTickerChan = ticker.C
	}
loop:
	for {
		select {
		case msg, ok := <-as.in: // read upstream messages
			if !ok {
				break loop
			}
			switch message := msg.(type) {
			case *Record:
				as.writeRecord(message)
			case Record:
				as.writeRecord(&message)
			case aero.BinMap:
				encoded, err := json.Marshal(message)
				if err == nil {
					var key *aero.Key
					// use the sha256 checksum of the bin map as the record key
					key, err = aero.NewKey(as.config.Namespace, as.config.SetName,
						sha256.Sum256(encoded))
					if err == nil {
						as.writeRecord(&Record{key, message})
					}
				}
				if err != nil {
					as.logger.Error("Error parsing bin map",
						slog.Any("error", err))
				}
			default:
				as.logger.Error("Unsupported message type",
					slog.String("type", fmt.Sprintf("%T", message)))
			}
		case <-flushTickerChan:
			as.flushBuffer()
		}
	}
	as.flushBuffer() // write buffered records in batch mode
}

func (as *Sink) writeRecord(record *Record) {
	if as.config.BatchSize > 1 { // batch mode
		if len(as.buf) == as.config.BatchSize {
			as.flushBuffer()
		}
		// add the record to the buffer
		as.buf = append(as.buf, record)
	} else {
		// use single record put operation
		if err := as.client.Put(as.config.WritePolicy, record.Key, record.Bins); err != nil {
			as.logger.Error("Failed to write record", slog.Any("error", err))
		}
	}
}

func (as *Sink) flushBuffer() {
	if as.config.BatchSize > 1 && len(as.buf) > 0 {
		// write records as a batch
		records := make([]aero.BatchRecordIfc, 0, as.config.BatchSize)
		for _, rec := range as.buf {
			records = append(records, rec.batchWrite(as.config.BatchWritePolicy))
		}
		as.logger.Debug("Writing batch of records",
			slog.Int("size", len(records)))
		if err := as.client.BatchOperate(as.config.BatchPolicy, records); err != nil {
			as.logger.Error("Failed to write batch of records",
				slog.Any("error", err))
		}
		as.buf = as.buf[:0] // clear the buffer
	}
}

// In returns the input channel of the Sink connector.
func (as *Sink) In() chan<- any {
	return as.in
}

// AwaitCompletion blocks until the Sink connector has processed all the received data.
func (as *Sink) AwaitCompletion() {
	<-as.done
}
