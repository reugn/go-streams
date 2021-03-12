package aerospike

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	aero "github.com/aerospike/aerospike-client-go"
	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// AerospikeProperties is the Aerospike connector configuration properties
type AerospikeProperties struct {
	Policy    *aero.ClientPolicy
	Hostname  string
	Port      int
	Namespase string
	SetName   string
}

// ChangeNotificationProperties holds the changes polling configuration
type ChangeNotificationProperties struct {
	PollingInterval time.Duration
}

// AerospikeSource connector
type AerospikeSource struct {
	client                       *aero.Client
	records                      chan *aero.Result
	scanPolicy                   *aero.ScanPolicy
	out                          chan interface{}
	ctx                          context.Context
	properties                   *AerospikeProperties
	changeNotificationProperties *ChangeNotificationProperties
}

// NewAerospikeSource returns a new AerospikeSource instance
// set changeNotificationProperties to nil to scan the entire namespace/set
func NewAerospikeSource(ctx context.Context,
	properties *AerospikeProperties,
	scanPolicy *aero.ScanPolicy,
	changeNotificationProperties *ChangeNotificationProperties) (*AerospikeSource, error) {

	client, err := aero.NewClientWithPolicy(properties.Policy, properties.Hostname, properties.Port)
	if err != nil {
		return nil, err
	}

	if scanPolicy == nil {
		scanPolicy = aero.NewScanPolicy()
	}

	records := make(chan *aero.Result)
	source := &AerospikeSource{
		client:                       client,
		records:                      records,
		scanPolicy:                   scanPolicy,
		out:                          make(chan interface{}),
		ctx:                          ctx,
		properties:                   properties,
		changeNotificationProperties: changeNotificationProperties,
	}

	go source.poll()
	go source.init()
	return source, nil
}

func (as *AerospikeSource) poll() {
	if as.changeNotificationProperties == nil {
		// scan the entire namespace/set
		as.doScan()
		close(as.records)
		return
	}

	// get change notifications by polling
	ticker := time.NewTicker(as.changeNotificationProperties.PollingInterval)
loop:
	for {
		select {
		case <-as.ctx.Done():
			break loop
		case t := <-ticker.C:
			ts := t.UnixNano() - as.changeNotificationProperties.PollingInterval.Nanoseconds()
			as.scanPolicy.PredExp = []aero.PredExp{
				aero.NewPredExpRecLastUpdate(),
				aero.NewPredExpIntegerValue(ts),
				aero.NewPredExpIntegerGreater(),
			}
			log.Printf("Polling records %v", as.scanPolicy.PredExp)

			as.doScan()
		}
	}
}

func (as *AerospikeSource) doScan() {
	recordSet, err := as.client.ScanAll(as.scanPolicy, as.properties.Namespase, as.properties.SetName)
	if err != nil {
		log.Printf("Aerospike client.ScanAll failed with: %v", err)
	} else {
		for result := range recordSet.Results() {
			as.records <- result
		}
	}
}

// init starts the main loop
func (as *AerospikeSource) init() {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

loop:
	for {
		select {
		case <-sigchan:
			break loop
		case <-as.ctx.Done():
			break loop
		case result, ok := <-as.records:
			if !ok {
				break loop
			}
			if result.Err == nil {
				as.out <- result.Record
			} else {
				log.Printf("Scan record error %s", result.Err)
			}
		}
	}

	log.Printf("Closing Aerospike consumer")
	close(as.out)
	as.client.Close()
}

// Via streams data through the given flow
func (as *AerospikeSource) Via(_flow streams.Flow) streams.Flow {
	flow.DoStream(as, _flow)
	return _flow
}

// Out returns an output channel for sending data
func (as *AerospikeSource) Out() <-chan interface{} {
	return as.out
}

// AerospikeKeyBins is an Aerospike Key and BinMap container
// use it to stream records to the AerospikeSink
type AerospikeKeyBins struct {
	Key  *aero.Key
	Bins aero.BinMap
}

// AerospikeSink connector
type AerospikeSink struct {
	client      *aero.Client
	in          chan interface{}
	ctx         context.Context
	properties  *AerospikeProperties
	writePolicy *aero.WritePolicy
}

// NewAerospikeSink returns a new AerospikeSink instance
func NewAerospikeSink(ctx context.Context,
	properties *AerospikeProperties, writePolicy *aero.WritePolicy) (*AerospikeSink, error) {
	client, err := aero.NewClientWithPolicy(properties.Policy, properties.Hostname, properties.Port)
	if err != nil {
		return nil, err
	}

	if writePolicy == nil {
		writePolicy = aero.NewWritePolicy(0, 0)
	}

	source := &AerospikeSink{
		client:      client,
		in:          make(chan interface{}),
		ctx:         ctx,
		properties:  properties,
		writePolicy: writePolicy,
	}

	go source.init()
	return source, nil
}

// init starts the main loop
func (as *AerospikeSink) init() {
	for msg := range as.in {
		switch m := msg.(type) {
		case AerospikeKeyBins:
			if err := as.client.Put(as.writePolicy, m.Key, m.Bins); err != nil {
				log.Printf("Aerospike client.Put failed with: %s", err)
			}
		case aero.BinMap:
			// use the sha256 checksum of the BinMap as a Key
			jsonStr, err := json.Marshal(m)
			if err == nil {
				key, err := aero.NewKey(as.properties.Namespase,
					as.properties.SetName,
					sha256.Sum256([]byte(jsonStr)))
				if err == nil {
					as.client.Put(as.writePolicy, key, m)
				}
			}

			if err != nil {
				log.Printf("Error on processing Aerospike message: %s", err)
			}
		default:
			log.Printf("Unsupported message type %v", m)
		}
	}
	as.client.Close()
}

// In returns an input channel for receiving data
func (as *AerospikeSink) In() chan<- interface{} {
	return as.in
}
