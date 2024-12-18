package extension

import (
	"encoding/csv"
	"log"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

type CsvSource struct {
	csvFileName string
	out         chan any
}

var _ streams.Source = (*CsvSource)(nil)

// NewCsvSource returns a new CsvSource connector.
func NewCsvSource(csvFileName string) CsvSource {
	source := CsvSource{
		csvFileName: csvFileName,
		out:         make(chan any),
	}

	go source.init()

	return source
}

func (c CsvSource) init() {
	file, err := os.Open(c.csvFileName)
	if err != nil {
		log.Fatalf("CsvSource failed to open the file %s", c.csvFileName)
	}
	defer file.Close()
	reader := csv.NewReader(file)
	for {
		record, err := reader.Read()
		if err != nil {
			close(c.out)
			break
		}

		c.out <- record
	}
}

func (c CsvSource) Out() <-chan any {
	return c.out
}

func (c CsvSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(c, operator)
	return operator
}

type CsvSink struct {
	csvFileName string
	in          chan any
}

func (c CsvSink) In() chan<- any {
	return c.in
}

func (c CsvSink) init() {
	file, err := os.OpenFile(c.csvFileName, os.O_CREATE|os.O_WRONLY, 0600)
	if err != nil {
		log.Fatalf("CsvSink failed to open the file %s", c.csvFileName)
	}
	defer file.Close()

	writer := csv.NewWriter(file)

	for element := range c.in {
		err := writer.Write(element.([]string))
		if err != nil {
			log.Fatalf("CsvSink failed to write to file %s", c.csvFileName)
		}
	}
}

var _ streams.Sink = (*CsvSink)(nil)

// NewCsvSink returns a new CsvSink connector.
func NewCsvSink(csvFileName string) CsvSink {
	sink := CsvSink{
		csvFileName: csvFileName,
		in:          make(chan any),
	}

	go sink.init()

	return sink
}
