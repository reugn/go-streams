package extension

import (
	"bufio"
	"log"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
	"github.com/reugn/go-streams/util/ospkg"
)

// FileSource represents an inbound connector that creates a stream of
// elements from a file. The streaming element is a new line in the file.
type FileSource struct {
	fileName string
	in       chan any
}

var _ streams.Source = (*FileSource)(nil)

// NewFileSource returns a new FileSource connector.
func NewFileSource(fileName string) *FileSource {
	source := &FileSource{
		fileName: fileName,
		in:       make(chan any),
	}
	source.init()
	return source
}

func (fs *FileSource) init() {
	go func() {
		file, err := os.Open(fs.fileName)
		if err != nil {
			log.Fatalf("FileSource failed to open the file %s", fs.fileName)
		}
		defer file.Close()
		reader := bufio.NewReader(file)
		for {
			lineBytes, isPrefix, err := reader.ReadLine()
			if err != nil {
				close(fs.in)
				break
			}

			var element string
			if isPrefix {
				element = string(lineBytes)
			} else {
				element = string(lineBytes) + ospkg.NewLine
			}

			fs.in <- element
		}
	}()
}

// Via streams data to a specified operator and returns it.
func (fs *FileSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(fs, operator)
	return operator
}

// Out returns the output channel of the FileSource connector.
func (fs *FileSource) Out() <-chan any {
	return fs.in
}

// FileSink represents an outbound connector that writes streaming data
// to a file.
type FileSink struct {
	fileName string
	in       chan any
}

var _ streams.Sink = (*FileSink)(nil)

// NewFileSink returns a new FileSink connector.
func NewFileSink(fileName string) *FileSink {
	sink := &FileSink{
		fileName: fileName,
		in:       make(chan any),
	}
	sink.init()
	return sink
}

func (fs *FileSink) init() {
	go func() {
		file, err := os.OpenFile(fs.fileName, os.O_CREATE|os.O_WRONLY, 0600)
		if err != nil {
			log.Fatalf("FileSink failed to open the file %s", fs.fileName)
		}
		defer file.Close()
		for element := range fs.in {
			_, err = file.WriteString(element.(string))
			if err != nil {
				log.Fatalf("FileSink failed to write to the file %s", fs.fileName)
			}
		}
	}()
}

// In returns the input channel of the FileSink connector.
func (fs *FileSink) In() chan<- any {
	return fs.in
}
