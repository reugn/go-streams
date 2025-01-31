package extension

import (
	"bufio"
	"fmt"
	"log"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
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
	fileSource := &FileSource{
		fileName: fileName,
		in:       make(chan any),
	}

	// asynchronously send file data downstream
	go fileSource.process()

	return fileSource
}

func (fs *FileSource) process() {
	file, err := os.Open(fs.fileName)
	if err != nil {
		log.Fatalf("FileSource failed to open the file %s: %v", fs.fileName, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("FileSource failed to close the file %s: %v", fs.fileName, err)
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		// send the file line downstream
		fs.in <- scanner.Text()
	}

	// check for errors that occurred during scanning
	if err := scanner.Err(); err != nil {
		log.Printf("FileSource scanner error: %v", err)
	}

	close(fs.in)
}

// Via asynchronously streams data to the given Flow and returns it.
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
	done     chan struct{}
}

var _ streams.Sink = (*FileSink)(nil)

// NewFileSink returns a new FileSink connector.
func NewFileSink(fileName string) *FileSink {
	fileSink := &FileSink{
		fileName: fileName,
		in:       make(chan any),
		done:     make(chan struct{}),
	}

	// asynchronously process stream data
	go fileSink.process()

	return fileSink
}

func (fs *FileSink) process() {
	defer close(fs.done)

	file, err := os.Create(fs.fileName)
	if err != nil {
		log.Fatalf("FileSink failed to open the file %s: %v", fs.fileName, err)
	}
	defer func() {
		if err := file.Close(); err != nil {
			log.Printf("FileSink failed to close the file %s: %v", fs.fileName, err)
		}
	}()

	for element := range fs.in {
		var stringElement string
		switch v := element.(type) {
		case string:
			stringElement = v
		case fmt.Stringer:
			stringElement = v.String()
		default:
			log.Printf("FileSink received an unsupported type %T, discarding", v)
			continue
		}

		// Write the processed string element to the file. If an error occurs,
		// terminate the sink.
		if _, err := file.WriteString(stringElement); err != nil {
			log.Fatalf("FileSink failed to write to the file %s: %v", fs.fileName, err)
		}
	}
}

// In returns the input channel of the FileSink connector.
func (fs *FileSink) In() chan<- any {
	return fs.in
}

// AwaitCompletion blocks until the FileSink has completed processing and
// flushing all data to the file.
func (fs *FileSink) AwaitCompletion() {
	<-fs.done
}
