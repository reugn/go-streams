package extension

import (
	"bufio"
	"fmt"
	"log/slog"
	"os"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// FileSource represents an inbound connector that creates a stream of
// elements from a file. Each line in the file is emitted as a string.
type FileSource struct {
	fileName string
	in       chan any

	opts options
}

var _ streams.Source = (*FileSource)(nil)

// NewFileSource returns a new FileSource connector.
func NewFileSource(fileName string, opts ...Opt) *FileSource {
	fileSource := &FileSource{
		fileName: fileName,
		in:       make(chan any),
		opts:     makeDefaultOptions(),
	}

	// apply functional options to configure the source
	for _, opt := range opts {
		opt(&fileSource.opts)
	}

	// asynchronously send file data downstream
	go fileSource.process()

	return fileSource
}

// process reads the file line by line and sends each line to the output channel.
func (fs *FileSource) process() {
	defer close(fs.in)

	file, err := os.Open(fs.fileName)
	if err != nil {
		fs.opts.logger.Error("Failed to open file",
			slog.String("name", fs.fileName),
			slog.Any("error", err))
		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			fs.opts.logger.Error("Failed to close file",
				slog.String("name", fs.fileName),
				slog.Any("error", err))
		}
	}()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		select {
		case <-fs.opts.ctx.Done():
			fs.opts.logger.Info("Context canceled",
				slog.Any("error", fs.opts.ctx.Err()))
			return
		default:
			// send the file line downstream
			fs.in <- scanner.Text()
		}
	}

	// check for errors that occurred during scanning
	if err := scanner.Err(); err != nil {
		fs.opts.logger.Error("Scanner error", slog.Any("error", err))
	}
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

	opts options
}

var _ streams.Sink = (*FileSink)(nil)

// NewFileSink returns a new FileSink connector.
func NewFileSink(fileName string, opts ...Opt) *FileSink {
	fileSink := &FileSink{
		fileName: fileName,
		in:       make(chan any),
		done:     make(chan struct{}),
		opts:     makeDefaultOptions(),
	}

	// apply functional options to configure the sink
	for _, opt := range opts {
		opt(&fileSink.opts)
	}

	// asynchronously process stream data
	go fileSink.process()

	return fileSink
}

// process reads data from the input channel and writes it to the file.
func (fs *FileSink) process() {
	defer close(fs.done)

	file, err := os.Create(fs.fileName)
	if err != nil {
		fs.opts.logger.Error("Failed to open file",
			slog.String("name", fs.fileName),
			slog.Any("error", err))

		// cancel the source context
		fs.opts.ctxCancel()

		// discard buffered input elements
		drainChan(fs.in)

		return
	}

	defer func() {
		if err := file.Close(); err != nil {
			fs.opts.logger.Error("Failed to close file",
				slog.String("name", fs.fileName),
				slog.Any("error", err))
		}
	}()

	for element := range fs.in {
		var stringElement string
		switch v := element.(type) {
		case string:
			stringElement = v
		case []byte:
			stringElement = string(v)
		case fmt.Stringer:
			stringElement = v.String()
		default:
			fs.opts.logger.Warn("Discarded stream element",
				slog.String("type", fmt.Sprintf("%T", v)))
			continue
		}

		// Write the processed string element to the file. Use the specified
		// retry function to retry if an error occurs.
		// If failed to write, cancel the source context, drain the input
		// channel and terminate the stream processing.
		if err := fs.opts.retryFunc(fs.opts.ctx, func() error {
			_, err := file.WriteString(stringElement)
			return err
		}); err != nil {
			fs.opts.logger.Error("Failed to write to file",
				slog.String("name", fs.fileName),
				slog.Any("error", err))

			// cancel the source context
			fs.opts.ctxCancel()

			// discard buffered input elements
			drainChan(fs.in)
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
