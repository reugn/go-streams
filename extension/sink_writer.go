package extension

import (
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/reugn/go-streams"
)

// WriterSink represents an outbound connector that writes data to an io.WriteCloser.
// It consumes messages from its input channel, converts them to byte slices,
// and writes them to the configured writer.
type WriterSink struct {
	writer io.WriteCloser
	in     chan any
	done   chan struct{}

	opts options
}

var _ streams.Sink = (*WriterSink)(nil)

// NewWriterSink returns a new WriterSink connector.
// It takes an io.WriteCloser, and optional configuration options.
func NewWriterSink(writer io.WriteCloser, opts ...Opt) (*WriterSink, error) {
	if writer == nil {
		return nil, errors.New("writer is nil")
	}

	writerSink := &WriterSink{
		writer: writer,
		in:     make(chan any),
		done:   make(chan struct{}),
		opts:   makeDefaultOptions(),
	}

	// apply functional options to configure the sink
	for _, opt := range opts {
		opt(&writerSink.opts)
	}

	// asynchronously process stream data
	go writerSink.process()

	return writerSink, nil
}

// process reads messages from the input channel, converts them to byte slices,
// and writes them to the writer.
func (s *WriterSink) process() {
	defer close(s.done)

	for msg := range s.in {
		var elementBytes []byte
		switch message := msg.(type) {
		case []byte:
			elementBytes = message
		case string:
			elementBytes = []byte(message)
		case fmt.Stringer:
			elementBytes = []byte(message.String())
		default:
			s.opts.logger.Warn("Discarded stream element",
				slog.String("type", fmt.Sprintf("%T", message)))
			continue
		}

		// Write the received bytes to the writer. Use the specified
		// retry function to retry if an error occurs.
		// If failed to write, cancel the source context, drain the input
		// channel and terminate the stream processing.
		if err := s.opts.retryFunc(s.opts.ctx, func() error {
			_, err := s.writer.Write(elementBytes)
			return err
		}); err != nil {
			s.opts.logger.Error("Failed to write element",
				slog.Any("error", err))

			// cancel the source context
			s.opts.ctxCancel()

			// discard buffered input elements
			drainChan(s.in)
		}
	}

	if err := s.writer.Close(); err != nil {
		s.opts.logger.Error("Failed to close writer", slog.Any("error", err))
	}
	s.opts.logger.Info("Closed writer")
}

// In returns the input channel of the WriterSink connector.
func (s *WriterSink) In() chan<- any {
	return s.in
}

// AwaitCompletion blocks until the WriterSink has processed all received data,
// closed the connection, and released all resources.
func (s *WriterSink) AwaitCompletion() {
	<-s.done
}
