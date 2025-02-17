package extension

import (
	"errors"
	"io"
	"log/slog"

	"github.com/reugn/go-streams"
	"github.com/reugn/go-streams/flow"
)

// ElementReader reads and parses an element from the provided io.Reader.
// It returns the parsed element as a byte slice or an error if parsing fails.
// Implementations should handle io.EOF appropriately to signal the end of the stream.
type ElementReader func(io.Reader) ([]byte, error)

// ReaderSource represents an inbound connector that reads elements from an io.Reader.
// It uses an ElementReader function to parse elements from the stream.
type ReaderSource struct {
	reader        io.ReadCloser
	elementReader ElementReader
	out           chan any

	opts options
}

var _ streams.Source = (*ReaderSource)(nil)

// NewReaderSource returns a new ReaderSource connector.
// It takes an io.ReadCloser, an ElementReader function, and optional configuration options.
func NewReaderSource(reader io.ReadCloser, elementReader ElementReader,
	opts ...Opt) (*ReaderSource, error) {
	if reader == nil {
		return nil, errors.New("reader is nil")
	}
	if elementReader == nil {
		return nil, errors.New("elementReader is nil")
	}

	readerSource := &ReaderSource{
		reader:        reader,
		elementReader: elementReader,
		out:           make(chan any),
		opts:          makeDefaultOptions(),
	}

	// apply functional options to configure the source
	for _, opt := range opts {
		opt(&readerSource.opts)
	}

	// asynchronously send element bytes downstream
	go readerSource.process()

	return readerSource, nil
}

// process reads elements from the reader, parses them using the ElementReader,
// and sends them to the output channel.
func (s *ReaderSource) process() {
	defer func() {
		if err := s.reader.Close(); err != nil {
			s.opts.logger.Error("Failed to close reader", slog.Any("error", err))
		}
		s.opts.logger.Info("Closed reader")
		close(s.out)
	}()

	for {
		select {
		case <-s.opts.ctx.Done():
			s.opts.logger.Info("Context canceled", slog.Any("error", s.opts.ctx.Err()))
			return
		default:
			elementBytes, err := s.elementReader(s.reader)
			if err != nil {
				if errors.Is(err, io.EOF) {
					s.opts.logger.Info("Reader finished", slog.Any("error", err))
					s.emitElement(elementBytes)
				} else {
					s.opts.logger.Error("Failed to read element", slog.Any("error", err))
				}
				return
			}

			s.emitElement(elementBytes)
		}
	}
}

// emitElement sends the element downstream to the output channel if the context
// is not canceled and the element is not empty.
func (s *ReaderSource) emitElement(element []byte) {
	if len(element) > 0 && s.opts.ctx.Err() == nil {
		s.out <- element
	}
}

// Via asynchronously streams data to the given Flow and returns it.
func (s *ReaderSource) Via(operator streams.Flow) streams.Flow {
	flow.DoStream(s, operator)
	return operator
}

// Out returns the output channel of the ReaderSource connector.
func (s *ReaderSource) Out() <-chan any {
	return s.out
}
