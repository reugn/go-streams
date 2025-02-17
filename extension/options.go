package extension

import (
	"context"
	"log/slog"
)

// options represents configuration options for standard connectors.
type options struct {
	logger    *slog.Logger
	ctx       context.Context
	ctxCancel context.CancelFunc
	retryFunc func(context.Context, func() error) error
}

// makeDefaultOptions returns an options with default values.
func makeDefaultOptions() options {
	return options{
		logger:    slog.Default(),
		ctx:       context.Background(),
		ctxCancel: func() {},
		retryFunc: func(_ context.Context, f func() error) error { return f() },
	}
}

// Opt is a functional option type used to configure an options.
type Opt func(*options)

// WithLogger configures the options with a custom logger.
// If not specified, the default slog.Default() will be used.
func WithLogger(logger *slog.Logger) Opt {
	return func(o *options) {
		o.logger = logger
	}
}

// WithContext configures the options with a context.
// Can be customized in source connectors to gracefully stop streaming data when
// cancelled. For sink connectors, this allows for configuring retry behavior.
//
// Sink connectors can use WithContextCancel to trigger cancellation of the source
// context upon encountering fatal errors.
func WithContext(ctx context.Context) Opt {
	return func(o *options) {
		o.ctx = ctx
	}
}

// WithContextCancel configures the options with a context cancellation function.
// This function should be called when a fatal error occurs in the connector, allowing
// for proper cleanup.
//
// Primarily intended for sink connectors to signal to source connectors that they
// should stop producing data.
// Source connectors should generally ignore this option.
func WithContextCancel(ctxCancel context.CancelFunc) Opt {
	return func(o *options) {
		o.ctxCancel = ctxCancel
	}
}

// WithRetryFunc configures the options with a custom retry function.
// This function will be used to retry operations that fail.
// If not specified, the default implementation will execute the function once
// without retries.
//
// The retry function receives the context and the function to be retried.
// It should handle the retry logic, respecting the context's cancellation signal.
func WithRetryFunc(retryFunc func(context.Context, func() error) error) Opt {
	return func(o *options) {
		o.retryFunc = retryFunc
	}
}
