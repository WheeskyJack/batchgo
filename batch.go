// Package batchgo provides batch provides batch processing functionality.
// It reads the data using Add method and processes it as per the Slicer interface defined.
// It launches the background goroutine to collect and export batches.
// Batch supports both size and time based batching.
package batchgo

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/benbjohnson/clock"
)

type Slicer interface {
	// Append handles merge of element into batch data
	Append(interface{})
	// Len provieds current batch length
	Len() int
	// Export handles the export of batched data
	Export() error
	// OnFailure handles any error returned from Export()
	OnFailure(error)
}

// Batch supports both size and time based batching.
// Size : item count after which a batch will be sent regardless of the Timeout.
// Timeout : time duration after which a batch will be sent regardless of Size.
type Batch struct {
	// Size is item count after which a batch will be sent regardless of the Timeout
	// must be non zero
	Size int

	// Timeout is time duration after which a batch will be sent regardless of Size
	// must be non zero
	Timeout time.Duration
	timer   *clock.Timer
	clock   clock.Clock

	// NewSlice is the method using which new slice can be craeted for export purpose.
	// All the items received will be merged into this.
	NewSlice func() Slicer

	// ReqBufferSize is the buffer size of req channel used in Add().
	// defualt value is 10.
	ReqBufferSize int
	req           chan interface{} // bufferred channel with default size of 10

	quit    chan struct{}  // signal the workers to stop
	startWg sync.WaitGroup // for stop and wait
	reqWg   sync.WaitGroup // for stop and wait

	isRunning bool // running state of batcher
	muState   sync.RWMutex
}

// Option configures options for Batch
type Option func(b *Batch)

// WithClock sets the clock.Clock used by the batcher.
// This is mainly used in unit tests.
func WithClock(clock clock.Clock) Option {
	return func(b *Batch) {
		b.clock = clock
	}
}

// WithReqBufferSize sets the buffer size for req channel.
func WithReqBufferSize(size int) Option {
	return func(b *Batch) {
		b.ReqBufferSize = size
	}
}

// New creates new batcher.
func New(size int, timeout time.Duration, createSlice func() Slicer, opts ...Option) (*Batch, error) {
	if timeout <= 0 {
		return nil, fmt.Errorf("timeout must be greater than 0")
	}
	if size <= 0 {
		return nil, fmt.Errorf("size must be greater than 0")
	}
	if createSlice == nil {
		return nil, fmt.Errorf("createSlice must be provided")
	}
	b := &Batch{
		Size:          size,
		Timeout:       timeout,
		NewSlice:      createSlice,
		ReqBufferSize: 10,
		isRunning:     false,
		muState:       sync.RWMutex{},
		clock:         clock.New(),
	}
	for _, opt := range opts {
		opt(b)
	}
	return b, nil
}

// Add puts up the item for merging and is async merge operation.
// this is concurrency-safe call.
func (b *Batch) Add(ctx context.Context, item interface{}) error {
	if !b.IsRunning() {
		return fmt.Errorf("batcher is not running")
	}
	b.reqWg.Add(1) // mark the request for waiting on worker receive
	select {
	case b.req <- item:
		return nil
	case <-b.quit:
		b.reqWg.Done() // mark the request process done
		return fmt.Errorf("batcher shutdown received")
	case <-ctx.Done():
		b.reqWg.Done() // mark the request process done
		return ctx.Err()
	}
}

// Start starts processing the batch in async fashion.
// concurrent calls to Start should be avoided;
// otherwise code may panic.
func (b *Batch) Start() {
	if b.IsRunning() {
		return
	}

	b.startWg = sync.WaitGroup{}
	b.reqWg = sync.WaitGroup{}
	b.startWg.Add(1)
	b.req = make(chan interface{}, b.ReqBufferSize)
	b.quit = make(chan struct{})
	b.setRunning(true)

	go func() {
		defer func() {
			b.startWg.Done()
			b.setRunning(false)
		}()
		bdata := b.NewSlice()
		b.timer = b.clock.Timer(b.Timeout)

		for {
			select {
			case item, ok := <-b.req:
				if !ok { // channel closed
					if bdata.Len() > 0 { // export leftover data
						b.send(bdata)
					}
					b.stopTimer()
					return
				}

				bdata.Append(item) // batch data
				b.reqWg.Done()     // mark the request process done

				if bdata.Len() >= b.Size { // batch size limit reached, export the data
					b.send(bdata)
					b.stopTimer()
					b.resetTimer() // reset timer so that next batch get entire timeout for batching
					bdata = b.NewSlice()
				}
			case <-b.timer.C:
				if bdata.Len() > 0 { // time limit reached, export the data
					b.send(bdata)
					bdata = b.NewSlice()
				}
				b.stopTimer()
				b.resetTimer()
			}
		}
	}()
}

func (b *Batch) send(bdata Slicer) {
	err := bdata.Export()
	if err != nil {
		bdata.OnFailure(err)
	}
}

// Stop executes gracefull shutdown;
// Stop waits till all in flight requests are finsihed and exported.
func (b *Batch) Stop() {
	if !b.IsRunning() {
		return
	}
	b.setRunning(false) // stop incoming requests
	close(b.quit)       // fail requests in wait state
	b.reqWg.Wait()      // wait till requests in worker to finish
	close(b.req)        // trigger close of worker
	b.startWg.Wait()    // wait for worker to close
}

func (b *Batch) stopTimer() {
	if !b.timer.Stop() {
		select {
		case <-b.timer.C:
		default:
		}
	}
}

func (b *Batch) resetTimer() {
	b.timer.Reset(b.Timeout)
}

func (b *Batch) IsRunning() bool {
	b.muState.RLock()
	defer b.muState.RUnlock()
	return b.isRunning
}

func (b *Batch) setRunning(running bool) {
	b.muState.Lock()
	defer b.muState.Unlock()
	b.isRunning = running
}
