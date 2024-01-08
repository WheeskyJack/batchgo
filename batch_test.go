package batchgo_test

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/WheeskyJack/batchgo"
	"github.com/benbjohnson/clock"
	gomock "github.com/golang/mock/gomock"
)

type counter struct {
	count int
	mu    sync.RWMutex
}

func (c *counter) Incr(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count += i
}

func (c *counter) Set(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.count = i
}

func (c *counter) Get() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.count
}

func TestNewBatch(t *testing.T) {
	// mock the Slicer  interface

	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)
	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}

	type args struct {
		size        int
		timeout     time.Duration
		createSlice func() batchgo.Slicer
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{
			name: "valid_batch_req",
			args: args{
				size:        10,
				timeout:     10 * time.Second,
				createSlice: newSlicer,
			},
			wantErr: false,
		},
		{
			name: "zero_batch_size",
			args: args{
				size:        0,
				timeout:     10 * time.Second,
				createSlice: newSlicer,
			},
			wantErr: true,
		},
		{
			name: "zero_timeout",
			args: args{
				size:        10,
				timeout:     0,
				createSlice: newSlicer,
			},
			wantErr: true,
		},
		{
			name: "nil_create_slice_func",
			args: args{
				size:        10,
				timeout:     10 * time.Second,
				createSlice: nil,
			},
			wantErr: true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := batchgo.New(tt.args.size, tt.args.timeout, tt.args.createSlice)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewBatch() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
		})
	}
}

func TestBatchAdd(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)

	cl := clock.NewMock()
	timer := 10
	delay := 100
	timeOut := time.Duration(timer) * time.Millisecond

	mockSlicer.EXPECT().Append(1).Times(1).
		Do(func(elem interface{}) {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		})

	mockSlicer.EXPECT().Len().AnyTimes().
		DoAndReturn(func() int {
			return 1
		})

	mockSlicer.EXPECT().Export().Times(1).
		DoAndReturn(func() error {
			return fmt.Errorf("dummy error") // test OnFailure()
		})

	mockSlicer.EXPECT().OnFailure(gomock.Any()).Times(1)

	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}

	b, err := batchgo.New(30, timeOut, newSlicer, batchgo.WithClock(cl), batchgo.WithReqBufferSize(0))
	if err != nil {
		t.Errorf("New() error = %v ", err)
	}
	b.Start()
	for i := 0; i < 100; i++ { // wait till batch start
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	// context timeout test
	err = b.Add(context.Background(), 1) // block the channel
	if err != nil {
		t.Errorf("Add() error = %v ", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeOut)
	defer cancel()
	err = b.Add(ctx, 2)
	if err == nil {
		t.Errorf("Add() nil error received for context cancel")
	}

	// quit test
	mockSlicer.EXPECT().Append(11).Times(1).
		Do(func(elem interface{}) {
			time.Sleep(time.Duration(delay) * time.Millisecond)
		})

	err = b.Add(context.Background(), 11) // block the channel
	if err != nil {
		t.Errorf("Add() error = %v ", err)
	}

	done := make(chan error)
	go func() {
		done <- b.Add(context.Background(), 22)
	}()
	time.Sleep(10 * time.Millisecond) // allow add to block
	b.Stop()
	err = <-done
	if err.Error() != "batcher shutdown received" {
		t.Errorf("Add() error = %v ", err)
	}

}

func TestBatchStartStop(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)

	cl := clock.NewMock()
	timer := 10
	timeOut := time.Duration(timer) * time.Millisecond

	mockSlicer.EXPECT().Append(1).Times(2).
		Do(func(elem interface{}) {
		})

	mockSlicer.EXPECT().Len().AnyTimes().
		DoAndReturn(func() int {
			return 1
		})

	mockSlicer.EXPECT().Export().Times(2).
		DoAndReturn(func() error {
			return fmt.Errorf("dummy error") // test OnFailure()
		})

	mockSlicer.EXPECT().OnFailure(gomock.Any()).Times(2)

	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}

	b, err := batchgo.New(30, timeOut, newSlicer, batchgo.WithClock(cl), batchgo.WithReqBufferSize(0))
	if err != nil {
		t.Errorf("New() error = %v ", err)
	}
	b.Start()
	for i := 0; i < 100; i++ { // wait till batch start
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	// should not panic if already running - test
	b.Start()
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	err = b.Add(context.Background(), 1)
	if err != nil {
		t.Errorf("Add() error = %v ", err)
	}

	b.Stop() // graceful shutdown
	if b.IsRunning() {
		t.Fatalf("Batch is running")
	}

	// should not panic if already stopped - test
	b.Stop()
	if b.IsRunning() {
		t.Fatalf("Batch is running")
	}

	// send on stopped batch test
	err = b.Add(context.Background(), 12)
	if err.Error() != "batcher is not running" {
		t.Errorf("Add() error = %v ", err)
	}

	// start batch again

	b.Start()
	for i := 0; i < 100; i++ { // wait till batch start
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}
	err = b.Add(context.Background(), 1)
	if err != nil {
		t.Errorf("Add() error = %v ", err)
	}

	b.Stop() // graceful shutdown
	if b.IsRunning() {
		t.Fatalf("Batch is running")
	}
}

func TestValidAddBatchWithoutTimeout(t *testing.T) {
	sendItemsForBatchCount := []int{2, 3, 10, 12, 300}
	batchSize := 3
	for _, itemCount := range sendItemsForBatchCount {
		t.Run(strconv.Itoa(itemCount), func(t *testing.T) {
			runTestWithoutTimeout(t, itemCount, batchSize)
		})
	}
}

func runTestWithoutTimeout(t *testing.T, itemCountTotal, batchSize int) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)
	var exportedBatchSize = &counter{
		count: 0,
		mu:    sync.RWMutex{},
	}

	for i := 0; i < itemCountTotal; i++ {
		mockSlicer.EXPECT().Append(i).Times(1).
			Do(func(elem interface{}) {
				exportedBatchSize.Incr(1) // increment count by one when item gets added to batch
			})
	}

	mockSlicer.EXPECT().Len().AnyTimes().
		DoAndReturn(func() int {
			return exportedBatchSize.Get()
		})

	exportCallExecutedCount := itemCountTotal / batchSize
	reminderElems := itemCountTotal % batchSize
	if reminderElems > 0 {
		exportCallExecutedCount++
	}
	mockSlicer.EXPECT().Export().Times(exportCallExecutedCount).
		DoAndReturn(func() error {
			if (exportedBatchSize.Get() != batchSize) &&
				(exportedBatchSize.Get() != reminderElems) { // check for correct batch size
				return fmt.Errorf("batchsize not matching %v:%v:%v", exportedBatchSize.Get(), batchSize, reminderElems)
			}
			exportedBatchSize.Set(0) // reset count to zero after export of batch is done
			return nil
		})

	mockSlicer.EXPECT().OnFailure(gomock.Any()).Times(0)

	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}
	cl := clock.NewMock()

	b, err := batchgo.New(batchSize, 1*time.Second, newSlicer, batchgo.WithClock(cl))
	if err != nil {
		t.Errorf("New() error = %v ", err)
	}
	b.Start()
	for i := 0; i < 100; i++ {
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	for i := 0; i < itemCountTotal; i++ { // send data for batching
		err := b.Add(context.Background(), i)
		if err != nil {
			t.Errorf("Add() error = %v ", err)
		}
	}

	b.Stop()
}

func TestValidAddBatchWithOnlyTimeout(t *testing.T) {
	batchSize := 12
	for batchCountTotal := 1; batchCountTotal < batchSize; batchCountTotal++ {
		t.Run(strconv.Itoa(batchCountTotal), func(t *testing.T) {
			runTestWithOnlyTimeout(t, batchCountTotal, batchSize)
		})
	}
}

func runTestWithOnlyTimeout(t *testing.T, batchCountTotal, batchSize int) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)
	var exportedBatchSize = &counter{
		count: 0,
		mu:    sync.RWMutex{},
	}
	cl := clock.NewMock()

	for i := 1; i < batchSize; i++ {
		mockSlicer.EXPECT().Append(i).Times(1).
			Do(func(elem interface{}) {
				exportedBatchSize.Incr(1)
			})
	}
	mockSlicer.EXPECT().Len().AnyTimes().
		DoAndReturn(func() int {
			return exportedBatchSize.Get()
		})

	exportCallExecutedCount := (batchSize - 1) / batchCountTotal
	reminderElems := (batchSize - 1) % batchCountTotal
	if reminderElems > 0 {
		exportCallExecutedCount++
	}

	mockSlicer.EXPECT().Export().Times(exportCallExecutedCount).
		DoAndReturn(func() error {
			if (exportedBatchSize.Get() != batchCountTotal) &&
				(exportedBatchSize.Get() != reminderElems) { // check for correct batch size
				return fmt.Errorf("batchsize not matching %v:%v:%v", exportedBatchSize.Get(), batchCountTotal, reminderElems)
			}
			//cl.WaitForAllTimers()
			exportedBatchSize.Set(0) // reset count to zero after export of batch is done
			return nil
		})

	mockSlicer.EXPECT().OnFailure(gomock.Any()).Times(0)

	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}

	waitTime := 100 * time.Millisecond
	mustSleep := func(d time.Duration) {
		cl.Add(d)
		time.Sleep(waitTime)
	}
	timeOut := 1 * time.Second

	b, err := batchgo.New(batchSize, timeOut, newSlicer, batchgo.WithClock(cl))
	if err != nil {
		t.Errorf("New() error = %v ", err)
	}
	b.Start()
	for i := 0; i < 100; i++ { // wait till batch start
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	for itemIndex := 1; itemIndex < batchSize; itemIndex++ { // send data for batching
		err := b.Add(context.Background(), itemIndex)
		if err != nil {
			t.Errorf("Add() error = %v ", err)
		}

		if itemIndex%batchCountTotal == 0 {
			for jj := 0; jj < 100; jj++ { // wait for worker to process the items
				if exportedBatchSize.Get() == batchCountTotal {
					break
				}
				if jj == 99 {
					t.Errorf("worker didnt process the items within time")
				}
				time.Sleep(10 * time.Millisecond)
			}

			mustSleep(timeOut) // trigger the batcher export

			for jj := 0; jj < 100; jj++ { // wait for worker to export the items
				if exportedBatchSize.Get() == 0 {
					break
				}
				if jj == 99 {
					t.Errorf("export didnt process the items within time")
				}
				time.Sleep(10 * time.Millisecond)
			}

			// trigger the timeout without adding any element, export should not be attempted
			mustSleep(timeOut)
			time.Sleep(100 * time.Millisecond)
			mustSleep(timeOut)
			time.Sleep(100 * time.Millisecond)
		}
	}

	b.Stop()
}

func TestAddBatchWithoutMockTime(t *testing.T) {
	itemcount := []int{2, 3, 10, 12, 300}
	batchSize := 3
	for _, itemCountTotal := range itemcount {
		t.Run(strconv.Itoa(itemCountTotal), func(t *testing.T) {
			runTesthWithoutMockTime(t, itemCountTotal, batchSize)
		})
	}
}

func runTesthWithoutMockTime(t gomock.TestReporter, itemCountTotal, batchSize int) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	mockSlicer := NewMockSlicer(mockCtrl)
	var lc = &counter{
		count: 0,
		mu:    sync.RWMutex{},
	}
	var totalCount = &counter{
		count: 0,
		mu:    sync.RWMutex{},
	}
	timer := 10
	timeOut := time.Duration(timer) * time.Millisecond

	for i := 0; i < itemCountTotal; i++ {
		mockSlicer.EXPECT().Append(i).Times(1).
			Do(func(elem interface{}) {
				lc.Incr(1)
			})
	}
	mockSlicer.EXPECT().Len().AnyTimes().
		DoAndReturn(func() int {
			return lc.Get()
		})

	mockSlicer.EXPECT().Export().AnyTimes().
		DoAndReturn(func() error {
			//cl.WaitForAllTimers()
			totalCount.Incr(lc.Get())
			if lc.Get() > batchSize { // check for correct batch size
				return fmt.Errorf("batchsize not matching %v:%v", lc.Get(), batchSize)
			}
			lc.Set(0) // reset count to zero after export of batch is done
			return nil
		})

	mockSlicer.EXPECT().OnFailure(gomock.Any()).Times(0)

	newSlicer := func() batchgo.Slicer {
		return mockSlicer
	}

	b, err := batchgo.New(batchSize, timeOut, newSlicer)
	if err != nil {
		t.Errorf("New() error = %v ", err)
	}
	b.Start()
	for i := 0; i < 100; i++ { // wait till batch start
		if b.IsRunning() {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if !b.IsRunning() {
		t.Fatalf("Batch is not running")
	}

	for i := 0; i < itemCountTotal; i++ { // send data for batching
		err := b.Add(context.Background(), i)
		if err != nil {
			t.Errorf("Add() error = %v ", err)
		}
		r := rand.Intn(timer)
		time.Sleep(time.Duration(r) * time.Millisecond) // sleep for random time to trigger batch timer
	}

	b.Stop()
	if totalCount.Get() != itemCountTotal {
		t.Errorf("not all items exported %v:%v", totalCount.Get(), itemCountTotal)
	}
}

func BenchmarkBatchBasic(b *testing.B) {
	b.Run("batch-benchmark-test", func(b *testing.B) {
		benchmarkBatch(b, 1500, 5)
	})

}
func benchmarkBatch(b *testing.B, itemCountTotal, batchSize int) {
	for n := 0; n < b.N; n++ {
		runTesthWithoutMockTime(b, itemCountTotal, batchSize)
	}
}
