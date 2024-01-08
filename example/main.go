package main

import (
	"context"
	"fmt"
	"time"

	"github.com/WheeskyJack/batchgo"
)

var (
	inpCount = 20
	in       = make(chan Items, 10)
)

type Item struct {
	Name  string
	Value int
}

type Items []Item

func NewItems() batchgo.Slicer {
	return &Items{}
}

// implement batchgo.Slicer

func (i *Items) Append(item interface{}) {
	ii, ok := item.(Item)
	if !ok {
		return
	}
	*i = append(*i, ii)

}

func (i *Items) Len() int {
	return len(*i)
}

func (i *Items) Export() error {
	in <- *i
	return nil
}

func (i *Items) OnFailure(err error) {
	fmt.Println(err)
}

func main() {

	b, err := batchgo.New(2, 1*time.Second, NewItems)
	if err != nil {
		panic(err)
	}
	b.Start()

	for i := 0; i < inpCount; i++ {

		item := Item{
			Name:  fmt.Sprintf("name%d", i),
			Value: i,
		}
		go func(ii Item) {
			err := b.Add(context.Background(), ii)
			if err != nil {
				fmt.Println(err)
			}
		}(item)
	}

	done := make(chan struct{})
	go func() {
		for i := range in {
			fmt.Println(i)
		}
		done <- struct{}{}
	}()

	time.Sleep(5 * time.Second) // allow all print to screen happen
	b.Stop()
	close(in)
	<-done
}
