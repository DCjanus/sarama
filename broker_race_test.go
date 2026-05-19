//go:build !functional

package sarama

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestBrokerConcurrentOpenAndFetchDoesNotRace(t *testing.T) {
	mockBroker := NewMockBroker(t, 0)
	defer mockBroker.Close()

	mockBroker.SetHandlerByMap(map[string]MockResponse{
		"FetchRequest": NewMockFetchResponse(t, 1).SetHighWaterMark("my_topic", 0, 0),
	})

	broker := NewBroker(mockBroker.Addr())
	t.Cleanup(func() { _ = broker.Close() })

	conf := NewTestConfig()
	conf.Net.DialTimeout = 100 * time.Millisecond
	conf.Net.ReadTimeout = 100 * time.Millisecond
	conf.Net.WriteTimeout = 100 * time.Millisecond

	newFetchRequest := func() *FetchRequest {
		request := &FetchRequest{}
		request.AddBlock("my_topic", 0, 0, 1, 0)
		return request
	}

	var stopFetchers atomic.Bool
	var fetchers sync.WaitGroup
	for range 4 {
		fetchers.Add(1)
		go func() {
			defer fetchers.Done()
			for !stopFetchers.Load() {
				_, _ = broker.Fetch(newFetchRequest())
			}
		}()
	}

	for range 1000 {
		_ = broker.Close()
		err := broker.Open(conf)
		if err != nil && !errors.Is(err, ErrAlreadyConnected) {
			t.Fatalf("failed to open broker: %v", err)
		}
		_, _ = broker.Connected()
	}

	stopFetchers.Store(true)
	fetchers.Wait()
}
