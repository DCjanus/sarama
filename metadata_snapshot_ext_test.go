package sarama

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMetadataSnapshotCopyAndNoRefresh(t *testing.T) {
	client := &client{
		conf:     NewConfig(),
		brokers:  map[int32]*Broker{1: NewBroker("addr1"), 2: NewBroker("addr2")},
		metadata: make(map[string]map[int32]*PartitionMetadata),
	}
	client.lock = sync.RWMutex{}
	client.metadata["foo"] = map[int32]*PartitionMetadata{
		0: {ID: 0, Leader: 1},
		1: {ID: 1, Leader: 2},
	}
	client.updateMetadataMs.Store(time.Now().UnixMilli())

	snap := client.MetadataSnapshot()
	require.NotNil(t, snap)
	require.Equal(t, 2, len(snap.Brokers))
	require.Equal(t, "addr1", snap.Brokers[1])
	require.Equal(t, "addr2", snap.Brokers[2])
	require.Equal(t, 2, len(snap.Topics["foo"]))

	// mutate snapshot; internal cache should remain unchanged
	snap.Brokers[1] = "mutated"
	snap.Topics["foo"][0] = PartitionMetadata{ID: 0, Leader: 99}

	require.Equal(t, "addr1", client.brokers[1].Addr())
	require.Equal(t, int32(1), client.metadata["foo"][0].Leader)
}

func TestMetadataSnapshotNilWhenEmpty(t *testing.T) {
	client := &client{
		conf:     NewConfig(),
		brokers:  map[int32]*Broker{},
		metadata: make(map[string]map[int32]*PartitionMetadata),
	}
	client.lock = sync.RWMutex{}
	client.updateMetadataMs.Store(0)

	require.Nil(t, client.MetadataSnapshot())
}

func TestMetadataSnapshotEmptyClusterButRefreshed(t *testing.T) {
	client := &client{
		conf:     NewConfig(),
		brokers:  map[int32]*Broker{},
		metadata: make(map[string]map[int32]*PartitionMetadata),
	}
	client.lock = sync.RWMutex{}
	client.updateMetadataMs.Store(time.Now().UnixMilli())

	snap := client.MetadataSnapshot()
	require.NotNil(t, snap)
	require.Empty(t, snap.Topics)
	require.Empty(t, snap.Brokers)
}
