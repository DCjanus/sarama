package sarama

import "slices"

// MetadataSnapshot provides a read-only view of the client's cached metadata.
// It does not trigger network calls and may be stale if no refresh has happened yet.
type MetadataSnapshot struct {
	Brokers map[int32]string
	Topics  map[string]map[int32]PartitionMetadata
}

func (ncc *nopCloserClient) MetadataSnapshot() *MetadataSnapshot {
	return ncc.Client.MetadataSnapshot()
}

// MetadataSnapshot returns a copy of the current cached metadata without triggering a refresh.
// It is safe for concurrent use and the returned snapshot is disconnected from internal caches.
func (client *client) MetadataSnapshot() *MetadataSnapshot {
	if client.Closed() {
		return nil
	}

	client.lock.RLock()
	defer client.lock.RUnlock()

	if client.updateMetadataMs.Load() == 0 {
		return nil
	}

	snapshot := &MetadataSnapshot{
		Brokers: make(map[int32]string, len(client.brokers)),
		Topics:  make(map[string]map[int32]PartitionMetadata, len(client.metadata)),
	}

	for id, broker := range client.brokers {
		snapshot.Brokers[id] = broker.Addr()
	}

	for topic, partitions := range client.metadata {
		pmap := make(map[int32]PartitionMetadata, len(partitions))
		for pid, pm := range partitions {
			pmap[pid] = clonePartitionMetadata(pm)
		}
		snapshot.Topics[topic] = pmap
	}

	return snapshot
}

func clonePartitionMetadata(pm *PartitionMetadata) PartitionMetadata {
	clone := *pm // struct copy to detach from internal cache
	clone.Replicas = slices.Clone(pm.Replicas)
	clone.Isr = slices.Clone(pm.Isr)
	clone.OfflineReplicas = slices.Clone(pm.OfflineReplicas)
	return clone
}
