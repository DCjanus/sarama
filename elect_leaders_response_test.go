//go:build !functional

package sarama

import "testing"

var electLeadersResponseOneTopic = []byte{
	0, 0, 3, 232, // ThrottleTimeMs 1000
	0, 0, // errorCode
	2,                         // number of topics
	6, 116, 111, 112, 105, 99, // topic name "topic"
	2,          // number of partitions
	0, 0, 0, 0, // partition 0
	0, 0, // empty tagged fields
	0, 0, // empty tagged fields
	0, 0, // empty tagged fields
}

func TestElectLeadersResponse(t *testing.T) {
	var response = &ElectLeadersResponse{
		Version:        int16(2),
		ThrottleTimeMs: int32(1000),
		ReplicaElectionResults: map[string]map[int32]*PartitionResult{
			"topic": {
				0: {},
			},
		},
	}

	testResponse(t, "one topic", response, electLeadersResponseOneTopic)
}

// electLeadersResponseOneTopicV1 is the expected wire encoding for a V1
// (non-flexible) ElectLeadersResponse.  V1 includes an ErrorCode field but
// uses standard (non-compact) array lengths and string prefixes with no
// trailing tagged-field bytes.
var electLeadersResponseOneTopicV1 = []byte{
	0, 0, 3, 232, // ThrottleTimeMs 1000
	0, 0, // ErrorCode ErrNoError (present in V1+)
	0, 0, 0, 1, // 1 topic (non-compact array)
	0, 5, 116, 111, 112, 105, 99, // topic name "topic" (2-byte len + bytes)
	0, 0, 0, 1, // 1 partition
	0, 0, 0, 0, // partition 0
	0, 0, // PartitionResult.ErrorCode ErrNoError
	255, 255, // PartitionResult.ErrorMessage null (-1 encoded as uint16)
}

// electLeadersResponseOneTopicV0 is the expected wire encoding for a V0
// (non-flexible) ElectLeadersResponse.  V0 omits the top-level ErrorCode field.
var electLeadersResponseOneTopicV0 = []byte{
	0, 0, 3, 232, // ThrottleTimeMs 1000
	// No top-level ErrorCode in V0
	0, 0, 0, 1, // 1 topic (non-compact array)
	0, 5, 116, 111, 112, 105, 99, // topic name "topic" (2-byte len + bytes)
	0, 0, 0, 1, // 1 partition
	0, 0, 0, 0, // partition 0
	0, 0, // PartitionResult.ErrorCode ErrNoError
	255, 255, // PartitionResult.ErrorMessage null
}

func TestElectLeadersResponseV1(t *testing.T) {
	response := &ElectLeadersResponse{
		Version:        int16(1),
		ThrottleTimeMs: int32(1000),
		ReplicaElectionResults: map[string]map[int32]*PartitionResult{
			"topic": {
				0: {},
			},
		},
	}
	testResponse(t, "one topic V1", response, electLeadersResponseOneTopicV1)
}

func TestElectLeadersResponseV0(t *testing.T) {
	response := &ElectLeadersResponse{
		Version:        int16(0),
		ThrottleTimeMs: int32(1000),
		ReplicaElectionResults: map[string]map[int32]*PartitionResult{
			"topic": {
				0: {},
			},
		},
	}
	testResponse(t, "one topic V0", response, electLeadersResponseOneTopicV0)
}

// TestElectLeadersResponseRoundTrip verifies that all supported response
// versions survive an encode → decode round-trip with no data loss.
func TestElectLeadersResponseRoundTrip(t *testing.T) {
	errMsg := "some error"
	for _, version := range []int16{0, 1, 2} {
		response := &ElectLeadersResponse{
			Version:        version,
			ThrottleTimeMs: int32(500),
			ErrorCode:      ErrReassignmentInProgress,
			ReplicaElectionResults: map[string]map[int32]*PartitionResult{
				"my-topic": {
					0: {ErrorCode: ErrNoError},
					1: {ErrorCode: ErrNotLeaderForPartition, ErrorMessage: &errMsg},
				},
			},
		}
		// V0 has no top-level ErrorCode field; zero it so DeepEqual works.
		if version == 0 {
			response.ErrorCode = ErrNoError
		}
		testResponse(t, "round-trip", response, nil)
	}
}