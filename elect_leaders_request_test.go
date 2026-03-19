//go:build !functional

package sarama

import (
	"bytes"
	"testing"
)

var (
	electLeadersRequestOneTopicV1 = []byte{
		0,          // preferred election type
		0, 0, 0, 1, // 1 topic
		0, 5, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 0, // partition 0
		0, 0, 39, 16, // timeout 10000
	}
	electLeadersRequestOneTopicV2 = []byte{
		0,                         // preferred election type
		2,                         // 2-1=1 topic
		6, 116, 111, 112, 105, 99, // topic name "topic" as compact string
		2,          // 2-1=1 partition
		0, 0, 0, 0, // partition 0
		0,            // empty tagged fields
		0, 0, 39, 16, // timeout 10000
		0, // empty tagged fields
	}
)

func TestElectLeadersRequest(t *testing.T) {
	var request = &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(1),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	testRequest(t, "one topic V1", request, electLeadersRequestOneTopicV1)

	request.Version = 2
	testRequest(t, "one topic V2", request, electLeadersRequestOneTopicV2)
}

func TestElectLeadersRequestHeaderVersion(t *testing.T) {
	reqBody := &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(1),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	packet, err := encode(&request{
		correlationID: 123,
		clientID:      "foo",
		body:          reqBody,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	nonFlexibleHeaderSize := 14 + len("foo")
	if !bytes.Equal(packet[nonFlexibleHeaderSize:], electLeadersRequestOneTopicV1) {
		t.Fatalf("expected V1 body to start immediately after request header")
	}

	reqBody.Version = 2
	packet, err = encode(&request{
		correlationID: 123,
		clientID:      "foo",
		body:          reqBody,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	flexibleHeaderSize := nonFlexibleHeaderSize + 1
	if packet[nonFlexibleHeaderSize] != 0 {
		t.Fatalf("expected V2 flexible request header to include empty tagged fields, got byte %d", packet[nonFlexibleHeaderSize])
	}
	if !bytes.Equal(packet[flexibleHeaderSize:], electLeadersRequestOneTopicV2) {
		t.Fatalf("expected V2 body to start after flexible request header")
	}
}

func TestElectLeadersResponseHeaderVersion(t *testing.T) {
	response := &ElectLeadersResponse{Version: 1}
	if response.headerVersion() != 0 {
		t.Fatalf("expected V1 response header version 0, got %d", response.headerVersion())
	}

	response.Version = 2
	if response.headerVersion() != 1 {
		t.Fatalf("expected V2 response header version 1, got %d", response.headerVersion())
	}
}

// TestElectLeadersRequestHeaderVersionV0 is a regression test: before the fix
// ElectLeadersRequest.headerVersion() always returned 2 regardless of version,
// which would insert a flexible-header tagged-fields byte into the non-flexible
// V0 wire format and corrupt the stream.
func TestElectLeadersRequestHeaderVersionV0(t *testing.T) {
	req := &ElectLeadersRequest{Version: 0}
	if got := req.headerVersion(); got != 1 {
		t.Fatalf("V0 request: expected headerVersion 1 (non-flexible), got %d", got)
	}
}

// TestElectLeadersResponseHeaderVersionV0 is a regression test: before the fix
// ElectLeadersResponse.headerVersion() always returned 1 regardless of version,
// which caused an extra tagged-fields byte to be expected when decoding V0 responses.
func TestElectLeadersResponseHeaderVersionV0(t *testing.T) {
	resp := &ElectLeadersResponse{Version: 0}
	if got := resp.headerVersion(); got != 0 {
		t.Fatalf("V0 response: expected headerVersion 0 (non-flexible), got %d", got)
	}
}

// TestElectLeadersHeaderVersionBoundary verifies that version 2 is exactly the
// boundary at which both request and response switch to the flexible wire format.
func TestElectLeadersHeaderVersionBoundary(t *testing.T) {
	cases := []struct {
		version         int16
		wantReqHeader   int16
		wantRespHeader  int16
		wantReqFlexible bool
	}{
		{0, 1, 0, false},
		{1, 1, 0, false},
		{2, 2, 1, true},
	}
	for _, tc := range cases {
		req := &ElectLeadersRequest{Version: tc.version}
		resp := &ElectLeadersResponse{Version: tc.version}

		if got := req.headerVersion(); got != tc.wantReqHeader {
			t.Errorf("request V%d: headerVersion=%d, want %d", tc.version, got, tc.wantReqHeader)
		}
		if got := resp.headerVersion(); got != tc.wantRespHeader {
			t.Errorf("response V%d: headerVersion=%d, want %d", tc.version, got, tc.wantRespHeader)
		}
		if got := req.isFlexible(); got != tc.wantReqFlexible {
			t.Errorf("request V%d: isFlexible=%v, want %v", tc.version, got, tc.wantReqFlexible)
		}
		if got := resp.isFlexible(); got != tc.wantReqFlexible {
			t.Errorf("response V%d: isFlexible=%v, want %v", tc.version, got, tc.wantReqFlexible)
		}
	}
}

// TestElectLeadersRequestV0WireFormat verifies that a V0 request (which has no
// election-type byte and uses a non-flexible header) encodes correctly.
func TestElectLeadersRequestV0WireFormat(t *testing.T) {
	// V0 omits the election-type byte and uses non-compact (4-byte) array lengths
	// and 2-byte string length prefixes.
	electLeadersRequestOneTopicV0 := []byte{
		0, 0, 0, 1, // 1 topic (non-compact array length)
		0, 5, 116, 111, 112, 105, 99, // "topic" (2-byte len prefix + bytes)
		0, 0, 0, 1, // 1 partition
		0, 0, 0, 0, // partition 0
		0, 0, 39, 16, // timeout 10000
	}

	request := &ElectLeadersRequest{
		TimeoutMs: int32(10000),
		Version:   int16(0),
		TopicPartitions: map[string][]int32{
			"topic": {0},
		},
		Type: PreferredElection,
	}

	testRequest(t, "one topic V0", request, electLeadersRequestOneTopicV0)
}