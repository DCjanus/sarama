package sarama

import (
	"errors"
	"testing"
)

func TestRestrictApiVersionLowersVersionToBrokerMax(t *testing.T) {
	request := NewMetadataRequest(V2_8_0_0, []string{"test-topic"})

	if request.version() != 10 {
		t.Errorf("Expected MetadataRequest version to be 10, got %d", request.version())
	}

	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 0,
			maxVersion: 8,
		},
	}

	err := restrictApiVersion(request, brokerVersions)
	if err != nil {
		t.Errorf("restrictApiVersion returned unexpected error: %v", err)
	}

	if request.version() != 8 {
		t.Errorf("Expected version to be restricted to 8, got %d", request.version())
	}
}

func TestRestrictApiVersionLeavesVersionUnchangedWhenWithinRange(t *testing.T) {
	request := NewMetadataRequest(V2_4_0_0, []string{"test-topic"})
	originalVersion := request.version()

	if originalVersion != 9 {
		t.Errorf("Expected MetadataRequest version to be 9, got %d", originalVersion)
	}

	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 0,
			maxVersion: 10,
		},
	}

	err := restrictApiVersion(request, brokerVersions)
	if err != nil {
		t.Errorf("restrictApiVersion returned unexpected error: %v", err)
	}

	if request.version() != originalVersion {
		t.Errorf("Expected version to remain %d, got %d", originalVersion, request.version())
	}
}

func TestRestrictApiVersionDoesNotRaiseVersionBeyondUserSetMax(t *testing.T) {
	// the Kafka version comes from conf.Version, which is the user-set max Kafka API version to use
	request := NewMetadataRequest(V0_10_0_0, []string{"test-topic"})

	if request.version() != 1 {
		t.Errorf("Expected MetadataRequest version to be 1, got %d", request.version())
	}

	// broker doesn't support versions below 5
	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 5,
			maxVersion: 10,
		},
	}

	// we expect the user's preference to be respected even when it's below the broker's minimum
	err := restrictApiVersion(request, brokerVersions)
	if err != nil {
		t.Errorf("restrictApiVersion returned unexpected error: %v", err)
	}

	if request.version() != 1 {
		t.Errorf("Expected version to be set to minimum 1, got %d", request.version())
	}
}

func TestRestrictApiVersionDoesNothingIfBrokerVersionRangeMissing(t *testing.T) {
	request := NewMetadataRequest(V2_8_0_0, []string{"test-topic"})
	originalVersion := request.version()

	brokerVersions := apiVersionMap{
		// no entry for apiKeyMetadata
	}

	err := restrictApiVersion(request, brokerVersions)
	if err != nil {
		t.Errorf("restrictApiVersion returned unexpected error: %v", err)
	}

	if request.version() != originalVersion {
		t.Errorf("Expected version to remain %d, got %d", originalVersion, request.version())
	}
}

func TestAutoVersionUsesClientMaxBeforeBrokerClamp(t *testing.T) {
	request := NewMetadataRequest(AutoVersion, []string{"test-topic"})

	if request.version() != 10 {
		t.Errorf("Expected AutoVersion to select client max 10, got %d", request.version())
	}

	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 0,
			maxVersion: 8,
		},
	}

	if err := restrictApiVersion(request, brokerVersions); err != nil {
		t.Errorf("restrictApiVersion returned unexpected error: %v", err)
	}

	if request.version() != 8 {
		t.Errorf("Expected AutoVersion request to be restricted to 8, got %d", request.version())
	}
}

func TestAutoVersionRequiresBrokerApiVersions(t *testing.T) {
	config := NewConfig()
	config.Version = AutoVersion

	broker := &Broker{conf: config}
	request := NewMetadataRequest(AutoVersion, []string{"test-topic"})

	err := broker.sendInternal(request, nil)
	if err == nil {
		t.Fatal("Expected sendInternal to reject AutoVersion without broker ApiVersions")
	}
	if !errors.Is(err, ErrUnsupportedVersion) {
		t.Fatalf("Expected ErrUnsupportedVersion, got %v", err)
	}
}
