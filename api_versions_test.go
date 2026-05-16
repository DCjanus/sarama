package sarama

import "testing"

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

func TestNegotiateApiVersionSelectsLatestUsableVersion(t *testing.T) {
	request := NewMetadataRequest(MaxVersion, []string{"test-topic"})

	if request.version() != 10 {
		t.Errorf("Expected MetadataRequest version to start at client max 10, got %d", request.version())
	}

	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 0,
			maxVersion: 8,
		},
	}

	if err := negotiateApiVersion(request, brokerVersions); err != nil {
		t.Fatalf("negotiateApiVersion returned unexpected error: %v", err)
	}

	if request.version() != 8 {
		t.Errorf("Expected negotiated version 8, got %d", request.version())
	}
}

func TestSupportedApiVersionRangeRestoresOriginalVersion(t *testing.T) {
	request := NewOffsetRequest(V0_10_1_0)
	originalVersion := request.version()

	minVersion, maxVersion, ok := supportedApiVersionRange(request)
	if !ok {
		t.Fatal("Expected OffsetRequest to have supported versions")
	}
	if minVersion != 0 || maxVersion != 5 {
		t.Fatalf("Expected OffsetRequest range [0,5], got [%d,%d]", minVersion, maxVersion)
	}
	if request.version() != originalVersion {
		t.Fatalf("Expected request version to be restored to %d, got %d", originalVersion, request.version())
	}
}

func TestNegotiateApiVersionRejectsMissingBrokerRange(t *testing.T) {
	request := NewMetadataRequest(MaxVersion, []string{"test-topic"})

	if err := negotiateApiVersion(request, apiVersionMap{}); err == nil {
		t.Fatal("Expected negotiateApiVersion to reject a missing broker range")
	}
}

func TestNegotiateApiVersionRejectsNoOverlap(t *testing.T) {
	request := NewMetadataRequest(MaxVersion, []string{"test-topic"})

	brokerVersions := apiVersionMap{
		apiKeyMetadata: &apiVersionRange{
			minVersion: 11,
			maxVersion: 12,
		},
	}

	if err := negotiateApiVersion(request, brokerVersions); err == nil {
		t.Fatal("Expected negotiateApiVersion to reject non-overlapping ranges")
	}
}

func TestExperimentalVersionForRequestUsesClientMaxForMigratedAPIs(t *testing.T) {
	config := NewConfig()
	config.Experimental.AutoVersionNegotiation = true

	metadataRequest := NewMetadataRequest(config.versionForRequest(apiKeyMetadata), []string{"test-topic"})
	if metadataRequest.version() != 10 {
		t.Errorf("Expected MetadataRequest to use client max 10, got %d", metadataRequest.version())
	}

	offsetRequest := NewOffsetRequest(config.versionForRequest(apiKeyListOffsets))
	if offsetRequest.version() != 5 {
		t.Errorf("Expected OffsetRequest to use client max 5, got %d", offsetRequest.version())
	}
}

func TestExperimentalVersionForRequestLeavesUnmigratedAPIsOnConfiguredVersion(t *testing.T) {
	config := NewConfig()
	config.Version = V0_11_0_0
	config.Experimental.AutoVersionNegotiation = true

	if got := config.versionForRequest(apiKeyProduce); got != config.Version {
		t.Errorf("Expected unmigrated API to use configured version %s, got %s", config.Version, got)
	}
}
