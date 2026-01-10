//go:build functional

package sarama

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

type otelMetricsGroupHandler struct {
	done chan struct{}
	once sync.Once
}

func (h *otelMetricsGroupHandler) Setup(_ ConsumerGroupSession) error   { return nil }
func (h *otelMetricsGroupHandler) Cleanup(_ ConsumerGroupSession) error { return nil }
func (h *otelMetricsGroupHandler) ConsumeClaim(sess ConsumerGroupSession, claim ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		sess.MarkMessage(msg, "")
		h.once.Do(func() { close(h.done) })
		return nil
	}
	return nil
}

func newOTelTestProvider(t *testing.T) (*sdkmetric.ManualReader, *sdkmetric.MeterProvider) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	t.Cleanup(func() {
		_ = provider.Shutdown(context.Background())
	})
	return reader, provider
}

func collectMetricNames(t *testing.T, reader *sdkmetric.ManualReader) map[string]bool {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	names := map[string]bool{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if metricHasData(m) {
				names[m.Name] = true
			}
		}
	}
	return names
}

func metricHasData(metric metricdata.Metrics) bool {
	switch data := metric.Data.(type) {
	case metricdata.Sum[int64]:
		return len(data.DataPoints) > 0
	case metricdata.Sum[float64]:
		return len(data.DataPoints) > 0
	case metricdata.Gauge[int64]:
		return len(data.DataPoints) > 0
	case metricdata.Gauge[float64]:
		return len(data.DataPoints) > 0
	case metricdata.Histogram[int64]:
		return len(data.DataPoints) > 0
	case metricdata.Histogram[float64]:
		return len(data.DataPoints) > 0
	case metricdata.ExponentialHistogram[int64]:
		return len(data.DataPoints) > 0
	case metricdata.ExponentialHistogram[float64]:
		return len(data.DataPoints) > 0
	case metricdata.Summary:
		return len(data.DataPoints) > 0
	default:
		return false
	}
}

func TestFuncOpenTelemetryMetricsEnabled(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	reader, provider := newOTelTestProvider(t)
	config := NewFunctionalTestConfig()
	config.Metrics.OpenTelemetry.Enabled = true
	config.Metrics.OpenTelemetry.MeterProvider = provider
	config.Consumer.Offsets.Initial = OffsetOldest

	broker := NewBroker(FunctionalTestEnv.KafkaBrokerAddrs[0])
	require.NoError(t, broker.Open(config))
	defer safeClose(t, broker)

	_, err := broker.GetMetadata(&MetadataRequest{Topics: []string{}})
	require.NoError(t, err)

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, producer)

	consumer, err := NewConsumer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, consumer)

	partitionConsumer, err := consumer.ConsumePartition("test.1", 0, OffsetNewest)
	require.NoError(t, err)
	defer safeClose(t, partitionConsumer)

	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("otel-metrics")})
	require.NoError(t, err)

	select {
	case <-partitionConsumer.Messages():
	case <-time.After(20 * time.Second):
		t.Fatal("timeout waiting for partition consumer message")
	}

	groupID := testFuncConsumerGroupID(t)
	group, err := NewConsumerGroup(FunctionalTestEnv.KafkaBrokerAddrs, groupID, config)
	require.NoError(t, err)
	defer safeClose(t, group)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	handler := &otelMetricsGroupHandler{done: make(chan struct{})}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-group.Errors():
			}
		}
	}()

	go func() {
		_ = group.Consume(ctx, []string{"test.1"}, handler)
	}()

	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("otel-metrics-group")})
	require.NoError(t, err)

	select {
	case <-handler.done:
		cancel()
	case <-ctx.Done():
		t.Fatal("timeout waiting for consumer group message")
	}

	names := collectMetricNames(t, reader)
	require.Contains(t, names, "request-rate")
	require.Contains(t, names, "record-send-rate")
	require.Contains(t, names, "consumer-fetch-rate")
	require.Contains(t, names, fmt.Sprintf("consumer-group-join-total-%s", groupID))

	require.NotNil(t, config.MetricRegistry.Get("request-rate"))
	require.NotNil(t, config.MetricRegistry.Get("record-send-rate"))
}

func TestFuncOpenTelemetryMetricsDisabled(t *testing.T) {
	setupFunctionalTest(t)
	defer teardownFunctionalTest(t)

	reader, provider := newOTelTestProvider(t)
	config := NewFunctionalTestConfig()
	config.Metrics.OpenTelemetry.MeterProvider = provider

	broker := NewBroker(FunctionalTestEnv.KafkaBrokerAddrs[0])
	require.NoError(t, broker.Open(config))
	defer safeClose(t, broker)

	_, err := broker.GetMetadata(&MetadataRequest{Topics: []string{}})
	require.NoError(t, err)

	producer, err := NewSyncProducer(FunctionalTestEnv.KafkaBrokerAddrs, config)
	require.NoError(t, err)
	defer safeClose(t, producer)

	_, _, err = producer.SendMessage(&ProducerMessage{Topic: "test.1", Value: StringEncoder("otel-metrics-disabled")})
	require.NoError(t, err)

	names := collectMetricNames(t, reader)
	require.Empty(t, names)
}
