package sarama

import (
	"context"
	"sync"

	gometrics "github.com/rcrowley/go-metrics"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelmetric "go.opentelemetry.io/otel/metric"
)

type otelMetrics struct {
	meter          otelmetric.Meter
	addOptions     []otelmetric.AddOption
	recordOptions  []otelmetric.RecordOption
	counters       map[string]otelmetric.Int64Counter
	upDownCounters map[string]otelmetric.Int64UpDownCounter
	histograms     map[string]otelmetric.Int64Histogram
	mutex          sync.Mutex
}

func newOTelMetrics(cfg OpenTelemetryMetricsConfig) *otelMetrics {
	if !cfg.Enabled {
		return nil
	}

	meter := cfg.Meter
	if meter == nil {
		provider := cfg.MeterProvider
		if provider == nil {
			provider = otel.GetMeterProvider()
		}
		name := cfg.MeterName
		if name == "" {
			name = "sarama"
		}
		var opts []otelmetric.MeterOption
		if cfg.MeterVersion != "" {
			opts = append(opts, otelmetric.WithInstrumentationVersion(cfg.MeterVersion))
		}
		meter = provider.Meter(name, opts...)
	}

	var addOptions []otelmetric.AddOption
	var recordOptions []otelmetric.RecordOption
	if len(cfg.Attributes) > 0 {
		attrSet := attribute.NewSet(cfg.Attributes...)
		attrOpt := otelmetric.WithAttributeSet(attrSet)
		addOptions = []otelmetric.AddOption{attrOpt}
		recordOptions = []otelmetric.RecordOption{attrOpt}
	}

	return &otelMetrics{
		meter:          meter,
		addOptions:     addOptions,
		recordOptions:  recordOptions,
		counters:       map[string]otelmetric.Int64Counter{},
		upDownCounters: map[string]otelmetric.Int64UpDownCounter{},
		histograms:     map[string]otelmetric.Int64Histogram{},
	}
}

func (m *otelMetrics) addCounter(name string, value int64) {
	if m == nil || value == 0 {
		return
	}
	counter := m.counter(name)
	if counter == nil {
		return
	}
	counter.Add(context.Background(), value, m.addOptions...)
}

func (m *otelMetrics) addUpDownCounter(name string, value int64) {
	if m == nil || value == 0 {
		return
	}
	counter := m.upDownCounter(name)
	if counter == nil {
		return
	}
	counter.Add(context.Background(), value, m.addOptions...)
}

func (m *otelMetrics) recordHistogram(name string, value int64) {
	if m == nil {
		return
	}
	histogram := m.histogram(name)
	if histogram == nil {
		return
	}
	histogram.Record(context.Background(), value, m.recordOptions...)
}

func (m *otelMetrics) counter(name string) otelmetric.Int64Counter {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if counter, ok := m.counters[name]; ok {
		return counter
	}
	counter, err := m.meter.Int64Counter(name)
	if err != nil {
		return nil
	}
	m.counters[name] = counter
	return counter
}

func (m *otelMetrics) upDownCounter(name string) otelmetric.Int64UpDownCounter {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if counter, ok := m.upDownCounters[name]; ok {
		return counter
	}
	counter, err := m.meter.Int64UpDownCounter(name)
	if err != nil {
		return nil
	}
	m.upDownCounters[name] = counter
	return counter
}

func (m *otelMetrics) histogram(name string) otelmetric.Int64Histogram {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	if histogram, ok := m.histograms[name]; ok {
		return histogram
	}
	histogram, err := m.meter.Int64Histogram(name)
	if err != nil {
		return nil
	}
	m.histograms[name] = histogram
	return histogram
}

type otelRegistry struct {
	parent   gometrics.Registry
	recorder *otelMetrics
	mutex    sync.Mutex
	wrapped  map[string]interface{}
}

func newOTelRegistry(parent gometrics.Registry, recorder *otelMetrics) gometrics.Registry {
	if parent == nil {
		parent = gometrics.DefaultRegistry
	}
	return &otelRegistry{
		parent:   parent,
		recorder: recorder,
		wrapped:  map[string]interface{}{},
	}
}

func (r *otelRegistry) Each(fn func(string, interface{})) {
	r.parent.Each(fn)
}

func (r *otelRegistry) Get(name string) interface{} {
	r.mutex.Lock()
	wrapped, ok := r.wrapped[name]
	r.mutex.Unlock()
	if ok {
		return wrapped
	}
	return r.parent.Get(name)
}

func (r *otelRegistry) GetOrRegister(name string, metric interface{}) interface{} {
	r.mutex.Lock()
	wrapped, ok := r.wrapped[name]
	r.mutex.Unlock()
	if ok {
		return wrapped
	}

	existing := r.parent.Get(name)
	if existing == nil {
		existing = r.parent.GetOrRegister(name, metric)
	}

	wrapped = r.wrapMetric(name, existing)
	r.mutex.Lock()
	r.wrapped[name] = wrapped
	r.mutex.Unlock()

	return wrapped
}

func (r *otelRegistry) Register(name string, metric interface{}) error {
	if err := r.parent.Register(name, metric); err != nil {
		return err
	}
	r.mutex.Lock()
	r.wrapped[name] = r.wrapMetric(name, metric)
	r.mutex.Unlock()
	return nil
}

func (r *otelRegistry) RunHealthchecks() {
	r.parent.RunHealthchecks()
}

func (r *otelRegistry) GetAll() map[string]map[string]interface{} {
	return r.parent.GetAll()
}

func (r *otelRegistry) Unregister(name string) {
	r.parent.Unregister(name)
	r.mutex.Lock()
	delete(r.wrapped, name)
	r.mutex.Unlock()
}

func (r *otelRegistry) UnregisterAll() {
	r.parent.UnregisterAll()
	r.mutex.Lock()
	r.wrapped = map[string]interface{}{}
	r.mutex.Unlock()
}

func (r *otelRegistry) wrapMetric(name string, metric interface{}) interface{} {
	switch typed := metric.(type) {
	case *otelMeter, *otelHistogram, *otelCounter:
		return typed
	case gometrics.Meter:
		if r.recorder == nil {
			return typed
		}
		return &otelMeter{name: name, meter: typed, recorder: r.recorder}
	case gometrics.Histogram:
		if r.recorder == nil {
			return typed
		}
		return &otelHistogram{name: name, histogram: typed, recorder: r.recorder}
	case gometrics.Counter:
		if r.recorder == nil {
			return typed
		}
		return &otelCounter{name: name, counter: typed, recorder: r.recorder}
	default:
		return metric
	}
}

type otelMeter struct {
	name     string
	meter    gometrics.Meter
	recorder *otelMetrics
}

func (m *otelMeter) Count() int64 {
	return m.meter.Count()
}

func (m *otelMeter) Mark(n int64) {
	m.meter.Mark(n)
	if m.recorder != nil {
		m.recorder.addCounter(m.name, n)
	}
}

func (m *otelMeter) Rate1() float64 {
	return m.meter.Rate1()
}

func (m *otelMeter) Rate5() float64 {
	return m.meter.Rate5()
}

func (m *otelMeter) Rate15() float64 {
	return m.meter.Rate15()
}

func (m *otelMeter) RateMean() float64 {
	return m.meter.RateMean()
}

func (m *otelMeter) Snapshot() gometrics.Meter {
	return m.meter.Snapshot()
}

func (m *otelMeter) Stop() {
	m.meter.Stop()
}

type otelHistogram struct {
	name      string
	histogram gometrics.Histogram
	recorder  *otelMetrics
}

func (h *otelHistogram) Clear() {
	h.histogram.Clear()
}

func (h *otelHistogram) Count() int64 {
	return h.histogram.Count()
}

func (h *otelHistogram) Max() int64 {
	return h.histogram.Max()
}

func (h *otelHistogram) Mean() float64 {
	return h.histogram.Mean()
}

func (h *otelHistogram) Min() int64 {
	return h.histogram.Min()
}

func (h *otelHistogram) Percentile(p float64) float64 {
	return h.histogram.Percentile(p)
}

func (h *otelHistogram) Percentiles(ps []float64) []float64 {
	return h.histogram.Percentiles(ps)
}

func (h *otelHistogram) Snapshot() gometrics.Histogram {
	return h.histogram.Snapshot()
}

func (h *otelHistogram) StdDev() float64 {
	return h.histogram.StdDev()
}

func (h *otelHistogram) Sum() int64 {
	return h.histogram.Sum()
}

func (h *otelHistogram) Update(value int64) {
	h.histogram.Update(value)
	if h.recorder != nil {
		h.recorder.recordHistogram(h.name, value)
	}
}

func (h *otelHistogram) Variance() float64 {
	return h.histogram.Variance()
}

type otelCounter struct {
	name     string
	counter  gometrics.Counter
	recorder *otelMetrics
}

func (c *otelCounter) Clear() {
	c.counter.Clear()
}

func (c *otelCounter) Count() int64 {
	return c.counter.Count()
}

func (c *otelCounter) Dec(i int64) {
	c.counter.Dec(i)
	if c.recorder != nil {
		c.recorder.addUpDownCounter(c.name, -i)
	}
}

func (c *otelCounter) Inc(i int64) {
	c.counter.Inc(i)
	if c.recorder != nil {
		c.recorder.addUpDownCounter(c.name, i)
	}
}

func (c *otelCounter) Snapshot() gometrics.Counter {
	return c.counter.Snapshot()
}
