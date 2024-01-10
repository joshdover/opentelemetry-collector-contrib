// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.18.0"
)

var expectedSpanBody = `{"@timestamp":"2023-04-19T03:04:05.000000006Z","Attributes.service.instance.id":"23","Duration":1000000,"EndTimestamp":"2023-04-19T03:04:06.000000006Z","Events.fooEvent.evnetMockBar":"bar","Events.fooEvent.evnetMockFoo":"foo","Events.fooEvent.time":"2023-04-19T03:04:05.000000006Z","Kind":"SPAN_KIND_CLIENT","Link":"[{\"attribute\":{},\"spanID\":\"\",\"traceID\":\"01020304050607080807060504030200\"}]","Name":"client span","Resource.cloud.platform":"aws_elastic_beanstalk","Resource.cloud.provider":"aws","Resource.deployment.environment":"BETA","Resource.service.instance.id":"23","Resource.service.name":"some-service","Resource.service.version":"env-version-1234","Scope.lib-foo":"lib-bar","Scope.name":"io.opentelemetry.rabbitmq-2.7","Scope.version":"1.30.0-alpha","SpanId":"1920212223242526","TraceId":"01020304050607080807060504030201","TraceStatus":2,"TraceStatusDescription":"Test"}`

func TestEncodeSpan(t *testing.T) {
	model := &encodeModel{dedup: true, dedot: false}
	td := mockResourceSpans()
	spanByte, err := model.encodeSpan(td.ResourceSpans().At(0).Resource(), td.ResourceSpans().At(0).ScopeSpans().At(0).Spans().At(0), td.ResourceSpans().At(0).ScopeSpans().At(0).Scope())
	assert.NoError(t, err)
	assert.Equal(t, expectedSpanBody, string(spanByte))
}

func mockResourceAttr() pcommon.Map {
	attr := pcommon.NewMap()
	attr.PutStr("cloud.provider", "aws")
	attr.PutStr("cloud.platform", "aws_elastic_beanstalk")
	attr.PutStr("deployment.environment", "BETA")
	attr.PutStr("service.instance.id", "23")
	attr.PutStr("service.version", "env-version-1234")
	attr.PutStr(semconv.AttributeServiceName, "some-service")
	return attr
}

func mockResourceSpans() ptrace.Traces {
	traces := ptrace.NewTraces()

	resourceSpans := traces.ResourceSpans().AppendEmpty()
	attr := resourceSpans.Resource().Attributes()
	mockResourceAttr().CopyTo(attr)
	resourceSpans.Resource().Attributes().PutStr(semconv.AttributeServiceName, "some-service")

	tStart := time.Date(2023, 4, 19, 3, 4, 5, 6, time.UTC)
	tEnd := time.Date(2023, 4, 19, 3, 4, 6, 6, time.UTC)

	scopeSpans := resourceSpans.ScopeSpans().AppendEmpty()
	scopeSpans.Scope().SetName("io.opentelemetry.rabbitmq-2.7")
	scopeSpans.Scope().SetVersion("1.30.0-alpha")
	scopeSpans.Scope().Attributes().PutStr("lib-foo", "lib-bar")

	span := scopeSpans.Spans().AppendEmpty()
	span.SetName("client span")
	span.SetSpanID([8]byte{0x19, 0x20, 0x21, 0x22, 0x23, 0x24, 0x25, 0x26})
	span.SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6, 5, 4, 3, 2, 1})
	span.SetKind(ptrace.SpanKindClient)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(tStart))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(tEnd))
	span.Status().SetCode(2)
	span.Status().SetMessage("Test")
	span.Attributes().PutStr("service.instance.id", "23")
	span.Links().AppendEmpty().SetTraceID([16]byte{1, 2, 3, 4, 5, 6, 7, 8, 8, 7, 6, 5, 4, 3, 2, 0})

	event := span.Events().AppendEmpty()
	event.SetName("fooEvent")
	event.SetTimestamp(pcommon.NewTimestampFromTime(tStart))
	event.Attributes().PutStr("evnetMockFoo", "foo")
	event.Attributes().PutStr("evnetMockBar", "bar")
	return traces
}

var (
	expMetricEcsDoc1 = `{"@timestamp":"2023-04-19T03:04:05.000000006Z","cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws","core":"0","state":"user","system.cpu.time":100,"system.cpu.utilization":0.1}`
	expMetricEcsDoc2 = `{"@timestamp":"2023-04-19T03:04:05.000000006Z","cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws","core":"1","state":"user","system.cpu.time":200,"system.cpu.utilization":0.2}`
	expMetricEcsDoc3 = `{"@timestamp":"2023-04-19T03:04:35.000000006Z","cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws","core":"0","state":"user","system.cpu.time":300,"system.cpu.utilization":0.3}`
)

func TestEncodeMetricEcs(t *testing.T) {
	model := &encodeModel{dedup: true, dedot: false}
	md := mockMetrics()
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	metricBytes, err := model.encodeMetrics(md.ResourceMetrics().At(0).Resource(), sm.Metrics(), sm.Scope())
	assert.NoError(t, err)

	assert.Equal(t, 3, len(metricBytes))

	// convert to strings to make strings easier to compare in case of failure
	mbStrs := make([]string, len(metricBytes))
	for i, mb := range metricBytes {
		mbStrs[i] = string(mb)
	}

	assert.ElementsMatch(t, []string{expMetricEcsDoc1, expMetricEcsDoc2, expMetricEcsDoc3}, mbStrs)
}

var (
	expMetricOtelDoc1 = `{"@timestamp":"2023-04-19T03:04:05.000000006Z","attributes":{"core":"0","state":"user"},"instrumentation_scope":{"dropped_attribute_count":0,"name":"","version":""},"metrics":{"system.cpu.time":100,"system.cpu.utilization":0.1},"resource":{"attributes":{"cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws"},"dropped_attribute_count":0}}`
	expMetricOtelDoc2 = `{"@timestamp":"2023-04-19T03:04:05.000000006Z","attributes":{"core":"1","state":"user"},"instrumentation_scope":{"dropped_attribute_count":0,"name":"","version":""},"metrics":{"system.cpu.time":200,"system.cpu.utilization":0.2},"resource":{"attributes":{"cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws"},"dropped_attribute_count":0}}`
	expMetricOtelDoc3 = `{"@timestamp":"2023-04-19T03:04:35.000000006Z","attributes":{"core":"0","state":"user"},"instrumentation_scope":{"dropped_attribute_count":0,"name":"","version":""},"metrics":{"system.cpu.time":300,"system.cpu.utilization":0.3},"resource":{"attributes":{"cloud.platform":"aws_elastic_beanstalk","cloud.provider":"aws"},"dropped_attribute_count":0}}`
)

func TestEncodeMetricOtel(t *testing.T) {
	model := &encodeModel{dedup: true, dedot: false, mapping: "otel"}
	md := mockMetrics()
	sm := md.ResourceMetrics().At(0).ScopeMetrics().At(0)
	metricBytes, err := model.encodeMetrics(md.ResourceMetrics().At(0).Resource(), sm.Metrics(), sm.Scope())
	assert.NoError(t, err)

	assert.Equal(t, 3, len(metricBytes))

	// convert to strings to make strings easier to compare in case of failure
	mbStrs := make([]string, len(metricBytes))
	for i, mb := range metricBytes {
		mbStrs[i] = string(mb)
	}

	assert.ElementsMatch(t, []string{expMetricOtelDoc1, expMetricOtelDoc2, expMetricOtelDoc3}, mbStrs)
}

func mockMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()

	resourceMetrics := metrics.ResourceMetrics().AppendEmpty()
	attr := resourceMetrics.Resource().Attributes()
	// mockResourceAttr().CopyTo(attr)
	attr.PutStr("cloud.provider", "aws")
	attr.PutStr("cloud.platform", "aws_elastic_beanstalk")

	scopeMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()

	t := time.Date(2023, 4, 19, 3, 4, 5, 6, time.UTC)

	m1 := scopeMetrics.Metrics().AppendEmpty()
	m1.SetName("system.cpu.utilization")
	g1 := m1.SetEmptyGauge()

	dp1 := g1.DataPoints().AppendEmpty()
	dp1.SetTimestamp(pcommon.NewTimestampFromTime(t))
	dp1.SetDoubleValue(0.1)
	dp1.Attributes().PutStr("state", "user")
	dp1.Attributes().PutStr("core", "0")

	// Same state + diff core, same time
	dp2 := g1.DataPoints().AppendEmpty()
	dp2.SetTimestamp(pcommon.NewTimestampFromTime(t))
	dp2.SetDoubleValue(0.2)
	dp2.Attributes().PutStr("state", "user")
	dp2.Attributes().PutStr("core", "1")

	// Same state + core, 30s later time
	dp3 := g1.DataPoints().AppendEmpty()
	dp3.SetTimestamp(pcommon.NewTimestampFromTime(t.Add(30 * time.Second)))
	dp3.SetDoubleValue(0.3)
	dp3.Attributes().PutStr("state", "user")
	dp3.Attributes().PutStr("core", "0")

	// Diff metric, same times / label combinations
	m2 := scopeMetrics.Metrics().AppendEmpty()
	m2.SetName("system.cpu.time")
	g2 := m2.SetEmptySum()

	dp4 := g2.DataPoints().AppendEmpty()
	dp4.SetTimestamp(pcommon.NewTimestampFromTime(t))
	dp4.SetIntValue(100)
	dp4.Attributes().PutStr("state", "user")
	dp4.Attributes().PutStr("core", "0")

	// Same state + diff core, same time
	dp5 := g2.DataPoints().AppendEmpty()
	dp5.SetTimestamp(pcommon.NewTimestampFromTime(t))
	dp5.SetIntValue(200)
	dp5.Attributes().PutStr("state", "user")
	dp5.Attributes().PutStr("core", "1")

	// Same state + core, 30s later time
	dp6 := g2.DataPoints().AppendEmpty()
	dp6.SetTimestamp(pcommon.NewTimestampFromTime(t))
	dp6.SetTimestamp(pcommon.NewTimestampFromTime(t.Add(30 * time.Second)))
	dp6.SetIntValue(300)
	dp6.Attributes().PutStr("state", "user")
	dp6.Attributes().PutStr("core", "0")

	return metrics
}
