// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"hash"
	"math"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter/internal/objmodel"
	"github.com/open-telemetry/opentelemetry-collector-contrib/internal/coreinternal/traceutil"
)

type mappingModel interface {
	encodeLog(pcommon.Resource, plog.LogRecord, pcommon.InstrumentationScope) ([]byte, error)
	encodeMetrics(pcommon.Resource, pmetric.MetricSlice, pcommon.InstrumentationScope) ([][]byte, error)
	encodeSpan(pcommon.Resource, ptrace.Span, pcommon.InstrumentationScope) ([]byte, error)
}

// encodeModel tries to keep the event as close to the original open telemetry semantics as is.
// No fields will be mapped by default.
//
// Field deduplication and dedotting of attributes is supported by the encodeModel.
//
// See: https://github.com/open-telemetry/oteps/blob/master/text/logs/0097-log-data-model.md
type encodeModel struct {
	dedup   bool
	dedot   bool
	mapping string
}

const (
	traceIDField   = "traceID"
	spanIDField    = "spanID"
	attributeField = "attribute"
)

// Adds resource.attributes.* and instrumentation_scope.* to the document
// Only supports OTel mapping mode
func encodeResourceAndScopeAttributes(doc *objmodel.Document, resource pcommon.Resource, scope pcommon.InstrumentationScope) {
	m := pcommon.NewMap()

	r := m.PutEmptyMap("resource")
	r.PutInt("dropped_attributes_count", int64(resource.DroppedAttributesCount()))
	ra := r.PutEmptyMap("attributes")
	resource.Attributes().CopyTo(ra)

	if scope.Name() != "" || scope.Version() != "" || scope.Attributes().Len() > 0 {
		s := m.PutEmptyMap("instrumentation_scope")
		if scope.Name() != "" {
			s.PutStr("name", scope.Name())
		}
		if scope.Version() != "" {
			s.PutStr("version", scope.Version())
		}
		if scope.Attributes().Len() > 0 {
			sa := s.PutEmptyMap("attributes")
			scope.Attributes().CopyTo(sa)
			s.PutInt("dropped_attributes_count", int64(scope.DroppedAttributesCount()))
		}
	}

	doc.AddAttributes("", m)
}

// if droppedCount is negative (invalid), then the dropped_attributes_count field is not added
func encodeAttributes(doc *objmodel.Document, attr pcommon.Map, droppedCount int) {
	a := pcommon.NewMap()
	if droppedCount >= 0 {
		a.PutInt("dropped_attributes_count", int64(droppedCount))
	}
	aa := a.PutEmptyMap("attributes")
	attr.CopyTo(aa)
	doc.AddAttributes("", a)
}

func (m *encodeModel) encodeLog(resource pcommon.Resource, record plog.LogRecord, scope pcommon.InstrumentationScope) ([]byte, error) {
	var document objmodel.Document
	document.AddTimestamp("@timestamp", record.Timestamp()) // We use @timestamp in order to ensure that we can index if the default data stream logs template is used.

	if record.ObservedTimestamp() > 0 {
		document.AddTimestamp("observed_timestamp", record.ObservedTimestamp())
	}

	if m.mapping == MappingOTel.String() {
		encodeAttributes(&document, record.Attributes(), int(record.DroppedAttributesCount()))
		encodeResourceAndScopeAttributes(&document, resource, scope)

		document.AddTraceID("trace_id", record.TraceID())
		document.AddSpanID("span_id", record.SpanID())
		document.AddInt("trace_flags", int64(record.Flags()))
		document.AddString("log.level", record.SeverityText())
		document.AddInt("log.level_number", int64(record.SeverityNumber()))
		document.AddAttribute("message", record.Body())
	} else if m.mapping == MappingECS.String() {
		/* Add message first and overwrite it from the record if present */
		document.AddAttribute("message", record.Body())

		document.AddAttributes("", record.Attributes())
		document.AddAttributes("", resource.Attributes())

		document.AddAttribute("log.level", pcommon.NewValueStr(record.SeverityText()))
		document.AddAttribute("log.level_number", pcommon.NewValueInt(int64(record.SeverityNumber())))
	} else {
		document.AddTraceID("TraceId", record.TraceID())
		document.AddSpanID("SpanId", record.SpanID())
		document.AddInt("TraceFlags", int64(record.Flags()))
		document.AddString("SeverityText", record.SeverityText())
		document.AddInt("SeverityNumber", int64(record.SeverityNumber()))
		document.AddAttribute("Body", record.Body())
		document.AddAttributes("Attributes", record.Attributes())
		document.AddAttributes("Resource", resource.Attributes())
		document.AddAttributes("Scope", scopeToAttributes(scope))
	}

	if m.dedup {
		document.Dedup()
	} else if m.dedot && m.mapping != MappingOTel.String() {
		document.Sort()
	}

	var buf bytes.Buffer
	err := document.Serialize(&buf, m.mapping != MappingOTel.String() && m.dedot)
	return buf.Bytes(), err
}

func valueHash(h hash.Hash, v pcommon.Value) {
	switch v.Type() {
	case pcommon.ValueTypeEmpty:
		h.Write([]byte{0})
	case pcommon.ValueTypeStr:
		h.Write([]byte(v.Str()))
	case pcommon.ValueTypeBool:
		if v.Bool() {
			h.Write([]byte{1})
		} else {
			h.Write([]byte{0})
		}
	case pcommon.ValueTypeDouble:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[:], math.Float64bits(v.Double()))
		h.Write(buf)
	case pcommon.ValueTypeInt:
		buf := make([]byte, 8)
		binary.BigEndian.PutUint64(buf[:], uint64(v.Int()))
		h.Write(buf)
	case pcommon.ValueTypeBytes:
		h.Write(v.Bytes().AsRaw())
	case pcommon.ValueTypeMap:
		mapHash(h, v.Map())
	case pcommon.ValueTypeSlice:
		sliceHash(h, v.Slice())
	}
}

func sliceHash(h hash.Hash, s pcommon.Slice) {
	for i := 0; i < s.Len(); i++ {
		valueHash(h, s.At(i))
	}
}

func mapHash(h hash.Hash, m pcommon.Map) {
	m.Range(func(k string, v pcommon.Value) bool {
		h.Write([]byte(v.Str()))
		valueHash(h, v)

		return true
	})
}

func metricHash(h hash.Hash, t pcommon.Timestamp, attrs pcommon.Map) string {
	// Avoid any hashing if there are no attributes
	if attrs.Len() == 0 {
		return t.String()
	}

	h.Reset()

	h.Write([]byte(t.AsTime().Format(time.RFC3339Nano)))

	mapHash(h, attrs)

	return string(h.Sum(nil))
}

func (m *encodeModel) encodeMetrics(resource pcommon.Resource, metrics pmetric.MetricSlice, scope pcommon.InstrumentationScope) ([][]byte, error) {
	hasher := sha256.New()
	var baseDoc objmodel.Document

	if m.mapping == MappingOTel.String() {
		encodeResourceAndScopeAttributes(&baseDoc, resource, scope)
	} else {
		baseDoc.AddAttributes("", resource.Attributes())
	}

	docsByHash := map[string]*objmodel.Document{}
	mFinalizers := []func(){}
	mvalsByHash := map[string]*pcommon.Map{}

	for i := 0; i < metrics.Len(); i++ {
		mt := metrics.At(i)

		var dps pmetric.NumberDataPointSlice

		// TODO: support more metric types
		switch mt.Type() {
		case pmetric.MetricTypeGauge:
			dps = mt.Gauge().DataPoints()
		case pmetric.MetricTypeSum:
			dps = mt.Sum().DataPoints()
		}

		for j := 0; j < dps.Len(); j++ {
			dp := dps.At(j)

			// TODO: Instead of hashing to match metrics, it would be faster if we just assumed that each resource metrics
			// belong to the same document in ES internally in Agent from Beats' shipper output. Make this a configurable
			// `document_merging_mode` option - maybe also dynamic based on resource attributes, like data_stream.* fields
			// or OTTL expressions.
			hash := metricHash(hasher, dp.Timestamp(), dp.Attributes())
			doc, ok := docsByHash[hash]
			mVals := mvalsByHash[hash]
			if !ok {
				d := baseDoc.Clone()
				d.AddTimestamp("@timestamp", dp.Timestamp())
				if m.mapping == MappingOTel.String() {
					encodeAttributes(&d, dp.Attributes(), -1)
				} else {
					d.AddAttributes("", dp.Attributes())
				}

				if m.mapping == MappingOTel.String() {
					mm := pcommon.NewMap()
					mv := mm.PutEmptyMap("metrics")
					mFinalizers = append(mFinalizers, func() {
						d.AddAttributes("", mm)
					})
					mvalsByHash[hash] = &mv
					mVals = &mv
				}

				docsByHash[hash] = &d
				doc = &d
			}

			switch dp.ValueType() {
			case pmetric.NumberDataPointValueTypeDouble:
				if m.mapping == MappingOTel.String() {
					mVals.PutDouble(mt.Name(), dp.DoubleValue())
				} else {
					doc.AddAttribute(mt.Name(), pcommon.NewValueDouble(dp.DoubleValue()))
				}
			case pmetric.NumberDataPointValueTypeInt:
				if m.mapping == MappingOTel.String() {
					mVals.PutInt(mt.Name(), dp.IntValue())
				} else {
					doc.AddAttribute(mt.Name(), pcommon.NewValueInt(dp.IntValue()))
				}
			}
		}
	}

	for _, f := range mFinalizers {
		f()
	}

	res := make([][]byte, 0, len(docsByHash))

	for _, doc := range docsByHash {
		if m.dedup {
			doc.Dedup()
		} else if m.dedot && m.mapping != MappingOTel.String() {
			doc.Sort()
		}

		var buf bytes.Buffer
		err := doc.Serialize(&buf, m.mapping != MappingOTel.String() && m.dedot)

		if err != nil {
			fmt.Printf("Serialize error, dropping doc: %v\n", err)
		} else {
			res = append(res, buf.Bytes())
		}
	}

	return res, nil
}

func (m *encodeModel) encodeSpan(resource pcommon.Resource, span ptrace.Span, scope pcommon.InstrumentationScope) ([]byte, error) {
	var document objmodel.Document
	document.AddTimestamp("@timestamp", span.StartTimestamp()) // We use @timestamp in order to ensure that we can index if the default data stream logs template is used.

	if m.mapping == MappingOTel.String() {
		document.AddInt("duration", span.EndTimestamp().AsTime().Sub(span.StartTimestamp().AsTime()).Nanoseconds())
		document.AddString("kind", traceutil.SpanKindStr(span.Kind()))
		document.AddString("name", span.Name())
		document.AddString("status", span.Status().Message())
		document.AddSpanID("span_id", span.SpanID())
		document.AddTraceID("trace_id", span.TraceID())
		document.AddString("trace_state", span.TraceState().AsRaw())
		document.AddSpanID("parent_span_id", span.ParentSpanID())
		// TODO: span links
		// TODO: span events

		encodeResourceAndScopeAttributes(&document, resource, scope)
		encodeAttributes(&document, span.Attributes(), int(span.DroppedAttributesCount()))

	} else {
		document.AddTimestamp("EndTimestamp", span.EndTimestamp())
		document.AddTraceID("TraceId", span.TraceID())
		document.AddSpanID("SpanId", span.SpanID())
		document.AddSpanID("ParentSpanId", span.ParentSpanID())
		document.AddString("Name", span.Name())
		document.AddString("Kind", traceutil.SpanKindStr(span.Kind()))
		document.AddInt("TraceStatus", int64(span.Status().Code()))
		document.AddString("TraceStatusDescription", span.Status().Message())
		document.AddString("Link", spanLinksToString(span.Links()))
		document.AddAttributes("Attributes", span.Attributes())
		document.AddAttributes("Resource", resource.Attributes())
		document.AddEvents("Events", span.Events())
		document.AddInt("Duration", durationAsMicroseconds(span.StartTimestamp().AsTime(), span.EndTimestamp().AsTime())) // unit is microseconds
		document.AddAttributes("Scope", scopeToAttributes(scope))
	}

	if m.dedup {
		document.Dedup()
	} else if m.dedot && m.mapping != MappingOTel.String() {
		document.Sort()
	}

	var buf bytes.Buffer
	err := document.Serialize(&buf, m.mapping != MappingOTel.String() && m.dedot)
	return buf.Bytes(), err
}

func spanLinksToString(spanLinkSlice ptrace.SpanLinkSlice) string {
	linkArray := make([]map[string]any, 0, spanLinkSlice.Len())
	for i := 0; i < spanLinkSlice.Len(); i++ {
		spanLink := spanLinkSlice.At(i)
		link := map[string]any{}
		link[spanIDField] = traceutil.SpanIDToHexOrEmptyString(spanLink.SpanID())
		link[traceIDField] = traceutil.TraceIDToHexOrEmptyString(spanLink.TraceID())
		link[attributeField] = spanLink.Attributes().AsRaw()
		linkArray = append(linkArray, link)
	}
	linkArrayBytes, _ := json.Marshal(&linkArray)
	return string(linkArrayBytes)
}

// durationAsMicroseconds calculate span duration through end - start nanoseconds and converts time.Time to microseconds,
// which is the format the Duration field is stored in the Span.
func durationAsMicroseconds(start, end time.Time) int64 {
	return (end.UnixNano() - start.UnixNano()) / 1000
}

func scopeToAttributes(scope pcommon.InstrumentationScope) pcommon.Map {
	attrs := pcommon.NewMap()
	attrs.PutStr("name", scope.Name())
	attrs.PutStr("version", scope.Version())
	for k, v := range scope.Attributes().AsRaw() {
		attrs.PutStr(k, v.(string))
	}
	return attrs
}
