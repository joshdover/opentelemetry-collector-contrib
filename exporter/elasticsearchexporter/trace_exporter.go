// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package elasticsearchexporter contains an opentelemetry-collector exporter
// for Elasticsearch.
package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.uber.org/zap"
)

type elasticsearchTracesExporter struct {
	logger *zap.Logger

	index            string
	logstashFormat   LogstashFormatSettings
	dynamicIndex     bool
	dynamicIndexMode string
	mappingMode      string
	maxAttempts      int

	client      *esClientCurrent
	bulkIndexer esBulkIndexerCurrent
	model       mappingModel
}

func newTracesExporter(logger *zap.Logger, cfg *Config) (*elasticsearchTracesExporter, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	client, err := newElasticsearchClient(logger, cfg)
	if err != nil {
		return nil, err
	}

	bulkIndexer, err := newBulkIndexer(logger, client, cfg)
	if err != nil {
		return nil, err
	}

	maxAttempts := 1
	if cfg.Retry.Enabled {
		maxAttempts = cfg.Retry.MaxRequests
	}

	model := &encodeModel{dedup: cfg.Mapping.Dedup, dedot: cfg.Mapping.Dedot}

	dynamicIndexMode := ""
	switch cfg.LogsDynamicIndex.Mode {
	case "":
		dynamicIndexMode = "prefix_suffix"
	case "data_stream":
		dynamicIndexMode = "data_stream"
	}

	return &elasticsearchTracesExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:            cfg.TracesIndex,
		dynamicIndex:     cfg.TracesDynamicIndex.Enabled,
		dynamicIndexMode: dynamicIndexMode,
		mappingMode:      cfg.Mapping.Mode,
		maxAttempts:      maxAttempts,
		model:            model,
		logstashFormat:   cfg.LogstashFormat,
	}, nil
}

func (e *elasticsearchTracesExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *elasticsearchTracesExporter) pushTraceData(
	ctx context.Context,
	td ptrace.Traces,
) error {
	var errs []error
	resourceSpans := td.ResourceSpans()
	for i := 0; i < resourceSpans.Len(); i++ {
		il := resourceSpans.At(i)
		resource := il.Resource()
		scopeSpans := il.ScopeSpans()
		for j := 0; j < scopeSpans.Len(); j++ {
			scope := scopeSpans.At(j).Scope()
			spans := scopeSpans.At(j).Spans()
			for k := 0; k < spans.Len(); k++ {
				if err := e.pushTraceRecord(ctx, resource, spans.At(k), scope); err != nil {
					if cerr := ctx.Err(); cerr != nil {
						return cerr
					}
					errs = append(errs, err)
				}
			}
		}
	}

	return errors.Join(errs...)
}

func (e *elasticsearchTracesExporter) pushTraceRecord(ctx context.Context, resource pcommon.Resource, span ptrace.Span, scope pcommon.InstrumentationScope) error {
	fIndex := e.index
	if e.dynamicIndex {
		if e.dynamicIndexMode == "prefix_suffix" {
			prefix := getStrFromAttributes(indexPrefix, "", resource.Attributes())
			suffix := getStrFromAttributes(indexSuffix, "", resource.Attributes())

			fIndex = fmt.Sprintf("%s%s%s", prefix, fIndex, suffix)
		} else if e.dynamicIndexMode == "data_stream" {
			dsDataset := getStrFromAttributes(dataStreamDataset, defaultDataStreamDataset, resource.Attributes())
			dsNamespace := getStrFromAttributes(dataStreamNamespace, defaultDataStreamNamespace, resource.Attributes())

			if e.mappingMode == "otel" {
				// Otel mapping mode requires otel.* prefix for dataset
				if !strings.HasPrefix(dsDataset, "otel.") {
					dsDataset = "otel." + dsDataset
					// Update resource to keep fields in sync with data stream name
					// currently assume that it was set on resource
					resource.Attributes().PutStr("data_stream.dataset", dsDataset)
				}

				fIndex = fmt.Sprintf("%s-%s-%s", "traces", dsDataset, dsNamespace)
			} else {
				fIndex = fmt.Sprintf("%s-%s-%s", "traces", dsDataset, dsNamespace)
			}
		} else {
			return fmt.Errorf("unknown dynamic index mode: %s", e.dynamicIndexMode)
		}
	}

	if e.logstashFormat.Enabled {
		formattedIndex, err := generateIndexWithLogstashFormat(fIndex, &e.logstashFormat, time.Now())
		if err != nil {
			return err
		}
		fIndex = formattedIndex
	}

	document, err := e.model.encodeSpan(resource, span, scope)
	if err != nil {
		return fmt.Errorf("failed to encode trace record: %w", err)
	}
	return pushDocuments(ctx, e.logger, fIndex, document, e.bulkIndexer, e.maxAttempts)
}
