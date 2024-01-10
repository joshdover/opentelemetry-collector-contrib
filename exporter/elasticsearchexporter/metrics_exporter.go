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

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
)

type elasticsearchMetricsExporter struct {
	logger *zap.Logger

	index            string
	dynamicIndex     bool
	dynamicIndexMode string
	mappingMode      string
	maxAttempts      int

	client      *esClientCurrent
	bulkIndexer esBulkIndexerCurrent
	model       mappingModel
}

func newMetricsExporter(logger *zap.Logger, cfg *Config) (*elasticsearchMetricsExporter, error) {
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

	model := &encodeModel{dedup: cfg.Mapping.Dedup, dedot: cfg.Mapping.Dedot, mapping: cfg.Mapping.Mode}

	indexStr := cfg.MetricsIndex
	if cfg.Index != "" {
		indexStr = cfg.Index
	}

	dynamicIndexMode := ""
	switch cfg.LogsDynamicIndex.Mode {
	case "":
		dynamicIndexMode = "prefix_suffix"
	case "data_stream":
		dynamicIndexMode = "data_stream"
	}

	esMetricsExp := &elasticsearchMetricsExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:            indexStr,
		dynamicIndex:     cfg.MetricsDynamicIndex.Enabled,
		dynamicIndexMode: dynamicIndexMode,
		mappingMode:      cfg.Mapping.Mode,
		maxAttempts:      maxAttempts,
		model:            model,
	}
	return esMetricsExp, nil
}

func (e *elasticsearchMetricsExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *elasticsearchMetricsExporter) pushMetricsData(ctx context.Context, ld pmetric.Metrics) error {
	var errs []error

	rls := ld.ResourceMetrics()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		ills := rl.ScopeMetrics()
		for j := 0; j < ills.Len(); j++ {
			scope := ills.At(j).Scope()
			metrics := ills.At(j).Metrics()

			if err := e.pushMetricRecord(ctx, resource, metrics, scope); err != nil {
				if cerr := ctx.Err(); cerr != nil {
					return cerr
				}

				errs = append(errs, err)
			}

		}
	}

	return errors.Join(errs...)
}

func (e *elasticsearchMetricsExporter) pushMetricRecord(ctx context.Context, resource pcommon.Resource, slice pmetric.MetricSlice, scope pcommon.InstrumentationScope) error {
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
				}

				fIndex = fmt.Sprintf("%s-%s-%s", "metrics", dsDataset, dsNamespace)
			} else {
				fIndex = fmt.Sprintf("%s-%s-%s", "metrics", dsDataset, dsNamespace)
			}
		} else {
			return fmt.Errorf("unknown dynamic index mode: %s", e.dynamicIndexMode)
		}
	}

	documents, err := e.model.encodeMetrics(resource, slice, scope)
	if err != nil {
		return fmt.Errorf("failed to encode metric event: %w", err)
	}

	for _, document := range documents {
		err := pushDocuments(ctx, e.logger, fIndex, document, e.bulkIndexer, e.maxAttempts)
		if err != nil {
			return err
		}
	}
	e.logger.Sugar().Infof("Successfully pushed %d metric documents to Elasticsearch queue.", len(documents))

	return nil
}
