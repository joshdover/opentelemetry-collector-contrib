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
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

type elasticsearchLogsExporter struct {
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

var retryOnStatus = []int{500, 502, 503, 504, 429}

const createAction = "create"

func newLogsExporter(logger *zap.Logger, cfg *Config) (*elasticsearchLogsExporter, error) {
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

	indexStr := cfg.LogsIndex
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

	esLogsExp := &elasticsearchLogsExporter{
		logger:      logger,
		client:      client,
		bulkIndexer: bulkIndexer,

		index:            indexStr,
		dynamicIndex:     cfg.LogsDynamicIndex.Enabled,
		dynamicIndexMode: dynamicIndexMode,
		mappingMode:      cfg.Mapping.Mode,
		maxAttempts:      maxAttempts,
		model:            model,
		logstashFormat:   cfg.LogstashFormat,
	}
	return esLogsExp, nil
}

func (e *elasticsearchLogsExporter) Shutdown(ctx context.Context) error {
	return e.bulkIndexer.Close(ctx)
}

func (e *elasticsearchLogsExporter) pushLogsData(ctx context.Context, ld plog.Logs) error {
	var errs []error

	rls := ld.ResourceLogs()
	for i := 0; i < rls.Len(); i++ {
		rl := rls.At(i)
		resource := rl.Resource()
		ills := rl.ScopeLogs()
		for j := 0; j < ills.Len(); j++ {
			scope := ills.At(j).Scope()
			logs := ills.At(j).LogRecords()
			for k := 0; k < logs.Len(); k++ {
				if err := e.pushLogRecord(ctx, resource, logs.At(k), scope); err != nil {
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

func (e *elasticsearchLogsExporter) pushLogRecord(ctx context.Context, resource pcommon.Resource, record plog.LogRecord, scope pcommon.InstrumentationScope) error {
	fIndex := e.index
	if e.dynamicIndex {
		if e.dynamicIndexMode == "prefix_suffix" {
			prefix := getStrFromAttributes(indexPrefix, "", resource.Attributes(), record.Attributes())
			suffix := getStrFromAttributes(indexSuffix, "", resource.Attributes(), record.Attributes())

			fIndex = fmt.Sprintf("%s%s%s", prefix, fIndex, suffix)
		} else if e.dynamicIndexMode == "data_stream" {
			dsDataset := getStrFromAttributes(dataStreamDataset, defaultDataStreamDataset, resource.Attributes(), record.Attributes())
			dsNamespace := getStrFromAttributes(dataStreamNamespace, defaultDataStreamNamespace, resource.Attributes(), record.Attributes())

			if e.mappingMode == "otel" {
				// Otel mapping mode requires otel.* prefix for dataset
				if !strings.HasPrefix(dsDataset, "otel.") {
					dsDataset = "otel." + dsDataset
					// Update resource to keep fields in sync with data stream name
					// currently assume that it was set on resource
					resource.Attributes().PutStr("data_stream.dataset", dsDataset)
				}

				fIndex = fmt.Sprintf("%s-%s-%s", "logs", dsDataset, dsNamespace)
			} else {
				fIndex = fmt.Sprintf("%s-%s-%s", "logs", dsDataset, dsNamespace)
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

	document, err := e.model.encodeLog(resource, record, scope)
	if err != nil {
		return fmt.Errorf("failed to encode log event: %w", err)
	}
	return pushDocuments(ctx, e.logger, fIndex, document, e.bulkIndexer, e.maxAttempts)
}
