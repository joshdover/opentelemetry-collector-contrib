// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

// Package elasticsearchexporter contains an opentelemetry-collector exporter
// for Elasticsearch.
package elasticsearchexporter // import "github.com/open-telemetry/opentelemetry-collector-contrib/exporter/elasticsearchexporter"

import "go.opentelemetry.io/collector/pdata/pcommon"

// dynamic index attribute key constants
const (
	indexPrefix         = "elasticsearch.index.prefix"
	indexSuffix         = "elasticsearch.index.suffix"
	dataStreamDataset   = "data_stream.dataset"
	dataStreamNamespace = "data_stream.namespace"

	defaultDataStreamNamespace = "default"
	defaultDataStreamDataset   = "generic"
)

// Retreive an attribute from resource or logrecord, span, or metric giving precdence to first item and falling back to
// second or later items if necessary
func getStrFromAttributes(name string, defaultVal string, attrMaps ...pcommon.Map) string {
	var str string = defaultVal

	for _, attrMap := range attrMaps {
		val, exist := attrMap.Get(name)
		if exist {
			str = val.AsString()
			break
		}
	}

	return str
}
