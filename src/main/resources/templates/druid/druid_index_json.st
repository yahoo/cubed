{
    "spec": {
        "dataSchema": {
            "dataSource": "$PRODUCT_NAME$",
            "granularitySpec": {
                "intervals": [
                    "$GRANULARITY_INTERVAL$"
                ],
                "segmentGranularity": "$SEGMENT_GRANULARITY$",
                "queryGranularity": "$SEGMENT_GRANULARITY$"
            },
            "metricsSpec": [
                {
                    "type": "longSum",
                     "name": "record_count",
                     "fieldName": "record_count"
                }
                $metrics$
            ],
            "parser": {
                "parseSpec": {
                    "columns": [
                        "hour_unix_timestamp",
                        "record_count",
                        $columns$
                    ],
                    "dimensionsSpec": {
                        "dimensions": [
                            $dimensions$
                        ]
                    },
                    "format": "tsv",
                    "timestampSpec": {
                        "column": "hour_unix_timestamp",
                        "format": "millis"
                    }
                },
                "type": "string"
            }
        },
        "ioConfig": {
            "inputSpec": {
                "paths": "$DB_OUTPUT_PATH$/$TABLE$_$INPUT_SPEC_GRANULARITY$",
                "type": "static"
            },
            "type": "hadoop"
        },
        "tuningConfig": {
            "type": "hadoop",
            "ignoreInvalidRows": true,
            "partitionsSpec": {
                "type": "hashed",
                "targetPartitionSize": 1000000
            }
        }
    },
    "type": "index_hadoop"
}
