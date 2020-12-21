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
          "fieldName": "id",
          "name": "id",
          "type": "thetaSketch",
          "isInputThetaSketch": true,
          "size": 262144
        }
      ],
      "parser": {
        "parseSpec": {
          "columns": [
              "date",
              $FINAL_PROJECTIONS$
              "step",
              "prev_step",
              "funnel_name",
              "id"
            ],
            "dimensionsSpec" : {
                "dimensions": [$FINAL_PROJECTIONS$ "step", "prev_step", "funnel_name"]
              },
          "format": "tsv",
          "timestampSpec": {
            "column": "date",
            "format": "yyyyMMdd"
          }
        },
        "type": "hadoopyString"
      }
    },
    "ioConfig": {
      "inputSpec": {
        "paths": "$DB_OUTPUT_PATH$/$TABLE$_$INPUT_SPEC_GRANULARITY$/",
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
