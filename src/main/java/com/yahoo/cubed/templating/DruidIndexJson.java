/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.util.Aggregation;
import com.yahoo.cubed.util.Constants;
import com.yahoo.cubed.util.Measurement;
import com.yahoo.cubed.util.XmlJsonUtils;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.stringtemplate.StringTemplate;

/**
 * Druid JSON index script generator.
 */
public class DruidIndexJson implements TemplateFile<Pipeline> {
    // Attributes
    private static final String COLUMNS_ATTRIBUTE = "columns";
    private static final String COLUMN_ATTRIBUTE = "column";
    private static final String COLUMN_TYPE_ATTRIBUTE = "column_type";
    private static final String METRICS_ATTRIBUTE = "metrics";
    private static final String DIMENSIONS_ATTRIBUTE = "dimensions";
    private static final String THETA_SKETCH_SIZE_ATTRIBUTE = "sketch_size";
    private static final String DB_OUTPUT_PATH_ATTRIBUTE = "DB_OUTPUT_PATH";
    private static final String TABLE_ATTRIBUTE = "TABLE";
    private static final String PRODUCT_NAME_ATTRIBUTE = "PRODUCT_NAME";
    private static final String GRANULARITY_INTERVAL_ATTRIBUTE = "GRANULARITY_INTERVAL";
    private static final String SEGMENT_GRANULARITY_ATTRIBUTE = "SEGMENT_GRANULARITY";
    private static final String INPUT_SPEC_GRANULARITY_ATTRIBUTE = "INPUT_SPEC_GRANULARITY";
    // Dynamic template files
    private static final String METRIC_SPEC_FRAGMENT_FILE = "metric_spec_fragment.st";
    private static final String METRIC_SPEC_THETA_FRAGMENT_FILE = "metric_spec_theta_fragment.st";
    private static final String INDEX_JSON_FILE = "druid_index_json.st";
    // Other settings
    private static final String DEFAULT_METRIC_AGGREGATION_TYPE = "thetaSketch";
    private static final Map<String, String> AGGREGATION_MAP = new HashMap<>();
    // Static variables
    private boolean isHourlyIngestion = false;
    private static final String HOUR_GRANULARITY_INTERVAL = "YEAR-MONTH-DAYTHOUR/PT1H";
    private static final String DAY_GRANULARITY_INTERVAL = "YEAR-MONTH-DAY/P1D";
    private static final String HOUR_INPUT_SPEC_GRANULARITY = "YEAR_MONTH_DAY_HOUR";
    private static final String DAY_INPUT_SPEC_GRANULARITY = "YEAR_MONTH_DAY";

    static {
        AGGREGATION_MAP.put(Aggregation.SUM.name, "longSum");
        AGGREGATION_MAP.put(Aggregation.COUNT.name, "longSum");
        AGGREGATION_MAP.put(Aggregation.COUNT_DISTINCT.name, "longSum");
        AGGREGATION_MAP.put(Aggregation.THETA_SKETCH.name, "thetaSketch");
        AGGREGATION_MAP.put(Aggregation.MIN.name, "longMin");
        AGGREGATION_MAP.put(Aggregation.MAX.name, "longMax");
    }

    /**
     * Create templates for hourly druid ingestion or daily druid ingestion.
     */
    public DruidIndexJson(boolean isHourlyIngestion) {
        this.isHourlyIngestion = isHourlyIngestion;
    }

    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        if (model == null || model.getProjections() == null) {
            throw new Exception("Cannot have null model or projections");
        }

        List<PipelineProjection> projections = model.getProjections();
        List<PipelineProjection> metricsProjections = 
                Measurement.getMetricProjections(projections);
        String dimensionsString = 
                Measurement.getDimensionProjections(projections).stream()
                .map(p -> String.format("\"%s\"", p.getAlias()))
                .collect(Collectors.joining(Constants.COMMA_DELIMITER + Constants.NEWLINE));

        String columnsString = projections.stream()
                .map(c -> String.format("\"%s\"", c.getAlias()))
                .collect(Collectors.joining(Constants.COMMA_DELIMITER + Constants.NEWLINE));
        // Build up list of metrics
        List<String> metricsStringList = new ArrayList<>();
        for (PipelineProjection metric : metricsProjections) {
            StringTemplate metricTemplate;
            // If theta sketch metric, use theta sketch template
            if (Aggregation.THETA_SKETCH.name.equals(metric.getAggregationName())) {
                metricTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + METRIC_SPEC_THETA_FRAGMENT_FILE));
                metricTemplate.setAttribute(THETA_SKETCH_SIZE_ATTRIBUTE, Aggregation.THETA_SKETCH_SIZE);
            } else {
                // Not theta sketch, use regular template
                metricTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + METRIC_SPEC_FRAGMENT_FILE));
            }
            metricTemplate.setAttribute(COLUMN_TYPE_ATTRIBUTE, getAggregationType(metric));
            metricTemplate.setAttribute(COLUMN_ATTRIBUTE, metric.getAlias());
            metricsStringList.add(metricTemplate.toString());
        }

        // Construct additional metrics (beyond record count) here
        String metricsString = "";
        if (metricsStringList.size() > 0) {
            metricsString = Constants.COMMA_DELIMITER + Constants.NEWLINE;
            metricsString += metricsStringList.stream().collect(Collectors.joining(Constants.COMMA_DELIMITER + Constants.NEWLINE));
        }

        StringTemplate indexJsonTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + INDEX_JSON_FILE));
        String pipelineName = model.getPipelineName();

        indexJsonTemplate.setAttribute(METRICS_ATTRIBUTE, metricsString);
        indexJsonTemplate.setAttribute(DIMENSIONS_ATTRIBUTE, dimensionsString);
        indexJsonTemplate.setAttribute(COLUMNS_ATTRIBUTE, columnsString);
        indexJsonTemplate.setAttribute(DB_OUTPUT_PATH_ATTRIBUTE, CLISettings.HDFS_OUTPUT_PATH);
        indexJsonTemplate.setAttribute(TABLE_ATTRIBUTE, pipelineName);
        indexJsonTemplate.setAttribute(PRODUCT_NAME_ATTRIBUTE, TemplateUtils.getProductName(pipelineName));
        if (isHourlyIngestion) { // create template for hourly ingestion
            indexJsonTemplate.setAttribute(GRANULARITY_INTERVAL_ATTRIBUTE, HOUR_GRANULARITY_INTERVAL);
            indexJsonTemplate.setAttribute(SEGMENT_GRANULARITY_ATTRIBUTE, Constants.HOUR);
            indexJsonTemplate.setAttribute(INPUT_SPEC_GRANULARITY_ATTRIBUTE, HOUR_INPUT_SPEC_GRANULARITY);
        } else { // create template for daily ingestion
            indexJsonTemplate.setAttribute(GRANULARITY_INTERVAL_ATTRIBUTE, DAY_GRANULARITY_INTERVAL);
            indexJsonTemplate.setAttribute(SEGMENT_GRANULARITY_ATTRIBUTE, Constants.DAY);
            indexJsonTemplate.setAttribute(INPUT_SPEC_GRANULARITY_ATTRIBUTE, DAY_INPUT_SPEC_GRANULARITY);
        }

        return XmlJsonUtils.prettyPrintJsonString(indexJsonTemplate.toString());
    }

    private String getAggregationType(PipelineProjection metric) {
        if (metric.isAggregation() && AGGREGATION_MAP.containsKey(metric.getAggregationName())) {
            return AGGREGATION_MAP.get(metric.getAggregationName());
        } else {
            return DEFAULT_METRIC_AGGREGATION_TYPE;
        }
    }
}
