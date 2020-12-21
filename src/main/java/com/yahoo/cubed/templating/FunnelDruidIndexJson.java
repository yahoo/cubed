/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.util.Constants;
import com.yahoo.cubed.util.XmlJsonUtils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.stream.Collectors;

import static com.yahoo.cubed.util.Constants.COMMA_DELIMITER;

/**
 * Druid JSON index script generator.
 */
public class FunnelDruidIndexJson implements TemplateFile<Pipeline> {
    // Attributes
    private static final String DB_OUTPUT_PATH_ATTRIBUTE = "DB_OUTPUT_PATH";
    private static final String TABLE_ATTRIBUTE = "TABLE";
    private static final String PRODUCT_NAME_ATTRIBUTE = "PRODUCT_NAME";
    private static final String GRANULARITY_INTERVAL_ATTRIBUTE = "GRANULARITY_INTERVAL";
    private static final String SEGMENT_GRANULARITY_ATTRIBUTE = "SEGMENT_GRANULARITY";
    private static final String INPUT_SPEC_GRANULARITY_ATTRIBUTE = "INPUT_SPEC_GRANULARITY";
    private static final String FINAL_PROJECTIONS_ATTRIBUTE = "FINAL_PROJECTIONS";
    // Dynamic template files
    private static final String FUNNEL_INDEX_JSON_FILE = "funnel_druid_index_json.st";
    private static final String FUNNEL_INDEX_JSON_FILE_FUNNEL_GROUP = "funnel_druid_index_json_funnel_group.st";
    // Static variables
    private static final String DAY_GRANULARITY_INTERVAL = "YEAR-MONTH-DAY/P1D";
    private static final String DAY_INPUT_SPEC_GRANULARITY = "YEAR_MONTH_DAY";

    /**
     * Create templates for hourly druid ingestion or daily druid ingestion.
     */
    public FunnelDruidIndexJson() { }

    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        if (model == null || model.getProjections() == null) {
            throw new Exception("Cannot have null model or projections");
        }

        StringTemplate indexJsonTemplate = null;
        if (model.getFunnelGroupId() > 0L) { // this funnel belongs to a funnel group
            indexJsonTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + FUNNEL_INDEX_JSON_FILE_FUNNEL_GROUP));
        } else { // this funnel does not belong to any funnel group
            indexJsonTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + FUNNEL_INDEX_JSON_FILE));
        }

        String pipelineName = model.getPipelineName();

        indexJsonTemplate.setAttribute(DB_OUTPUT_PATH_ATTRIBUTE, CLISettings.HDFS_OUTPUT_PATH);
        indexJsonTemplate.setAttribute(TABLE_ATTRIBUTE, pipelineName);
        indexJsonTemplate.setAttribute(PRODUCT_NAME_ATTRIBUTE, TemplateUtils.getProductName(pipelineName));
        // create template for daily ingestion
        indexJsonTemplate.setAttribute(GRANULARITY_INTERVAL_ATTRIBUTE, DAY_GRANULARITY_INTERVAL);
        indexJsonTemplate.setAttribute(SEGMENT_GRANULARITY_ATTRIBUTE, Constants.DAY);
        indexJsonTemplate.setAttribute(INPUT_SPEC_GRANULARITY_ATTRIBUTE, DAY_INPUT_SPEC_GRANULARITY);

        String finalProjectionsStr = "";
        if (model.getProjections().size() > 0) {
            finalProjectionsStr = model.getProjections().stream().map(m -> m.getAlias()).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);
        }

        indexJsonTemplate.setAttribute(FINAL_PROJECTIONS_ATTRIBUTE, finalProjectionsStr);
        return XmlJsonUtils.prettyPrintJsonString(indexJsonTemplate.toString());
    }
}
