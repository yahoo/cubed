/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.util.Constants;
import com.yahoo.cubed.util.XmlJsonUtils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.List;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static com.yahoo.cubed.util.Constants.COMMA_DELIMITER;

/**
 * Druid JSON index script generator.
 */
public class FunnelGroupDruidIndexJson implements TemplateFile<FunnelGroup> {
    // Dynamic template file
    private static final String FUNNEL_GROUP_INDEX_JSON_FILE = "funnel_group_druid_index_json.st";

    // Attributes
    private static final String PRODUCT_NAME_ATTRIBUTE = "PRODUCT_NAME";
    private static final String GRANULARITY_INTERVAL_ATTRIBUTE = "GRANULARITY_INTERVAL";
    private static final String SEGMENT_GRANULARITY_ATTRIBUTE = "SEGMENT_GRANULARITY";
    private static final String FINAL_PROJECTIONS_ATTRIBUTE = "FINAL_PROJECTIONS";
    private static final String INPUT_SPEC_ATTRIBUTE = "INPUT_SPEC";
    // Static variables
    private static final String DAY_GRANULARITY_INTERVAL = "YEAR-MONTH-DAY/P1D";
    private static final String DAY_INPUT_SPEC_GRANULARITY = "YEAR_MONTH_DAY";
    private static final String MULTI_TYPE = "multi";

    // Partial json templates
    private static final String INPUT_SPEC_TEMPALTE_MULTI =
            "\"inputSpec\": {\n" +
            "    \"type\": \"%s\",\n" +
            "    \"children\": [\n" +
            "        %s\n" +
            "    ]\n" +
            "}";

    private static final String INPUT_SPEC_CHILDREN =
            "{\n" +
            "    \"type\": \"static\",\n" +
            "    \"paths\": \"hdfs://%s/%s_YEAR_MONTH_DAY/\"\n" +
            "}";

    private static final String INPUT_SPEC_TEMPLATE_SINGLE =
            "\"inputSpec\": {\n" +
            "    \"paths\": \"%s/%s_%s/\",\n" +
            "    \"type\": \"static\"\n" +
            "}";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(FunnelGroup model, long version) throws Exception {
        if (model == null || model.getProjections() == null) {
            throw new Exception("Cannot have null model or projections");
        }

        StringTemplate indexJsonTemplate = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + FUNNEL_GROUP_INDEX_JSON_FILE));
        String funnelGroupName = model.getFunnelGroupName();

        indexJsonTemplate.setAttribute(PRODUCT_NAME_ATTRIBUTE, TemplateUtils.getProductName(funnelGroupName));
        indexJsonTemplate.setAttribute(GRANULARITY_INTERVAL_ATTRIBUTE, DAY_GRANULARITY_INTERVAL);
        indexJsonTemplate.setAttribute(SEGMENT_GRANULARITY_ATTRIBUTE, Constants.DAY);

        String finalProjectionsStr = "";
        if (model.getProjections().size() > 0) {
            finalProjectionsStr = model.getProjections().stream().map(m -> m.getAlias()).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);
        }

        indexJsonTemplate.setAttribute(FINAL_PROJECTIONS_ATTRIBUTE, finalProjectionsStr);

        // filling the input spec
        List<Pipeline> pipelines = model.getPipelines();
        String inputSpec = "";
        if (pipelines.size() == 1) {
            // single funnel
            inputSpec = String.format(INPUT_SPEC_TEMPLATE_SINGLE, CLISettings.HDFS_OUTPUT_PATH, pipelines.get(0).getPipelineName(), DAY_INPUT_SPEC_GRANULARITY);
        } else if (pipelines.size() > 1) {
            // multiple funnels
            List<String> children = new ArrayList<>();
            for (Pipeline pipeline : pipelines) {
                children.add(String.format(INPUT_SPEC_CHILDREN, CLISettings.HDFS_OUTPUT_PATH, pipeline.getPipelineName()));
            }
            inputSpec = String.format(INPUT_SPEC_TEMPALTE_MULTI, MULTI_TYPE, String.join(",\n", children));
        } else {
            throw new Exception("The funnel group should contain at least one funnel.");
        }
        indexJsonTemplate.setAttribute(INPUT_SPEC_ATTRIBUTE, inputSpec);

        return XmlJsonUtils.prettyPrintJsonString(indexJsonTemplate.toString());
    }
}
