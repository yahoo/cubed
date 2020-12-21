/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.util.Constants;
import com.yahoo.cubed.util.XmlJsonUtils;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.util.Utils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.Map;

/**
 * Filling the parameters of funnel coordinator xml.
 */
public class FunnelCoordinatorXml implements TemplateFile<Pipeline> {

    // the dynamic template file
    private static final String FUNNEL_COORDINATOR_XML_FILE = "funnel_coordinator_xml.st";
    // static template files
    private static final String DAY_INPUT_DATASET_FRAGMENT_FILE = "day_input_dataset_funnel_coord_fragment.xml";
    private static final String HOUR_INPUT_DATASET_FRAGMENT_FILE = "hour_input_dataset_funnel_coord_fragment.xml";

    // funnel coordinator xml attributes
    private static final String INPUT_DATASET_ATTRIBUTE = "INPUT_DATASET";
    private static final String START_END_INSTANCE_ATTRIBUTE = "START_END_INSTANCE";
    private static final String COORD_NAME_ATTRIBUTE = "COORD_NAME";
    private static final String FUNNEL_NAME_ATTRIBUTE = "FUNNEL_NAME";

    // xml templates
    private static final String DAY_START_END_INSTANCE = "<instance>${coord:current(0)}</instance>";

    private static final String HOUR_START_END_INSTANCE =
            "<start-instance>${coord:current(-(coord:hoursInDay(0)) * funnel_repeat_interval)}</start-instance>\n" +
            "<end-instance>${coord:current(-1)}</end-instance>";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_COORDINATOR_XML_FILE));

        // figure out the data source granularity
        Schema schema = ServiceFactory.schemaService().fetchByName(model.getPipelineSchemaName());
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> tables = mapper.readValue(schema.getSchemaTables(), Map.class);
        String inDayTable = tables.get(Constants.DAILY_TABLE);
        String inHourTable = tables.get(Constants.HOURLY_TABLE);
        boolean isDataSourceHourly;
        if (inDayTable != null && !inDayTable.isEmpty()) {
            isDataSourceHourly = false;
        } else if (inHourTable != null && !inHourTable.isEmpty()) {
            isDataSourceHourly = true;
        } else {
            throw new Exception("Please specify either daily_table or hourly_table.");
        }

        // fill in day / hour specific sections
        if (isDataSourceHourly) {
            template.setAttribute(INPUT_DATASET_ATTRIBUTE, TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.STATIC_RESOURCE_DIR + HOUR_INPUT_DATASET_FRAGMENT_FILE));
            template.setAttribute(START_END_INSTANCE_ATTRIBUTE, HOUR_START_END_INSTANCE);
        } else {
            template.setAttribute(INPUT_DATASET_ATTRIBUTE, TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.STATIC_RESOURCE_DIR + DAY_INPUT_DATASET_FRAGMENT_FILE));
            template.setAttribute(START_END_INSTANCE_ATTRIBUTE, DAY_START_END_INSTANCE);
        }

        // fill in funnel name related sections
        String funnelName = model.getPipelineName();
        template.setAttribute(COORD_NAME_ATTRIBUTE, CLISettings.INSTANCE_NAME + Utils.UNDERSCORE_DELIMITER + funnelName);
        template.setAttribute(FUNNEL_NAME_ATTRIBUTE, funnelName);
        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }

}
