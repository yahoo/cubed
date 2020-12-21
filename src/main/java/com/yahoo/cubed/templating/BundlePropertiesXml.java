/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.util.Constants;
import com.yahoo.cubed.util.XmlJsonUtils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.Map;

/**
 * Bundle properties XML generation.
 */
public class BundlePropertiesXml implements TemplateFile<Pipeline> {
    // Dynamic template files
    private static final String PROPERTIES_XML_FILE = "properties_xml.st";

    // End time of the regular (moving forward, not backfill) oozie job
    private static final String REGULAR_END_TIME = "REGULAR_END_TIME";

    
    /**
     * Set extra properties.
     */
    protected void setExtraProperties(StringTemplate template, Pipeline model) {
        template.setAttribute(BUNDLE_PROPERTY_FILE_ATTRIBUTE, "bundle");
        template.setAttribute(FUNNEL_QUERY_RANGE, "1");
        template.setAttribute(FUNNEL_REPEAT_INTERVAL, "1");
        template.setAttribute(BundlePropertiesXml.FUNNEL_STEPS, "default");
    }

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        String deployPath = String.format(DEPLOY_PATH_TEMPLATE, model.getPipelineName(), version);
        String productName = TemplateUtils.getProductName(model.getPipelineName());
        String pipelineOwner = CLISettings.PIPELINE_OWNER;
        String tableName = model.getPipelineName();
        String regularEndTime = model.getPipelineEndTime();

        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + PROPERTIES_XML_FILE));
        template.setAttribute(VERSION_ATTRIBUTE, version);
        template.setAttribute(USER_NAME_ATTRIBUTE, CLISettings.USER_NAME);
        template.setAttribute(PRODUCT_NAME_ATTRIBUTE, productName);
        template.setAttribute(DEPLOY_PATH_ATTRIBUTE, deployPath);
        template.setAttribute(PIPELINE_OWNER_ATTRIBUTE, pipelineOwner);
        template.setAttribute(TRASH_DIR_ATTRIBUTE, CLISettings.TRASH_DIR);
        template.setAttribute(JOB_TRACKER_URL_ATTRIBUTE, CLISettings.JOB_TRACKER_URL);
        template.setAttribute(NAME_NODE_URL_ATTRIBUTE, CLISettings.NAME_NODE_URL);
        template.setAttribute(HCAT_URI_ATTRIBUTE, CLISettings.HCAT_URI);
        template.setAttribute(HCAT_PRINCIPAL_ATTRIBUTE, CLISettings.HCAT_PRINCIPAL);
        template.setAttribute(OOZIE_UI_URL_ATTRIBUTE, CLISettings.OOZIE_UI_URL);
        template.setAttribute(DB_OUTPUT_PATH_ATTRIBUTE, CLISettings.HDFS_OUTPUT_PATH);
        template.setAttribute(HDFS_SERVERS_ATTRIBUTE, CLISettings.HDFS_SERVERS);
        template.setAttribute(QUEUE_ATTRIBUTE, CLISettings.QUEUE);
        template.setAttribute(TABLE_ATTRIBUTE, tableName);
        template.setAttribute(EMAIL_ATTRIBUTE, CLISettings.PIPELINE_EMAIL);

        // Fetch schema info
        Schema schema = ServiceFactory.schemaService().fetchByName(model.getPipelineSchemaName());
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> tables = mapper.readValue(schema.getSchemaTables(), Map.class);
        final String inDatabase = schema.getSchemaDatabase();
        final String inHourTable = tables.get(Constants.HOURLY_TABLE);
        final String inDayTable = tables.get(Constants.DAILY_TABLE);
        final String inHourPartition = tables.get(Constants.HOURLY_PARTITION_TEMPLATE);
        final String inDayPartition = tables.get(Constants.DAILY_PARTITION_TEMPLATE);
        // Set values from the schema
        template.setAttribute(IN_DATABASE, inDatabase);
        template.setAttribute(IN_HOUR_TABLE, inHourTable);
        template.setAttribute(IN_HOUR_PARTITION, inHourPartition);
        template.setAttribute(IN_DAY_TABLE, inDayTable);
        template.setAttribute(IN_DAY_PARTITION, inDayPartition);

        // Set regular oozie job end time
        if (regularEndTime == null) {
            // Use the default end time
            template.setAttribute(REGULAR_END_TIME, DEFAULT_END_TIME_DATE);
        } else {
            // Use the user provided end time
            template.setAttribute(REGULAR_END_TIME, regularEndTime + ZERO_HOUR);
        }
        // for overriding: extra properties
        this.setExtraProperties(template, model);

        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }
}
