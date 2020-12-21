/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.util.XmlJsonUtils;
import com.yahoo.cubed.util.Constants;
import org.antlr.stringtemplate.StringTemplate;
import java.util.Map;

/**
 * Bundle properties funnel group XML generator.
 */
public class BundlePropertiesFunnelGroupXml implements TemplateFile<FunnelGroup> {
    // Dynamic template files
    private static final String FUNNEL_GROUP_PROPERTIES_XML_FILE = "funnel_group_properties_xml.st";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(FunnelGroup model, long version) throws Exception {
        String deployPath = String.format(DEPLOY_PATH_TEMPLATE, model.getFunnelGroupName(), version);
        String productName = TemplateUtils.getProductName(model.getFunnelGroupName());
        String pipelineOwner = CLISettings.PIPELINE_OWNER;
        String tableName = model.getFunnelGroupName();

        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_GROUP_PROPERTIES_XML_FILE));
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
        template.setAttribute(FUNNEL_END_TIME, DEFAULT_END_TIME_DATE);
        template.setAttribute(BUNDLE_PROPERTY_FILE_ATTRIBUTE, "bundle");
        template.setAttribute(FUNNEL_QUERY_RANGE, model.getFunnelGroupQueryRange());
        template.setAttribute(FUNNEL_REPEAT_INTERVAL, model.getFunnelGroupRepeatInterval());

        // Fill in schema related attributes
        Schema schema = ServiceFactory.schemaService().fetchByName(model.getFunnelGroupSchemaName());
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> tables = mapper.readValue(schema.getSchemaTables(), Map.class);
        template.setAttribute(TARGET_TABLE, schema.getSchemaTargetTable());
        template.setAttribute(IN_DATABASE, schema.getSchemaDatabase());
        template.setAttribute(IN_HOUR_TABLE, tables.get(Constants.HOURLY_TABLE));
        template.setAttribute(IN_HOUR_PARTITION, tables.get(Constants.HOURLY_PARTITION_TEMPLATE));
        template.setAttribute(IN_DAY_TABLE, tables.get(Constants.DAILY_TABLE));
        template.setAttribute(IN_DAY_PARTITION, tables.get(Constants.DAILY_PARTITION_TEMPLATE));

        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }
}
