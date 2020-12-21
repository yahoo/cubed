/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.util.XmlJsonUtils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.ArrayList;
import java.util.List;

/**
 * Filling the parameters of funnel group daily workflow xml.
 */
public class FunnelGroupWorkflowXml implements TemplateFile<FunnelGroup> {

    // the dynamic template file
    private static final String FUNNEL_GROUP_WORKFLOW_XML_FILE = "funnel_group_workflow_xml.st";

    // funnel group workflow xml attributes
    private static final String TSV_DIR_TO_DELETE_ATTRIBUTE = "TSV_DIR_TO_DELETE";

    // xml templates
    private static final String DELETE_TSV_TEMPLATE = "<delete path='hdfs://${hdfs_output_path}/%s_${year}_${month}_${day}' />";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(FunnelGroup model, long version) throws Exception {
        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_GROUP_WORKFLOW_XML_FILE));

        List<String> pathsToDelete = new ArrayList<>();

        for (Pipeline pipeline : model.getPipelines()) {
            String pipelineName = pipeline.getPipelineName();
            pathsToDelete.add(String.format(DELETE_TSV_TEMPLATE, pipelineName));
        }

        template.setAttribute(TSV_DIR_TO_DELETE_ATTRIBUTE, String.join("\n", pathsToDelete));

        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }
}
