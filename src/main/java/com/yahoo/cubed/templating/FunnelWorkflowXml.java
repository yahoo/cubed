/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.util.XmlJsonUtils;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.util.Utils;
import org.antlr.stringtemplate.StringTemplate;

/**
 * Filling the parameters of funnel workflow xml.
 */
public class FunnelWorkflowXml implements TemplateFile<Pipeline> {

    private static final String FUNNEL_WORKFLOW_XML_FILE = "funnel_workflow_xml.st";
    private static final String WORKFLOW_NAME_ATTRIBUTE = "WORKFLOW_NAME";
    private static final String FUNNEL_NAME_ATTRIBTUE = "FUNNEL_NAME";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_WORKFLOW_XML_FILE));
        String funnelName = model.getPipelineName();
        template.setAttribute(WORKFLOW_NAME_ATTRIBUTE, CLISettings.INSTANCE_NAME + Utils.UNDERSCORE_DELIMITER + funnelName);
        template.setAttribute(FUNNEL_NAME_ATTRIBTUE, funnelName);
        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }
}
