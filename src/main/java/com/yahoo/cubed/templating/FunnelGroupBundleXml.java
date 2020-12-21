/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.util.Utils;
import com.yahoo.cubed.util.XmlJsonUtils;
import org.antlr.stringtemplate.StringTemplate;

import java.util.List;
import java.util.ArrayList;

/**
 * Filling the parameters of funnel group bundle xml.
 */
public class FunnelGroupBundleXml implements TemplateFile<FunnelGroup> {

    // the dynamic template file
    private static final String FUNNEL_GROUP_BUNDLE_XML_FILE = "funnel_group_bundle_xml.st";
    // all coordinator attributes
    private static final String COORDINATORS_ATTRIBUTE = "COORDINATORS";
    // individual coordinator xml template
    private static final String COORDINATOR_TEMPLATE =
            "<coordinator name=\"%s\">\n" +
            "    <app-path>${project_path}/funnel_group/coordinator_%s.xml</app-path>\n" +
            "</coordinator>";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(FunnelGroup model, long version) throws Exception {

        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_GROUP_BUNDLE_XML_FILE));

        String funnelGroupName = model.getFunnelGroupName();
        List<String> coordinators = new ArrayList<>();
        for (Pipeline pipeline : model.getPipelines()) {
            String simplifiedFunnelName = pipeline.getPipelineName().replace(funnelGroupName + Utils.UNDERSCORE_DELIMITER, "");
            coordinators.add(String.format(COORDINATOR_TEMPLATE, simplifiedFunnelName, pipeline.getPipelineName()));
        }
        template.setAttribute(COORDINATORS_ATTRIBUTE, String.join("", coordinators));
        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }
}
