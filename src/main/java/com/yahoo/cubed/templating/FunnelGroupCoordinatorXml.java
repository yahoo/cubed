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
 * Filling the parameters of funnel group daily coordinator xml.
 */
public class FunnelGroupCoordinatorXml implements TemplateFile<FunnelGroup> {

    // the dynamic template file
    private static final String FUNNEL_GROUP_COORDINATOR_XML_FILE = "funnel_group_coordinator_xml.st";

    // funnel group coordinator xml attributes
    private static final String INPUT_DATASETS_ATTRIBUTE = "INPUT_DATASETS";
    private static final String INPUT_EVENTS_ATTRIBUTE = "INPUT_EVENTS";

    // xml templates
    private static final String INPUT_DATASET_TEMPLATE =
            "<dataset name=\"%s\" frequency=\"${coord:days(funnel_repeat_interval)}\" initial-instance=\"${funnel_start_time}\" timezone=\"UTC\">\n" +
            "    <uri-template>hdfs://${hdfs_output_path}/%s_${YEAR}_${MONTH}_${DAY}</uri-template>\n" +
            "    <done-flag/>\n" +
            "</dataset>";

    private static final String INPUT_EVENT_TEMPLATE =
            "<data-in name=\"INPUT_%s\" dataset=\"%s\">\n" +
             "   <instance>${coord:current(0)}</instance>\n" +
             "</data-in>";

    /**
     * Generate template file.
     */
    @Override
    public String generateFile(FunnelGroup model, long version) throws Exception {

        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + FUNNEL_GROUP_COORDINATOR_XML_FILE));

        List<String> inputDatasets = new ArrayList<>();
        List<String> inputEvents = new ArrayList<>();

        for (Pipeline pipeline : model.getPipelines()) {
            String pipelineName = pipeline.getPipelineName();
            inputDatasets.add(String.format(INPUT_DATASET_TEMPLATE, pipelineName, pipelineName));
            inputEvents.add(String.format(INPUT_EVENT_TEMPLATE, pipelineName, pipelineName));
        }

        template.setAttribute(INPUT_DATASETS_ATTRIBUTE, String.join("", inputDatasets));
        template.setAttribute(INPUT_EVENTS_ATTRIBUTE, String.join("", inputEvents));

        return XmlJsonUtils.prettyPrintXmlString(template.toString(), DEFAULT_XML_INDENT);
    }

}
