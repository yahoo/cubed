/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.util.Constants;
import org.antlr.stringtemplate.StringTemplate;

/**
 * Druid load shell script generator.
 */
public class DruidLoadSh implements TemplateFile<Pipeline> {
    // Template files
    private static final String DRUID_LOAD_SCRIPT_FILE = "druid_load_sh.st";
    private boolean isHourlyIngestion = false;

    /**
     * Create druid load with hourly ingestion setting.
     */
    public DruidLoadSh(boolean isHourlyIngestion) {
        this.isHourlyIngestion = isHourlyIngestion;
    }

    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.DRUID_TEMPLATES_DIR + DRUID_LOAD_SCRIPT_FILE));
        if (this.isHourlyIngestion) {
            template.setAttribute(GRANULARITY_ATTRIBUTE, Constants.HOUR);
        } else {
            template.setAttribute(GRANULARITY_ATTRIBUTE, Constants.DAY);
        }
        template.setAttribute(DRUID_HTTP_PROXY_ATTRIBUTE, CLISettings.DRUID_HTTP_PROXY);
        template.setAttribute(DRUID_INDEX_ATTRIBUTE, CLISettings.DRUID_INDEXER);
        return template.toString();
    }
}
