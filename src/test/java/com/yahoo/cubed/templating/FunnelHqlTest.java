/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.yahoo.cubed.App;
import com.yahoo.cubed.json.NewFunnelQuery;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.ConfigurationLoader;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test funnel Hive ETL script generator.
 */
public class FunnelHqlTest {
    FunnelHql tg;

    /**
     * Init funnel Hive ETL script generator.
     */
    @BeforeClass
    public void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
        ConfigurationLoader.load();
        tg = new FunnelHql();
    }

    /**
     * Test generation of funnel Hive ETL script.
     */
    @Test
    public void testGenerateFileNonPipeline() throws Exception {
        String jsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        ObjectMapper mapper = new ObjectMapper();
        NewFunnelQuery query = mapper.readValue(jsonReq, NewFunnelQuery.class);
        String template = tg.generateFile(query, false, false);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_template_expected.hql");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }

    /**
     * Test generation of funnel pipeline Hive ETL script.
     */
    @Test
    public void testGenerateFilePipeline() throws Exception {
        String jsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        ObjectMapper mapper = new ObjectMapper();
        NewFunnelQuery query = mapper.readValue(jsonReq, NewFunnelQuery.class);
        String template = tg.generateFile(query, true, false);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_template_pipeline_expected.hql");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }

    /**
     * Test generation of funnel group pipeline Hive ETL script.
     */
    @Test
    public void testGenerateFileFunnelGroup() throws Exception {
        String jsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        ObjectMapper mapper = new ObjectMapper();
        NewFunnelQuery query = mapper.readValue(jsonReq, NewFunnelQuery.class);
        String template = tg.generateFile(query, true, true);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_template_expected.hql");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }
}
