/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.App;
import com.yahoo.cubed.TestUtils;
import com.yahoo.cubed.settings.CLISettings;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test funnel coordinator xml generation.
 */
public class FunnelCoordinatorXmlTest {

    private FunnelCoordinatorXml xml;

    /**
     * Init bundle properties generation.
     */
    @BeforeClass
    public void initialize() throws Exception {
        xml = new FunnelCoordinatorXml();
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generate funnel coordinator xml (daily source).
     */
    @Test
    public void testGenerateFunnelCoordinatorTemplate() throws Exception {
        String template = xml.generateFile(TestUtils.constructSampleFunnel(), 1L);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_coordinator_expected.xml");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }

    /**
     * Test generate funnel coordinator xml (hourly source).
     */
    @Test
    public void testGenerateFunnelCoordinatorTemplateHourlySource() throws Exception {
        String template = xml.generateFile(TestUtils.constructSampleFunnelHourlySource(), 1L);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_coordinator_hourly_source_expected.xml");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }
}
