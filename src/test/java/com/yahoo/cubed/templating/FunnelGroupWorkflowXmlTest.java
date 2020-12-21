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
 * Test funnel group workflow xml generation.
 */
public class FunnelGroupWorkflowXmlTest {

    private FunnelGroupWorkflowXml xml;

    /**
     * Init bundle properties generation.
     */
    @BeforeClass
    public void initialize() throws Exception {
        xml = new FunnelGroupWorkflowXml();
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generate workflow xml.
     */
    @Test
    public void testGenerateWorkflowTemplate() throws Exception {
        String template = xml.generateFile(TestUtils.constructSampleFunnelGroup(), 1L);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_workflow_expected.xml");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }
}
