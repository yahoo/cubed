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
 * Test funnel group druid index json generation.
 */
public class FunnelGroupDruidIndexJsonTest {

    private FunnelGroupDruidIndexJson json;

    /**
     * Init bundle properties generation.
     */
    @BeforeClass
    public void initialize() throws Exception {
        json = new FunnelGroupDruidIndexJson();
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generate druid index json.
     */
    @Test
    public void testGenerateDruidIndexJsonTemplate() throws Exception {
        String template = json.generateFile(TestUtils.constructSampleFunnelGroup(), 1L);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_druid_index_expected.json");
        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }
}
