/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Druid load shell script.
 */
public class DruidLoadShTest {
    DruidLoadSh tg;

    /**
     * Init Druid shell generation.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new DruidLoadSh(true);
    }

    /**
     * Test generation of Druid load shell script.
     */
    @Test
    public void testGenerateDruidLoadScriptTemplate() throws Exception {
        String template = tg.generateFile(new Pipeline(), System.currentTimeMillis());
        String expected = TemplateTestUtils.loadTemplateInstance("templates/druid_load.sh");
        Assert.assertEquals(template.trim(), expected.trim());
    }
}
