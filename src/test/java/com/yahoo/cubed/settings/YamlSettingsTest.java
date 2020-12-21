/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.settings;


import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * YamlSettings Test.
 */
public class YamlSettingsTest {
    /**
     * Test printing YAML settings.
     */
    @Test
    public void yamlSettingPrintTest() throws Exception {
        try {
            // Parse CLI args
            YamlSettings settings = new YamlSettings();

            // Print settings
            settings.print();
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }
}
