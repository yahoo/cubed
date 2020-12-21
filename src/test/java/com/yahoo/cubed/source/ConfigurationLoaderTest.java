/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.yahoo.cubed.settings.CLISettings;

/**
 * Test configuration loader.
 */
public class ConfigurationLoaderTest {
    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
    }

    /**
     * Configuration load test.
     */
    @Test
    public void testConfigurationLoad() {
        // property file in test resources
        try {
            ConfigurationLoader.load();
        } catch (IOException e) {
            Assert.fail("exception", e);
        }
    }
}
