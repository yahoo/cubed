/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.settings;

import com.beust.jcommander.JCommander;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test settings.
 */
public class CLISettingsTest {
    /**
     * Test simple CLI settings.
     */
    @Test
    public void testParseArgsHappyPath() throws Exception {
        String[] args = {"--schema-files-dir", "src/test/resources/schemas/", "--db-config-file", "src/test/resources/database-configuration.properties"};

        // Parse CLI args
        CLISettings settings = new CLISettings();
        new JCommander(settings, args);

        // Print settings
        settings.print();

        Assert.assertEquals("src/test/resources/schemas/", CLISettings.SCHEMA_FILES_DIR);
        Assert.assertEquals("src/test/resources/database-configuration.properties", CLISettings.DB_CONFIG_FILE);
    }
}
