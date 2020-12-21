/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import com.yahoo.cubed.settings.CLISettings;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Load database settings.
 */
public class ConfigurationLoader {
    /** Database URL. */
    public static final String DATABASEURL = "database_url";
    /** Database username. */
    public static final String USERNAME = "database_username";
    /** Database password. */
    public static final String PASSWORD = "database_password";
    /** SQL driver. */
    public static final String DRIVER = "sql_driver";
    /** SQL dialect. */
    public static final String DIALECT = "sql_dialect";
    private static Properties config = null;

    /**
     * Load the configuration file.
     */
    public static void load() throws IOException {
        config = new Properties();
        File file = new File(CLISettings.DB_CONFIG_FILE);
        InputStream in = new FileInputStream(file);
        config.load(in);
        in.close();
    }

    /**
     * Get property value.
     */
    public static String getProperty(String key) {
        if (config == null) {
            return null;
        }
        return config.getProperty(key);
    }
}
