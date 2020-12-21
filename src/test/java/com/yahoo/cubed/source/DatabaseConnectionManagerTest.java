/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.yahoo.cubed.settings.CLISettings;
import java.sql.DatabaseMetaData;

/**
 * Test configuration loader.
 */
public class DatabaseConnectionManagerTest {
    /**
     * Setup database config file.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
    }

    /**
     * Test get database password.
     */
    @Test
    public void testCreateConnection() throws Exception {
        DatabaseMetaData metadata = DatabaseConnectionManager.createConnection().getMetaData();
        Assert.assertEquals(metadata.getURL(), "jdbc:h2:file:./target/cubed");
        Assert.assertEquals(metadata.getUserName(), "ROOT");
        Assert.assertEquals(metadata.getDriverName(), "H2 JDBC Driver");
    }
}
