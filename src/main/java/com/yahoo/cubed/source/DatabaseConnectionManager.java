/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Database connection manager.
 */
public class DatabaseConnectionManager {
    /**
     * Create a database connection.
     */
    public static Connection createConnection() throws ClassNotFoundException, SQLException {
        Class.forName(
                ConfigurationLoader.getProperty(ConfigurationLoader.DRIVER));
        return DriverManager.getConnection(
                ConfigurationLoader.getProperty(ConfigurationLoader.DATABASEURL),
                ConfigurationLoader.getProperty(ConfigurationLoader.USERNAME),
                ConfigurationLoader.getProperty(ConfigurationLoader.PASSWORD));
    }
}
