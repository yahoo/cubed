/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import com.yahoo.cubed.json.FunnelQueryResult;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;

/**
 * Hive Connection manager.
 */
@Slf4j
public class HiveConnectionManager {
    /**
     * jdbc connection string.
     */
    public static final String HIVE_JDBC = "hive-jdbc";
    /**
     * hive driver.
     */
    public static final String HIVE_DRIVER = "hive-driver";
    /**
     * username.
     */
    public static final String HIVE_USERNAME = "hive-username";
    /**
     * password.
     */
    public static final String HIVE_PASSWORD = "hive-password";
    /**
     * custom hive settings.
     */
    public static final String HIVE_SETTING = "hive-setting";
    /**
     * Hive driver.
     */
    public static final String DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
    /**
     * setting prefix.
     */
    public static final String SETTING_PREFIX = "set ";

    private Statement statement;

    private final OptionParser parser = new OptionParser() {
        {
            acceptsAll(singletonList(HIVE_JDBC), "JDBC string to the HiveServer2 with an optional database. " +
                    "If the database is provided, the queries must NOT have one. " +
                    "Ex: 'jdbc:hive2://HIVE_SERVER:PORT/[DATABASE_FOR_ALL_QUERIES]' ")
                    .withRequiredArg()
                    .required()
                    .describedAs("Hive JDBC connector");
            acceptsAll(singletonList(HIVE_DRIVER), "Fully qualified package name to the hive driver.")
                    .withRequiredArg()
                    .describedAs("Hive driver")
                    .defaultsTo(DRIVER_NAME);
            acceptsAll(singletonList(HIVE_USERNAME), "Hive server username.")
                    .withRequiredArg()
                    .describedAs("Hive server username")
                    .defaultsTo("anon");
            acceptsAll(singletonList(HIVE_PASSWORD), "Hive server password.")
                    .withRequiredArg()
                    .describedAs("Hive server password")
                    .defaultsTo("anon");
            acceptsAll(singletonList(HIVE_SETTING), "Settings and their values. Ex: 'hive.execution.engine=mr'")
                    .withRequiredArg()
                    .describedAs("Hive generic settings to use.");
            allowsUnrecognizedOptions();
        }
    };

    /**
     * Setup HiveConnection.
     * @param arguments
     * @return
     */
    public boolean setup(String[] arguments) {
        OptionSet options = parser.parse(arguments);
        try {
            statement = setupConnection(options);
            setHiveSettings(options, statement);
        } catch (ClassNotFoundException | SQLException e) {
            log.error("Could not set up the Hive engine", e);
            return false;
        }
        return true;
    }

    /**
     * Execute hive query.
     * @param query
     * @return
     */
    public ArrayList<FunnelQueryResult> execute(String query) {
        log.info("Running {}: {}", query);
        try {
            String[] queries = query.split(";");
            for (int i = 0; i < queries.length - 1; i++) {
                log.info("Executing " + queries[i]);
                statement.executeQuery(queries[i]);
            }
            String lastQuery = queries[queries.length - 1];
            log.info("Executing " + lastQuery);
            ResultSet result = statement.executeQuery(lastQuery);
            ResultSetMetaData metadata = result.getMetaData();
            int columns = metadata.getColumnCount();
            log.info("Finished running query, storing results");

            ArrayList<FunnelQueryResult> resultsList = new ArrayList();
            while (result.next()) {
                addRow(result, metadata, columns, resultsList);
            }
            result.close();
            return resultsList;
        } catch (SQLException e) {
            log.error("SQL problem with Hive query: {}\n{}\n{}", query, e);
        }
        return null;
    }

    private void addRow(ResultSet result, ResultSetMetaData metadata, int columns, ArrayList<FunnelQueryResult> storage) throws SQLException {
        FunnelQueryResult currentResult = new FunnelQueryResult();
        for (int i = 1; i < columns + 1; i++) {
            log.info("Iterating over result {}", i);
            // The name and type getting is being done per row. We should fix it even though Hive gets it only once.
            String name = metadata.getColumnName(i);
            int type = metadata.getColumnType(i);
            if (type == Types.ARRAY) {
                // JDBC driver does not support fetching arrays, so fetch as a String and then parse.
                // Results are in the format [123,456]
                String value = result.getString(i);
                String[] values = value.substring(1, value.length() - 1).split(",");
                log.info("Found an array with value {} and parsed values {}", value, values);
                currentResult.getValues().addAll(Arrays.asList(values));
                storage.add(currentResult);
                currentResult = new FunnelQueryResult();
            } else if (type == Types.VARCHAR) {
                String value = result.getString(i);
                log.info("Found a string with value {}", value);
                currentResult.getKeys().add(value);
            }
            log.info("Column: {}\tType: {}\tValue: {}", name, type, result.getString(i));
        }
    }

    /**
     * Sets up the connection using JDBC.
     *
     * @param options A {@link joptsimple.OptionSet} object.
     * @return The created {@link java.sql.Statement} object.
     * @throws java.lang.ClassNotFoundException if any.
     * @throws java.sql.SQLException            if any.
     */
    Statement setupConnection(OptionSet options) throws ClassNotFoundException, SQLException {
        // Load the JDBC driver
        String driver = (String) options.valueOf(HIVE_DRIVER);
        log.info("Loading JDBC driver: {}", driver);
        Class.forName(driver);

        // Get the JDBC connector
        String jdbcConnector = (String) options.valueOf(HIVE_JDBC);

        log.info("Connecting to: {}", jdbcConnector);
        String username = (String) options.valueOf(HIVE_USERNAME);
        String password = (String) options.valueOf(HIVE_PASSWORD);

        // Start the connection
        Connection connection = DriverManager.getConnection(jdbcConnector, username, password);
        return connection.createStatement();
    }

    /**
     * Applies any settings if provided.
     *
     * @param options   A {@link joptsimple.OptionSet} object.
     * @param statement A {@link java.sql.Statement} to execute the setting updates to.
     * @throws java.sql.SQLException if any.
     */
    void setHiveSettings(OptionSet options, Statement statement) throws SQLException {
        for (String setting : (List<String>) options.valuesOf(HIVE_SETTING)) {
            log.info("Applying setting {}", setting);
            statement.executeUpdate(SETTING_PREFIX + setting);
        }
    }
}
