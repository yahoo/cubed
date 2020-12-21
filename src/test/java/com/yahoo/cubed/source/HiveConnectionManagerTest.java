/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.source;

import com.yahoo.cubed.json.FunnelQueryResult;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import org.powermock.core.classloader.annotations.PrepareForTest;

import com.yahoo.cubed.settings.CLISettings;

import org.testng.IObjectFactory;
import org.testng.annotations.ObjectFactory;

import java.sql.Types;
import java.sql.Connection;
import java.sql.Statement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;

import static org.mockito.Matchers.anyString;

/**
 * Test hive connection manager.
 */
@PrepareForTest(HiveConnectionManager.class)
public class HiveConnectionManagerTest {

    /**
     * Method for using PowerMockito with Testng.
     */
    @ObjectFactory
    public IObjectFactory getObjectFactory() {
        return new org.powermock.modules.testng.PowerMockObjectFactory();
    }

    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
    }

    /**
     * setup test.
     */
    @Test
    public void setupTest() {
        // property file in test resources
        try {
            HiveConnectionManager tg = new HiveConnectionManager();
            String[] args = new String[] {"--hive-jdbc", "jdbc:test", "--hive-setting", "test"};
            // Elaborately fail, setup should catchgit  exception
            tg.setup(args);

            // Mock Class.forName
            PowerMockito.mockStatic(Class.class);
            Mockito.when(Class.forName(anyString())).thenReturn(null);
            // Mock connection/statement
            PowerMockito.mockStatic(DriverManager.class);
            Connection mockedConnection = Mockito.mock(Connection.class);
            Statement mockedStatement = Mockito.mock(Statement.class);
            Mockito.when(mockedStatement.executeUpdate(anyString())).thenReturn(0);
            Mockito.when(mockedConnection.createStatement()).thenReturn(mockedStatement);
            Mockito.when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(mockedConnection);
            // Execute setup
            tg.setup(args);
            // Capture arguments
            ArgumentCaptor<String> jdbcConnectorArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> usernameArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> passwordArg = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<String> updateArg = ArgumentCaptor.forClass(String.class);

            PowerMockito.verifyStatic(DriverManager.class);
            DriverManager.getConnection(jdbcConnectorArg.capture(), usernameArg.capture(), passwordArg.capture());

            Mockito.verify(mockedStatement).executeUpdate(updateArg.capture());
            // Assert captured values
            Assert.assertEquals(jdbcConnectorArg.getValue(), "jdbc:test");
            Assert.assertEquals(usernameArg.getValue(), "anon");
            Assert.assertEquals(passwordArg.getValue(), "anon");
            Assert.assertEquals(updateArg.getValue(), "set test");
        } catch (Exception e) {
            Assert.fail("exception", e);
        }
    }

    /**
     * execute test & addRow test.
     */
    @Test
    public void executeAndAddRowTest() {
        // property file in test resources
        try {
            HiveConnectionManager tg = new HiveConnectionManager();
            String[] args = new String[] {"--hive-jdbc", "jdbc:test"};
            String query = "SET hive.exec.compress.output=false;\n" +
                    "       funnel_merge(funnels)\n" +
                    "FROM (SELECT funnel(step, array(0,1,2,3)) as funnels\n" +
                    "      FROM (SELECT CASE  WHEN (event == 'app_install') THEN 0 \n" +
                    "  WHEN (event == 'app_start') THEN 1 \n" +
                    "  WHEN (event == 'web_view_load-join_public_league') THEN 2 \n" +
                    "  WHEN ((\n" +
                    "  event == 'join_public_league' OR \n" +
                    "  event == 'join_private_league' OR \n" +
                    "  event == 'join_custom_league' OR \n" +
                    "  event == 'join_renewed_league'\n" +
                    ")) THEN 3\n" +
                    "                        ELSE NULL\n" +
                    "                   END AS step,\n" +
                    "                   timestamp AS ts,\n" +
                    "                   bcookie AS user_id\n" +
                    "            FROM schema1.daily_data\n" +
                    "            WHERE (\n" +
                    "  filter_tag is NULL AND \n" +
                    "  network == 'on' AND \n" +
                    "  pty_family == 'sports' AND \n" +
                    "  pty_device == 'mobile' AND \n" +
                    "  pty_experience == 'app' AND \n" +
                    "  event_family == 'unregistered' AND \n" +
                    "  (\n" +
                    "    ptyid == '500013' OR \n" +
                    "    ptyid == '530013'\n" +
                    "  )\n" +
                    ") AND  ((event == 'app_install') OR  \n" +
                    " (event == 'app_start') OR  \n" +
                    " (event == 'web_view_load-join_public_league') OR  \n" +
                    " ((\n" +
                    "  event == 'join_public_league' OR \n" +
                    "  event == 'join_private_league' OR \n" +
                    "  event == 'join_custom_league' OR \n" +
                    "  event == 'join_renewed_league'\n" +
                    "))) AND \n" +
                    "                  dt >= '${hivevar:QUERY_START_DATE}' AND dt <= '${hivevar:QUERY_END_DATE}'\n" +
                    "            DISTRIBUTE BY user_id\n" +
                    "            SORT BY ts, user_id, step ASC) AS sorted_data_for_funnel\n" +
                    "      GROUP BY  user_id) grouped_data_for_funnel;";
            // Mock Class.forName
            PowerMockito.mockStatic(Class.class);
            Mockito.when(Class.forName(anyString())).thenReturn(null);

            // Mock connection/statement
            PowerMockito.mockStatic(DriverManager.class);
            Connection mockedConnection = Mockito.mock(Connection.class);
            Statement mockedStatement = Mockito.mock(Statement.class);
            ResultSet mockedResultSet = Mockito.mock(ResultSet.class);
            ResultSetMetaData mockedResultSetMetaData = Mockito.mock(ResultSetMetaData.class);

            Mockito.when(mockedResultSetMetaData.getColumnCount()).thenReturn(2);
            Mockito.when(mockedResultSetMetaData.getColumnType(1)).thenReturn(Types.VARCHAR);
            Mockito.when(mockedResultSetMetaData.getColumnType(2)).thenReturn(Types.ARRAY);
            Mockito.when(mockedResultSet.next()).thenReturn(true).thenReturn(false);
            Mockito.when(mockedResultSet.getString(1)).thenReturn("test");
            Mockito.when(mockedResultSet.getString(2)).thenReturn("[100,50]");
            Mockito.when(mockedResultSet.getMetaData()).thenReturn(mockedResultSetMetaData);
            Mockito.when(mockedStatement.executeQuery(anyString())).thenReturn(mockedResultSet);
            Mockito.when(mockedConnection.createStatement()).thenReturn(mockedStatement);
            Mockito.when(DriverManager.getConnection(anyString(), anyString(), anyString())).thenReturn(mockedConnection);
            // Execute setup
            tg.setup(args);


            List<FunnelQueryResult> result = tg.execute(query);

            Assert.assertEquals(result.size(), 1);
            Assert.assertEquals(result.get(0).getKeys().size(), 1);
            Assert.assertEquals(result.get(0).getKeys().get(0), "test");
            Assert.assertEquals(result.get(0).getValues().size(), 2);
            Assert.assertEquals(result.get(0).getValues().get(0), "100");
            Assert.assertEquals(result.get(0).getValues().get(1), "50");

            // Elaborately fail, setup should catch the exception
            Mockito.when(mockedStatement.executeQuery(anyString())).thenThrow(new SQLException("test"));
            tg.setup(args);
            tg.execute("invalid sql query");

        } catch (Exception e) {
            Assert.fail("exception", e);
        }
    }
}
