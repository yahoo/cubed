/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.Schema;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;

/**
 * Test pipeline data access object.
 */
public class SchemaDAOTest {

    private static Session session;

    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
        session = HibernateSessionFactoryManager.getSessionFactory().openSession();
    }

    /**
     * Close database.
     */
    @AfterClass
    public static void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }

    /**
     * Check fetch schema by schema name.
     */
    @Test
    public void testFetchByName() {
        Schema schema = DAOFactory.schemaDAO().fetchByName(session, "schema1");
        Assert.assertNotNull(schema);
    }

}
