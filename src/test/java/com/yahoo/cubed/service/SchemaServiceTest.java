/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.settings.CLISettings;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * SchemaKeyService Test.
 */
public class SchemaServiceTest {
    private final String schemaName = "schema1";

    private static void dropAllSchemas() throws DatabaseException, DataValidatorException {
        // Drop all schemas
        List<Schema> schemas = ServiceFactory.schemaService().fetchAll();
        for (Schema schema : schemas) {
            ServiceFactory.schemaService().delete(schema.getPrimaryName());
        }
    }
    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
    }

    /**
     * Update causes validation exception for the unknown name.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException1() throws DataValidatorException, DatabaseException {
        dropAllSchemas();
        Schema schema = new Schema();
        schema.setSchemaName("test");
        ServiceFactory.schemaService().update(schema);
    }

    /**
     * Delete causes validation exception for the unknown name.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException2() throws DataValidatorException, DatabaseException {
        dropAllSchemas();
        ServiceFactory.schemaService().delete("test");
    }
    /**
     * Check fetch schema by name.
     */
    @Test
    public void testFetchByName() throws Exception {
        dropAllSchemas();
        App.loadSchemas("src/test/resources/schemas/");
        Schema schema = ServiceFactory.schemaService().fetch("schema1");
        Assert.assertNotNull(schema);
        dropAllSchemas();
    }

    /**
     * Check fetch all Schema name with order.
     */
    @Test
    public void testFetchAllName() throws Exception {
        dropAllSchemas();

        // Fetch all schema names no matter deleted or not
        App.loadSchemas("src/test/resources/schemas/");
        List<String> names = new ArrayList<>();
        names.add("schema1");
        names.add("schema2");
        names.add("schema3");
        names.add("schema4");

        Assert.assertEquals(ServiceFactory.schemaService().fetchAll().size(), 4);
        List<String> schemaNames = ServiceFactory.schemaService().fetchAllName(false);
        for (int i = 0; i < names.size(); i++) {
            Assert.assertEquals(schemaNames.get(i), names.get(i));
        }

        // Fetch all schema names not deleted
        App.loadSchemas("src/test/resources/updated_schemas/");
        names.clear();
        names.add("schema1");
        names.add("schema2");

        Assert.assertEquals(ServiceFactory.schemaService().fetchAll().size(), 4);
        schemaNames = ServiceFactory.schemaService().fetchAllName(true);
        Assert.assertEquals(schemaNames.size(), 2);
        for (int i = 0; i < names.size(); i++) {
            Assert.assertEquals(schemaNames.get(i), names.get(i));
        }

        dropAllSchemas();
    }
}
