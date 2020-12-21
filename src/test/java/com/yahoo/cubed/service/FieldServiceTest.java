/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test field service.
 */
public class FieldServiceTest {
    private final String schemaName = "schema1";
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
     * Update causes validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException1() throws DataValidatorException, DatabaseException {
        Field field = new Field();
        field.setFieldName("newfield");
        field.setFieldType("string");
        field.setSchemaName(schemaName);
        
        ServiceFactory.fieldService().update(field);  
    }

    /**
     * Update causes validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException2() throws DataValidatorException, DatabaseException {
        Field field = new Field();
        field.setFieldId(100);
        field.setFieldName("newfield");
        field.setSchemaName(schemaName);
        field.setFieldType("string");
        
        ServiceFactory.fieldService().update(field);  
    }

    /**
     * Update causes validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException3() throws DataValidatorException, DatabaseException {
        int id = 1234;

        Field field1 = new Field();
        field1.setFieldName("newfield");
        field1.setFieldType("string");
        field1.setFieldId(id);
        field1.setSchemaName(schemaName);
        
        ServiceFactory.fieldService().save(field1);
        
        Field field2 = new Field();
        field2.setFieldName(field1.getFieldName());
        field2.setFieldId(id);
        field2.setSchemaName(schemaName);
        
        try {
            ServiceFactory.fieldService().save(field2);
        } finally {
            ServiceFactory.fieldService().delete(schemaName, id);
        }
    }

    /**
     * Update causes validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testDataValitationException4() throws DataValidatorException, DatabaseException {
        int id = 1234;

        Field field = new Field();
        field.setFieldName("newfield");
        field.setFieldType("string");
        field.setFieldId(id);
        field.setSchemaName(schemaName);
        
        ServiceFactory.fieldService().save(field);
        
        try {
            ServiceFactory.fieldService().delete(schemaName, id + 1);
        } finally {
            ServiceFactory.fieldService().delete(schemaName, id);
        }
    }

    /**
     * Create field, save, update.
     */
    @Test
    public void test5() throws DataValidatorException, DatabaseException {
        int fieldNum = ServiceFactory.fieldService().fetchAll().size();
        int id = 1234;
        
        Field field1 = new Field();
        field1.setFieldName("newfield");
        field1.setFieldType("string");
        field1.setSchemaName(schemaName);
        field1.setFieldId(id);
        
        ServiceFactory.fieldService().save(field1);
        
        Field field2 = new Field();
        field2.setFieldId(id);
        field2.setFieldName("newnewfield");
        field2.setFieldType("string");
        field2.setSchemaName(schemaName);

        ServiceFactory.fieldService().update(field2);
        
        Assert.assertEquals(ServiceFactory.fieldService().fetchByCompositeKey(schemaName, id).getFieldName(), field2.getFieldName());
        Assert.assertEquals(ServiceFactory.fieldService().fetchAll().size(), fieldNum + 1);
        
        ServiceFactory.fieldService().delete(schemaName, id);
        Assert.assertEquals(ServiceFactory.fieldService().fetchAll().size(), fieldNum);
    }
}
