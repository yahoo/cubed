/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.settings.CLISettings;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * FieldKeyService Test.
 */
public class FieldKeyServiceTest {
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
     * Test for null field key.
     */
    @Test
    public void nullFieldKeyTest() throws Exception {
        try {
            ServiceFactory.fieldKeyService().save(null);
        } catch (DataValidatorException e) {
            Assert.assertEquals(e.getMessage(), "FieldKey model is not provided.");
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }

    /**
     * Test for field key with null name.
     */
    @Test
    public void nullNameFieldKeyTest() throws Exception {
        try {
            FieldKey fieldKey = new FieldKey();
            ServiceFactory.fieldKeyService().save(fieldKey);
        } catch (DataValidatorException e) {
            Assert.assertEquals(e.getMessage(), "The name of the FieldKey is not provided.");
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }

    /**
     * Test for field key with invalid name.
     */
    @Test
    public void invalidNameFieldKeyTest() throws Exception {
        try {
            FieldKey fieldKey = new FieldKey();
            fieldKey.setKeyName("1nvalid");
            fieldKey.setSchemaName("schema1");
            fieldKey.setKeyId(1234);
            fieldKey.setFieldId(1234);
            ServiceFactory.fieldKeyService().save(fieldKey);
        } catch (DataValidatorException e) {
            Assert.assertEquals(e.getMessage(), "The name of the FieldKey should start with an English letter. It should contain only English letters and underscores.");
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }

    /**
     * Create fieldKey, save, update.
     */
    @Test
    public void test5() throws DataValidatorException, DatabaseException {
        int fieldKeyNum = ServiceFactory.fieldKeyService().fetchAll().size();
        int fieldId = 1234;
        int keyId = 5678;

        FieldKey fieldKey1 = new FieldKey();
        fieldKey1.setSchemaName(schemaName);
        fieldKey1.setFieldId(fieldId);
        fieldKey1.setKeyId(keyId);
        fieldKey1.setKeyName("newfieldkey");
        ServiceFactory.fieldKeyService().save(fieldKey1);

        FieldKey fieldKey2 = new FieldKey();
        fieldKey2.setSchemaName(schemaName);
        fieldKey2.setFieldId(fieldId);
        fieldKey2.setKeyId(keyId);
        fieldKey2.setKeyName("newnewfieldkey");
        ServiceFactory.fieldKeyService().update(fieldKey2);

        Assert.assertEquals(ServiceFactory.fieldKeyService().fetchByCompositeKey(schemaName, fieldId, keyId).getKeyName(), fieldKey2.getKeyName());
        Assert.assertEquals(ServiceFactory.fieldKeyService().fetchAll().size(), fieldKeyNum + 1);

        ServiceFactory.fieldKeyService().delete(schemaName, fieldId, keyId);
        Assert.assertEquals(ServiceFactory.fieldKeyService().fetchAll().size(), fieldKeyNum);
    }
}
