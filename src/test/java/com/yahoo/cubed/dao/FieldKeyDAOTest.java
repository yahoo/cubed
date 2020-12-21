/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test fieldKey key data access object.
 */
public class FieldKeyDAOTest {
    private static Session session;
    private final String schemaName = "schema1";

    private static final Logger LOG = LoggerFactory.getLogger(FieldKeyDAOTest.class);
    
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
     * Close database session.
     */
    @AfterClass
    public static void close() throws Exception {
        if (session != null) {
            session.close();
        }
    }
    
    /**
     * Test multiple field keys.
     */
    @Test
    public void testAll() {
        long fieldId1 = 2101;
        long fieldId2 = 2102;

        FieldKey fieldKey1 = new FieldKey();
        fieldKey1.setKeyName("testFieldKey1");
        fieldKey1.setFieldId(fieldId1);
        fieldKey1.setKeyId(1101);
        fieldKey1.setSchemaName(schemaName);

        FieldKey fieldKey2 = new FieldKey();
        fieldKey2.setKeyName("testFieldKey2");
        fieldKey2.setFieldId(fieldId2);
        fieldKey2.setKeyId(1102);
        fieldKey2.setSchemaName(schemaName);
        
        FieldKeyDAO fieldKeyDAO = DAOFactory.fieldKeyDAO();
        
        int fieldKeyNum = fieldKeyDAO.fetchAll(session).size();
        
        fieldKeyDAO.save(session, fieldKey1);
        long id1 = fieldKey1.getKeyId();
        fieldKeyDAO.save(session, fieldKey2);
        long id2 = fieldKey2.getKeyId();

        Assert.assertTrue(id1 != 0);
        Assert.assertTrue(id2 != 0);
                
        Assert.assertEquals(fieldKeyDAO.fetchByCompositeKey(session, schemaName, fieldId1, id1).getKeyName(), "testFieldKey1");
        Assert.assertEquals(fieldKeyDAO.fetchByCompositeKey(session, schemaName, fieldId2, id2).getKeyName(), "testFieldKey2");
        
        Assert.assertEquals(fieldKeyDAO.fetchAll(session).size(), fieldKeyNum + 2);
        
        Assert.assertEquals(fieldKeyDAO.fetchByName(session, schemaName + FieldKey.FIELD_KEY_NAME_SEPARATOR + fieldId1 + FieldKey.FIELD_KEY_NAME_SEPARATOR + "testFieldKey1").getKeyId(), id1);
        Assert.assertEquals(fieldKeyDAO.fetchByName(session, schemaName + FieldKey.FIELD_KEY_NAME_SEPARATOR + fieldId2 + FieldKey.FIELD_KEY_NAME_SEPARATOR + "testFieldKey2").getKeyId(), id2);
        
        fieldKeyDAO.delete(session, fieldKey1);
        fieldKeyDAO.delete(session, fieldKey2);
        
        Assert.assertEquals(fieldKeyDAO.fetchAll(session).size(), fieldKeyNum);
        
    }
    
    /**
     * Check fetch field key by composite name.
     */
    @Test
    public void testFetchByName() {
        FieldKey fieldKey1 = DAOFactory.fieldKeyDAO().fetchByName(session, "schema1-4-city");
        Assert.assertEquals(fieldKey1.getKeyId(), 40001);
        FieldKey fieldKey2 = DAOFactory.fieldKeyDAO().fetchByName(session, "schema1-6-is_deleted");
        Assert.assertEquals(fieldKey2.getKeyId(), 60002);
    }

    /**
     * Check fetch fieldKey by composite key.
     */
    @Test
    public void testFetchByCompositeKey() {
        FieldKey fieldKey1 = new FieldKey();
        fieldKey1.setKeyName("testfieldKey1");
        fieldKey1.setFieldId(2101);
        fieldKey1.setKeyId(1101);
        fieldKey1.setSchemaName(schemaName);

        FieldKeyDAO fieldKeyDAO = DAOFactory.fieldKeyDAO();
        fieldKeyDAO.save(session, fieldKey1);
        long id1 = fieldKey1.getKeyId();

        Assert.assertTrue(id1 != 0);
        Assert.assertEquals(fieldKeyDAO.fetchByCompositeKey(session, schemaName, 2101, id1).getKeyName(), "testfieldKey1");

        fieldKeyDAO.delete(session, fieldKey1);
    }

    /**
     * Check fetch fieldKey by schema name, field Id and fieldKey name.
     */
    @Test
    public void testFetchBySchemaNameFieldIdKeyName() {
        FieldKey fieldKey1 = new FieldKey();
        fieldKey1.setKeyName("testfieldKey1");
        fieldKey1.setFieldId(2101);
        fieldKey1.setKeyId(1101);
        fieldKey1.setSchemaName(schemaName);

        FieldKeyDAO fieldKeyDAO = DAOFactory.fieldKeyDAO();
        fieldKeyDAO.save(session, fieldKey1);
        long id1 = fieldKey1.getKeyId();

        Assert.assertTrue(id1 != 0);
        Assert.assertEquals(fieldKeyDAO.fetchBySchemaNameFieldIdKeyName(session, schemaName, 2101, "testfieldKey1").getKeyId(), id1);

        fieldKeyDAO.delete(session, fieldKey1);

    }
}
