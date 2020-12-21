/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;
import com.yahoo.cubed.util.Measurement;
import java.util.List;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test field data access object.
 */
public class FieldDAOTest {
    private static Session session;
    private final String schemaName = "schema1";

    private static final Logger LOG = LoggerFactory.getLogger(FieldDAOTest.class);
    
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
     * Test multiple fields.
     */
    @Test
    public void testAll() {
        Field field1 = new Field();
        field1.setFieldName("testfield1");
        field1.setFieldType("string");
        field1.setFieldId(1101);
        field1.setSchemaName(schemaName);
        field1.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        Field field2 = new Field();
        field2.setFieldName("testfield2");
        field2.setFieldType("string");
        field2.setFieldId(1102);
        field2.setSchemaName(schemaName);
        field2.setMeasurementType(Measurement.METRIC.measurementTypeCode);
        
        FieldDAO fieldDAO = DAOFactory.fieldDAO();
        
        int fieldNum = fieldDAO.fetchAll(session).size();
        
        fieldDAO.save(session, field1);
        long id1 = field1.getFieldId();
        fieldDAO.save(session, field2);
        long id2 = field2.getFieldId();

        Assert.assertTrue(id1 != 0);
        Assert.assertTrue(id2 != 0);
                
        Assert.assertEquals(fieldDAO.fetchByCompositeKey(session, schemaName, id1).getFieldName(), "testfield1");
        Assert.assertEquals(fieldDAO.fetchByCompositeKey(session, schemaName, id2).getFieldName(), "testfield2");
        Assert.assertEquals(fieldDAO.fetchByCompositeKey(session, schemaName, id1).getMeasurementType(), Measurement.DIMENSION.measurementTypeCode);
        Assert.assertEquals(fieldDAO.fetchByCompositeKey(session, schemaName, id2).getMeasurementType(), Measurement.METRIC.measurementTypeCode);
        
        Assert.assertEquals(fieldDAO.fetchAll(session).size(), fieldNum + 2);
        
        Assert.assertEquals(fieldDAO.fetchByName(session, schemaName + Field.FIELD_NAME_SEPARATOR + "testfield1").getFieldId(), id1);
        Assert.assertEquals(fieldDAO.fetchByName(session, schemaName + Field.FIELD_NAME_SEPARATOR + "testfield2").getFieldId(), id2);
        
        fieldDAO.delete(session, field1);
        fieldDAO.delete(session, field2);
        
        Assert.assertEquals(fieldDAO.fetchAll(session).size(), fieldNum);
        
    }
    
    /**
     * Check that map columns have keys.
     */
    @Test
    public void testFieldKeys() {
        Field field1 = DAOFactory.fieldDAO().fetchByName(session, "schema1-geo_info");
        Assert.assertNotNull(field1.getFieldKeys());
        Assert.assertEquals(field1.getFieldKeys().size(), 3);
        Assert.assertEquals(field1.getFieldType(), "string");

        Field field2 = DAOFactory.fieldDAO().fetchByName(session, "schema1-debug_tag");
        DAOFactory.fieldDAO().fetchByCompositeKey(session, schemaName, field2.getFieldId());
        Assert.assertNotNull(field2.getFieldKeys());
        Assert.assertEquals(field2.getFieldKeys().size(), 2);
        Assert.assertEquals(field2.getFieldType(), "boolean");

        Field field3 = DAOFactory.fieldDAO().fetchByName(session, "schema1-cookie_one");
        DAOFactory.fieldDAO().fetchByCompositeKey(session, schemaName, field3.getFieldId());
        Assert.assertNotNull(field3.getFieldKeys());
        Assert.assertEquals(field3.getFieldKeys().size(), 0);
        Assert.assertEquals(field3.getFieldType(), "string");
        
        List<Field> allFields = DAOFactory.fieldDAO().fetchAll(session);
        Field someMap = null;
        for (Field field : allFields) {
            if (field.getFieldName().equals("geo_info") && field.getSchemaName().equals(schemaName)) {
                someMap = field;
                break;              
            }
        }
        Assert.assertNotNull(someMap);
        Assert.assertNotNull(someMap.getFieldKeys());
        Assert.assertEquals(someMap.getFieldKeys().size(), 3);  
    }

    /**
     * Check fetch field by composite key.
     */
    @Test
    public void testFetchByCompositeKey() {
        Field field1 = new Field();
        field1.setFieldName("testfield1");
        field1.setFieldType("string");
        field1.setFieldId(1101);
        field1.setSchemaName(schemaName);
        field1.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        FieldDAO fieldDAO = DAOFactory.fieldDAO();
        fieldDAO.save(session, field1);
        long id1 = field1.getFieldId();

        Assert.assertTrue(id1 != 0);
        Assert.assertEquals(fieldDAO.fetchByCompositeKey(session, schemaName, id1).getFieldName(), "testfield1");

        fieldDAO.delete(session, field1);
    }

    /**
     * Check fetch field by schema name and field name.
     */
    @Test
    public void testFetchBySchemaNameFieldName() {
        Field field1 = new Field();
        field1.setFieldName("testfield1");
        field1.setFieldType("string");
        field1.setFieldId(1101);
        field1.setSchemaName(schemaName);
        field1.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        FieldDAO fieldDAO = DAOFactory.fieldDAO();
        fieldDAO.save(session, field1);
        long id1 = field1.getFieldId();

        Assert.assertTrue(id1 != 0);
        Assert.assertEquals(fieldDAO.fetchByName(session, schemaName + Field.FIELD_NAME_SEPARATOR + "testfield1").getFieldId(), id1);

        fieldDAO.delete(session, field1);

    }

    /**
     * Check fetch fields by schema name.
     */
    @Test
    public void testFetchFieldsBySchemaName() {
        FieldDAO fieldDAO = DAOFactory.fieldDAO();
        List<Field> fields = fieldDAO.fetchBySchemaName(session, schemaName);

        Assert.assertEquals(fields.size(), 17);
    }

    /**
     * Check fetch field by composite name.
     */
    @Test
    public void testFetchByName() {
        Field field1 = DAOFactory.fieldDAO().fetchByName(session, "schema1-debug_tag");
        Assert.assertNotNull(field1.getFieldKeys());
        Assert.assertEquals(field1.getFieldKeys().size(), 2);
        Assert.assertEquals(field1.getFieldType(), "boolean");
    }
}
