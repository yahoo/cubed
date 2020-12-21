/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed;

import com.yahoo.cubed.dao.DAOFactory;
import com.yahoo.cubed.dao.SchemaDAO;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;
import com.yahoo.cubed.model.Field;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.hibernate.Session;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import com.yahoo.cubed.settings.CLISettings;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.Arrays;

/**
 * Unit test for simple App.
 */
public class AppTest {
    private static Session session;

    // all pre-defined data types
    private static String[] dataTypes = {"integer", "boolean", "string"};
    private static Set<String> allowedDataType = new HashSet<>(Arrays.asList(dataTypes));

    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        session = HibernateSessionFactoryManager.getSessionFactory().openSession();
    }

    /**
     * Test loading a custom schema.
     */
    @Test
    public void testLoadCustomSchema() throws Exception {
        App.loadSchemas("src/test/resources/schemas/");
        // Test read operational params.
        SchemaDAO schemaDAO = DAOFactory.schemaDAO();
        List<Schema> schemas = schemaDAO.fetchAll(session);

        Assert.assertEquals(schemas.size(), 4);

        Schema schema = ServiceFactory.schemaService().fetchByName("schema1");
        List<String> schemaUserIdFields = (new ObjectMapper()).readValue(schema.getSchemaUserIdFields(), List.class);
        Assert.assertEquals(schemaUserIdFields.size(), 2);
        Assert.assertEquals(schema.getSchemaTargetTable(), "daily_data");

        // Check that there are the correct number of fields in the database
        List<Field> allFields = schema.getFields();
        int fieldNum = allFields.size();
        Assert.assertEquals(fieldNum, 17);

        schema = ServiceFactory.schemaService().fetchByName("schema3");
        schemaUserIdFields = (new ObjectMapper()).readValue(schema.getSchemaUserIdFields(), List.class);
        Assert.assertNull(schemaUserIdFields);
        Assert.assertEquals(schema.getSchemaDisableBullet().booleanValue(), true);
        Assert.assertEquals(schema.getSchemaDisableFunnel().booleanValue(), true);
        Assert.assertEquals(schema.getFields().size(), 3);
    }

    /**
     * Test updating a schema's operational params.
     * @throws Exception
     */
    @Test
    public void testUpdateSchemaOperationalParams() throws Exception {
        // Load original info
        App.loadSchemas("src/test/resources/schemas/");
        Schema schema1 = ServiceFactory.schemaService().fetchByName("schema1");
        Schema schema2 = ServiceFactory.schemaService().fetchByName("schema2");
        Schema schema3 = ServiceFactory.schemaService().fetchByName("schema3");
        Assert.assertEquals(schema1.getSchemaDefaultFilters(), "[{\"id\":\"user_logged_in\",\"operator\":\"equal\",\"value\":\"1\"},{\"id\":\"browser\",\"operator\":\"equal\",\"value\":\"browser1\"},{\"id\":\"debug_tag\",\"operator\":\"is_null\"}]");
        Assert.assertEquals(schema2.getSchemaDefaultFilters(), "[{\"id\":\"filter\",\"operator\":\"is_null\"}]");
        Assert.assertFalse(schema1.getIsSchemaDeleted());
        Assert.assertFalse(schema2.getIsSchemaDeleted());
        Assert.assertFalse(schema3.getIsSchemaDeleted());

        // Update
        App.loadSchemas("src/test/resources/updated_schemas/");
        schema1 = ServiceFactory.schemaService().fetchByName("schema1");
        schema2 = ServiceFactory.schemaService().fetchByName("schema2");
        schema3 = ServiceFactory.schemaService().fetchByName("schema3");
        Assert.assertEquals(schema1.getSchemaDefaultFilters(), "[{\"id\":\"user_logged_in\",\"operator\":\"equal\",\"value\":\"1\"},{\"id\":\"browser\",\"operator\":\"equal\",\"value\":\"browser1\"}]");
        Assert.assertEquals(schema2.getSchemaDefaultFilters(), "[{\"id\":\"filter\",\"operator\":\"is_null\"},{\"id\":\"cookie_version\",\"operator\":\"equal\",\"value\":\"1\"}]");
        Assert.assertFalse(schema1.getIsSchemaDeleted());
        Assert.assertFalse(schema2.getIsSchemaDeleted());
        Assert.assertTrue(schema3.getIsSchemaDeleted());
    }

    /**
     * Test loading a file rather than directory.
     */
    @Test(expectedExceptions = { IllegalArgumentException.class })
    public void testLoadCustomSchemaWithIllegalPath() throws Exception {
        // The argument is the path for a file
        App.loadSchemas("src/test/resources/schemas/testfile");
    }

    /**
     * Test run the app.
     */
    @Test
    public void mainTest() throws Exception {
        String[] args = {
            "--version", "0.0.0",
            "--schema-files-dir", "src/test/resources/schemas/" ,
            "--db-config-file", "src/test/resources/database-configuration.properties"
        };
        File templateOutputFolder = new File("/tmp/funnelmart_test");
        if (!templateOutputFolder.exists()) {
            templateOutputFolder.mkdir();
        } else {
            FileUtils.deleteDirectory(templateOutputFolder);
            templateOutputFolder.mkdir();
        }
        try {
            App.main(args);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
    }

    /**
     * Check if the field type is one of the pre-defined types.
     */
    private String fieldTypeValidityCheck(String fieldType) throws Exception {
        if (!fieldType.equals("") && !allowedDataType.contains(fieldType)) {
            return "Type " + fieldType + " is not valid; ";
        }
        return "";
    }

    /**
     * Check if the field type meets expectation.
     */
    private String fieldDataTypeCheck(String actualFieldType, String expectedFieldType) throws Exception {
        if (!expectedFieldType.equals("") && !actualFieldType.equals(expectedFieldType)) {
            return "Expected " + expectedFieldType + " type, but get " + actualFieldType + " type; ";
        }
        return "";
    }

    /**
     * Check if the number of subfields meets expectation.
     */
    private String subfieldNumCheck(int actualSubfieldNum, int expectedSubfieldNum) throws Exception {
        if (expectedSubfieldNum > 0 && actualSubfieldNum != expectedSubfieldNum) {
            return "Expected " + expectedSubfieldNum + " subfields, but get " + actualSubfieldNum + " subfields; ";
        }
        return "";
    }

    /**
     * Check if certain subfields exist under their parent fields.
     */
    private String subfieldMemberCheck(String[] actualFields, List<String> expectedFields) throws Exception {
        if (!expectedFields.equals("")) {
            StringBuilder invalidFields = new StringBuilder();
            Set<String> expectedFieldNames = new HashSet<>(expectedFields);
            for (String f: actualFields) {
                if (!expectedFieldNames.contains(f)) {
                    invalidFields.append(f + ", ");
                }
            }
            if (invalidFields.length() != 0) {
                return "Subfields not exist: " + invalidFields.toString() + "; ";
            }
        }
        return "";
    }

    /**
     * Check if the field exists.
     */
    private String fieldExistCheck(String fieldName) throws Exception {
        if (!fieldName.equals("")) {
            return "Field not exist; ";
        }
        return "";
    }

}
