/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Unit test for SchemaReader.
 */
public class SchemaReaderTest {
    /**
     * Test succussful read of schema file.
     */
    @Test
    public void testSchemaReadSuccess() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/valid_sample_schema.yaml");
        Assert.assertNotNull(schemas);
        Assert.assertEquals(schemas.getSchemas().size(), 1);
        Assert.assertEquals(schemas.getSchemas().get(0).getName(), "alpha");
        Assert.assertEquals(schemas.getSchemas().get(0).getDatabase(), "test");
        Assert.assertEquals(schemas.getSchemas().get(0).getTables().size(), 4);
        Assert.assertEquals(schemas.getSchemas().get(0).getFields().size(), 3);
    }

    /**
     * Test read of a schema file with no database name specified.
     */
    @Test(expectedExceptions = Exception.class)
    public void testSchemaReadNoDatabase() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_schema_no_database.yaml");
    }

    /**
     * Test read of a schema file with no ids specified.
     */
    @Test(expectedExceptions = Exception.class)
    public void testSchemaReadNoIds() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_schema_no_ids.yaml");
    }

    /**
     * Test read of a schema file with no table specified.
     */
    @Test(expectedExceptions = Exception.class)
    public void testNoTableSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_schema_no_table_specified.yaml");
    }

    /**
     * Test read of a schema file with duplicated schema names.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateSchemaNameSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_sample_schema_duplicate_schema_name.yaml");
    }

    /**
     * Test read of a schema file with duplicated field name.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateFieldNameSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_sample_schema_duplicate_field_name.yaml");
    }

    /**
     * Test read of a schema file with duplicated field IDs.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateFieldIdSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_sample_schema_duplicate_field_id.yaml");
    }

    /**
     * Test read of a schema file with duplicated key name.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateKeyNameSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_sample_schema_duplicate_key_name.yaml");
    }

    /**
     * Test read of a schema file with duplicated key IDs.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateKeyIdSchemaRead() throws Exception {
        Schemas schemas = SchemaReader.readSchema("src/test/resources/invalid_sample_schema_duplicate_key_id.yaml");
    }
}
