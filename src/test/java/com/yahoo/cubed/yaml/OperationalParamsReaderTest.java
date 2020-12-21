/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import org.testng.Assert;
import org.testng.annotations.Test;
import org.testng.annotations.BeforeClass;
import com.yahoo.cubed.settings.CLISettings;

/**
 * Unit test for OperationalParamsreader.
 */
public class OperationalParamsReaderTest {
    /**
     * This test depends on schema file, so first set schema directory.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.SCHEMA_FILES_DIR = "src/test/resources/schemas/";
    }

    /**
     * Test succussful read of operational params file.
     */
    @Test
    public void testOperationalParamsReadSuccess() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/schemas/schema1/test_operational_params.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
        Assert.assertNotNull(operationalParams);
        Assert.assertEquals(operationalParams.getFunnelTargetTable(), "daily_data");
        Assert.assertEquals(operationalParams.getUserIdFields().size(), 2);

        // funnel disabled
        operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/valid_operational_params_funnel_disabled.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
        Assert.assertTrue(operationalParams.isDisableFunnel());
    }

    /**
     * Test read of a operational params file with target table name not specified in schema.
     */
    @Test(expectedExceptions = Exception.class)
    public void testInvalidTableOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_wrong_table.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with duplicated userIdField names.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateUserIdFieldOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_duplicate_userIdField.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with userIdField not defined in schema.
     */
    @Test(expectedExceptions = Exception.class)
    public void testInvalidUserIdFieldOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_wrong_userIdField.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with empty filter id fields.
     */
    @Test(expectedExceptions = Exception.class)
    public void testNoIdDefaultFiltersOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_no_filter_field_id.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with duplicate filter id fields.
     */
    @Test(expectedExceptions = Exception.class)
    public void testDuplicateDefaultFiltersOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_duplicate_filter_field_id.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with filter id fields not defined in schema.
     */
    @Test(expectedExceptions = Exception.class)
    public void testInvalidDefaultFiltersOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_wrong_filter_field_id.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file without timestamp column in schema.
     */
    @Test(expectedExceptions = Exception.class)
    public void testNoTimestampOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_no_timestamp.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with invalid timestamp column in schema.
     */
    @Test(expectedExceptions = Exception.class)
    public void testInvalidTimestampOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_invalid_timestamp.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }

    /**
     * Test read of a operational params file with funnel enabled but no target table specified.
     */
    @Test(expectedExceptions = Exception.class)
    public void testNoTargetTableOperationalParamsRead() throws Exception {
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams("src/test/resources/invalid_operational_params_no_target_table.yaml", "src/test/resources/schemas/schema1/test_schema.yaml");
    }
}
