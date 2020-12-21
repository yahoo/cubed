/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.ConfigurationLoader;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test new datamart class.
 */
public class NewDataMartTest {
    NewDatamart tg;
    String req;

    /**
     * Setup database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
        ConfigurationLoader.load();
    }

    /**
     * Test NewDatamart with invalid query string (duplicate projections).
     */
    @Test
    public void testInvalidNewDataMartWithDuplicateProjections() throws Exception {
        req = "{" +
                "\"name\":\"test_datamart\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}, {\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[]]," +
                "\"filter\":{" +
                    "\"condition\":\"AND\"," +
                    "\"rules\":[" +
                        "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                        "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                        "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                        "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                        "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                        "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]}," +
                "\"backfillEnabled\":true," +
                "\"backfillStartDate\":\"2018-03-13\"," +
                "\"endTimeEnabled\":false," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, NewDatamart.class);
        Assert.assertEquals(tg.isValid(), "Duplicate column alias:bcookie");
    }

    /**
     * Test NewDatamart with invalid query string (invalid filter).
     */
    @Test
    public void testInvalidNewDataMartWithInvalidFilter() throws Exception {
        req = "{" +
                "\"name\":\"test_datamart\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[]]," +
                "\"filter\":{" +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]}," +
                "\"backfillEnabled\":true," +
                "\"backfillStartDate\":\"2018-03-13\"," +
                "\"endTimeEnabled\":false," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, NewDatamart.class);
        Assert.assertEquals(tg.isValid(), "filter must have junction operation at the top level.");
    }

    /**
     * Test NewDatamart with invalid query string (invalid backfill).
     */
    @Test
    public void testInvalidNewDataMartWithInvalidBackfill() throws Exception {
        req = "{" +
                "\"name\":\"test_datamart\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[]]," +
                "\"filter\":{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]}," +
                "\"backfillEnabled\":true," +
                "\"endTimeEnabled\":false," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, NewDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing backfill start date");
    }

    /**
     * Test NewDatamart with invalid query string (missing endDate).
     */
    @Test
    public void testInvalidNewDataMartMissingEndDate() throws Exception {
        req = "{" +
                "\"name\":\"test_datamart\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[]]," +
                "\"filter\":{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]}," +
                "\"backfillEnabled\":true," +
                "\"backfillStartDate\":\"2018-03-13\"," +
                "\"endTimeEnabled\":true," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, NewDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing end time date");
    }

}

