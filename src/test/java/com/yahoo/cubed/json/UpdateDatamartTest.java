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
 * Test update datamart class.
 */
public class UpdateDatamartTest {
    UpdateDatamart tg;
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
     * Test UpdateDatamart with invalid query string (missing data).
     */
    @Test
    public void testInvalidUpdateDatamartMissingDescription() throws Exception {
        req = "{" +
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
                "\"endTimeEnabled\":false," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing description");
    }

    /**
     * Test UpdateDatamart with invalid query string (missing owner).
     */
    @Test
    public void testInvalidUpdateDatamartMissingOwner() throws Exception {
        req = "{" +
                "\"description\":\"test\"," +
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
                "\"endTimeEnabled\":false," +
                "\"endTimeDate\":null}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing owner");
    }

    /**
     * Test UpdateDatamart with invalid query string (invalid filter).
     */
    @Test
    public void testInvalidUpdateDatamartWithInvalidFilter() throws Exception {
        req = "{" +
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
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "filter must have junction operation at the top level.");
    }

    /**
     * Test UpdateDatamart with invalid query string (invalid backfill).
     */
    @Test
    public void testInvalidUpdateDatamartWithInvalidBackfill() throws Exception {
        req = "{" +
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
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing backfill start date");
    }

    /**
     * Test UpdateDatamart with invalid query string (missing endDate).
     */
    @Test
    public void testInvalidUpdateDatamartMissingEndDate() throws Exception {
        req = "{" +
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
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Missing end time date");
    }

    /**
     * Test UpdateDatamart with invalid query string (invalid format).
     */
    @Test
    public void testInvalidUpdateDatamartInvalidVMFormat() throws Exception {
        req = "{" +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[[\"testv\"]]]," +
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
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Wrong format of value mapping");
    }

    /**
     * Test UpdateDatamart with invalid query string (ambiguous VM).
     */
    @Test
    public void testInvalidUpdateDatamartAmbiguousVM() throws Exception {
        req = "{" +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":\"9\",\"key\":\"\",\"alias\":\"bcookie\",\"aggregate\":\"NONE\", \"schema_name\":\"schema1\"}]," +
                "\"projectionVMs\":[[[\"testv\", \"testa\"], [\"testv\", \"testb\"]]]," +
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
        tg = mapper.readValue(req, UpdateDatamart.class);
        Assert.assertEquals(tg.isValid(), "Ambiguous value mapping for value:testv");
    }

}
