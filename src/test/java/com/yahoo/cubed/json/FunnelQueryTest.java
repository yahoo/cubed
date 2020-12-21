/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.ConfigurationLoader;
import com.yahoo.cubed.templating.TemplateTestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test funnel query class.
 */
public class FunnelQueryTest {
    FunnelQuery tg;
    String query;

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
     * Test FunnelQuery with valid query string.
     */
    @Test
    public void testValidFunnelQueryString() throws Exception {
        query = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");;
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), null);
    }

    /**
     * Test FunnelQuery with invalid query string (missing user column).
     */
    @Test
    public void testInvalidFunnelQueryStringMissingUserColumn() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":42, \"key\":\"gender\", \"alias\":\"gender\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"25664825\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"782200249\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180301\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"stepNames\":[\"yahoo sports\",\"yahoo fantasy\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Missing user id column");
    }

    /**
     * Test FunnelQuery with invalid query string (only have one step).
     */
    @Test
    public void testInvalidFunnelQueryStringWithOneStep() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":42, \"key\":\"gender\", \"alias\":\"gender\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"25664825\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180301\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"userIdColumn\":\"bcookie\"," +
                "\"stepNames\":[\"yahoo sports\",\"yahoo fantasy\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Must have at least 2 steps");
    }

    /**
     * Test FunnelQuery with invalid query string (invalid step definition).
     */
    @Test
    public void testInvalidFunnelQueryStringWithInvalidStepDefinition() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":42, \"key\":\"gender\", \"alias\":\"gender\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"25664825\"}" +
                "]" +
                "}," +
                "{" +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"25664825\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"spaceid\",\"field\":\"spaceid\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"782200249\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}," +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}," +
                "{\"id\":\"pty_family\",\"field\":\"pty_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sports\"}," +
                "{\"id\":\"pty_device\",\"field\":\"pty_device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"desktop\"}," +
                "{\"id\":\"pty_experience\",\"field\":\"pty_experience\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"web\"}," +
                "{\"id\":\"event_family\",\"field\":\"event_family\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"view\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180301\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"userIdColumn\":\"bcookie\"," +
                "\"stepNames\":[\"yahoo sports\",\"yahoo fantasy\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "filter must have junction operation at the top level.");
    }

    /**
     * Test FunnelQuery with invalid query string (invalid projections).
     */
    @Test
    public void testInvalidFunnelQueryStringWithInvalidProjections() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}, {\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"2566\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"7822\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"network_status\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180301\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"userIdColumn\":\"cookie_one\"," +
                "\"stepNames\":[\"step1\",\"step2\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Duplicate column alias:city");
    }

    /**
     * Test FunnelQuery with invalid query string (missing start date).
     */
    @Test
    public void testInvalidFunnelQueryStringMissingStartDate() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"2566\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"7822\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}" +
                "]" +
                "}," +
                "\"startDate\":\"\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"userIdColumn\":\"cookie_one\"," +
                "\"stepNames\":[\"step1\",\"step2\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Missing start time date");
    }

    /**
     * Test FunnelQuery with invalid query string (missing end date).
     */
    @Test
    public void testInvalidFunnelQueryStringMissingEndDate() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"2566\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"7822\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"network_status\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180301\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"userIdColumn\":\"cookie_one\"," +
                "\"stepNames\":[\"step1\",\"step1\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Missing end time date");
    }

    /**
     * Test FunnelQuery with invalid query string (invalid start date).
     */
    @Test
    public void testInvalidFunnelQueryStringWithInvalidStartDate() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"2566\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"7822\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180311\"," +
                "\"queryRange\":\"1\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180302\"," +
                "\"userIdColumn\":\"cookie_one\"," +
                "\"stepNames\":[\"step1\",\"step2\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Start date must be before end date");
    }

    /**
     * Test FunnelQuery with invalid query string (invalid query range).
     */
    @Test
    public void testInvalidFunnelQueryStringWithInvalidQueryRange() throws Exception {
        query = "{" +
                "\"name\":\"test\"," +
                "\"description\":\"test\"," +
                "\"owner\":\"test\"," +
                "\"projections\":[{\"column_id\":4, \"key\":\"city\", \"alias\":\"city\", \"schema_name\":\"schema1\"}]," +
                "\"steps\":[" +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"2566\"}" +
                "]" +
                "}," +
                "{" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"content_id\",\"field\":\"content_id\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"7822\"}" +
                "]" +
                "}" +
                "]," +
                "\"filter\": {" +
                "\"condition\":\"AND\"," +
                "\"rules\":[" +
                "{\"id\":\"network\",\"field\":\"network\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"}" +
                "]" +
                "}," +
                "\"startDate\":\"20180201\"," +
                "\"queryRange\":\"32\"," +
                "\"repeatInterval\":\"1\"," +
                "\"endDate\":\"20180304\"," +
                "\"userIdColumn\":\"cookie_one\"," +
                "\"stepNames\":[\"step1\",\"step2\"]" +
                "}";
        ObjectMapper mapper = new ObjectMapper();
        tg = mapper.readValue(query, NewFunnelQuery.class);
        Assert.assertEquals(tg.isValid(), "Queries can only run for 30 days at most");
    }
}
