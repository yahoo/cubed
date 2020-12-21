/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.json.filter.LogicalRule;
import com.yahoo.cubed.json.filter.RelationalRule;

/**
 * Filter tests.
 */
public class FilterTest {
    
    /**
     * Test filter deserialization from JSON.
     */
    @Test
    public void filterDeserializeTest() throws JsonParseException, JsonMappingException, IOException {
        String json = "{\"condition\":\"AND\",\"rules\":[{\"id\":\"price\",\"field\":\"price\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"less\",\"value\":\"10.25\"},{\"condition\":\"OR\",\"rules\":[{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"2\"},{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"1\",\"subfield\":[\"_A\"]}]}]}";
        
        LogicalRule filter = (LogicalRule) Filter.fromJson(json);
        
        Assert.assertEquals(filter.getCondition(), "AND");
        Assert.assertEquals(filter.getRules().size(), 2);
        LogicalRule nestedLogical = null;
        RelationalRule nestedRelational = null;
        for (Filter rule : filter.getRules()) {
            if (rule instanceof LogicalRule) {
                nestedLogical = (LogicalRule) rule;
            } else if (rule instanceof RelationalRule) {
                nestedRelational = (RelationalRule) rule;
            }
        }
        Assert.assertNotNull(nestedLogical);
        Assert.assertNotNull(nestedRelational);
        
        Assert.assertEquals(nestedLogical.getCondition(), "OR");
        Assert.assertEquals(nestedLogical.getRules().size(), 2);
        
        Assert.assertEquals(nestedRelational.getField(), "price");
        Assert.assertEquals(nestedRelational.getOperator(), "less");

    }

    /**
     * Test filter deserialization from JSON with logical rule name.
     */
    @Test
    public void filterDeserializeWithNameTest() throws JsonParseException, JsonMappingException, IOException {
        String json = "{\"name\":\"name_1\",\"condition\":\"AND\",\"rules\":[{\"id\":\"price\",\"field\":\"price\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"less\",\"value\":\"10.25\"},{\"name\":\"name_2\",\"condition\":\"OR\",\"rules\":[{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"2\"},{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"1\",\"subfield\":[\"_A\"]}]}]}";

        LogicalRule filter = (LogicalRule) Filter.fromJson(json);

        Assert.assertEquals(filter.getName(), "name_1");
        Assert.assertEquals(filter.getCondition(), "AND");
        Assert.assertEquals(filter.getRules().size(), 2);
        LogicalRule nestedLogical = null;
        RelationalRule nestedRelational = null;
        for (Filter rule : filter.getRules()) {
            if (rule instanceof LogicalRule) {
                nestedLogical = (LogicalRule) rule;
            } else if (rule instanceof RelationalRule) {
                nestedRelational = (RelationalRule) rule;
            }
        }
        Assert.assertNotNull(nestedLogical);
        Assert.assertNotNull(nestedRelational);

        Assert.assertEquals(nestedLogical.getName(), "name_2");
        Assert.assertEquals(nestedLogical.getCondition(), "OR");
        Assert.assertEquals(nestedLogical.getRules().size(), 2);

        Assert.assertEquals(nestedRelational.getField(), "price");
        Assert.assertEquals(nestedRelational.getOperator(), "less");

    }
    
    /**
     * Test filter serialization to JSON.
     */
    @Test
    public void filterSerializeTest() throws Exception {
        RelationalRule filter1 = new RelationalRule();
        filter1.setId("1");
        filter1.setField("fd1");
        filter1.setOperator("less");
        filter1.setValue("1000");

        RelationalRule filter2 = new RelationalRule();
        filter2.setId("2");
        filter2.setField("fd2");
        filter2.setOperator("is_not_null");

        List<Filter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);
        
        LogicalRule filter3 = new LogicalRule();
        filter3.setCondition("AND");
        filter3.setRules(filters);
        
        String expected = "{\"name\":null,\"condition\":\"AND\",\"rules\":[{\"id\":\"1\",\"field\":\"fd1\",\"type\":null,\"input\":null,\"operator\":\"less\",\"value\":\"1000\",\"subfield\":null},{\"id\":\"2\",\"field\":\"fd2\",\"type\":null,\"input\":null,\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}";

        Assert.assertEquals(Filter.toJson(filter3), expected);

    }

    /**
     * Test filter serialization to JSON with logical rule name.
     */
    @Test
    public void filterSerializeWithNameTest() throws Exception {
        RelationalRule filter1 = new RelationalRule();
        filter1.setId("1");
        filter1.setField("fd1");
        filter1.setOperator("less");
        filter1.setValue("1000");

        RelationalRule filter2 = new RelationalRule();
        filter2.setId("2");
        filter2.setField("fd2");
        filter2.setOperator("is_not_null");

        List<Filter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);

        LogicalRule filter3 = new LogicalRule();
        filter3.setName("name_1");
        filter3.setCondition("AND");
        filter3.setRules(filters);

        String expected = "{\"name\":\"name_1\",\"condition\":\"AND\",\"rules\":[{\"id\":\"1\",\"field\":\"fd1\",\"type\":null,\"input\":null,\"operator\":\"less\",\"value\":\"1000\",\"subfield\":null},{\"id\":\"2\",\"field\":\"fd2\",\"type\":null,\"input\":null,\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}";

        Assert.assertEquals(Filter.toJson(filter3), expected);

    }

    /**
     * Test filter serialization to JSON with null value.
     */
    @Test
    public void filterToJsonWithNullTest() throws Exception {
        Assert.assertNull(Filter.toJson(null));
    }

    /**
     * Test filter de-serialization to JSON with null value.
     */
    @Test
    public void filterFromJsonWithNullTest() throws Exception {
        Assert.assertNull(Filter.fromJson(null));
    }

}
