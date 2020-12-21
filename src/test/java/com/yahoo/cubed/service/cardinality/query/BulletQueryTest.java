/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.query;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilterLogicalRule;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilterRelationalRule;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.util.Aggregation;
import com.yahoo.cubed.util.Measurement;
import java.util.ArrayList;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Bullet query.
 */
public class BulletQueryTest {
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
     * Test happy path bullet query.
     */
    @Test
    public void testHappyPath() {
        Field field1 = new Field();
        field1.setFieldId(1L);
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setMeasurementType(Measurement.METRIC.measurementTypeCode);

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("F1");
        projection1.setAggregation(Aggregation.COUNT);

        Field field2 = new Field();
        field2.setFieldId(2L);
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("F2");
        projection2.setKey("_KEY");

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection1);
        projections.add(projection2);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("pipeline");
        pipeline.setPipelineDescription("pipeline description");
        pipeline.setProjections(projections);
        pipeline.setPipelineFilterJson("{\"condition\":\"AND\",\"rules\":[{\"id\":\"price\",\"field\":\"price\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"less\",\"value\":\"10.25\"},{\"condition\":\"OR\",\"rules\":[{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"2\"},{\"id\":\"category\",\"field\":\"category\",\"type\":\"integer\",\"input\":\"select\",\"operator\":\"equal\",\"value\":\"1\",\"subfield\":[\"_A\"]},{\"id\":\"filter\",\"field\":\"filter\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null}]}]}");

        BulletQuery query = BulletQuery.createBulletQueryInstance(pipeline);

        // check aggregation
        Assert.assertEquals(query.aggregation.type, BulletQuery.AGGREGATION_TYPE);
        Assert.assertEquals(query.aggregation.fields.size(), 1);
        Assert.assertTrue(query.aggregation.fields.containsKey("field2.\"_KEY\""));

        // check filter
        Assert.assertEquals(query.filters.size(), 1);
        BulletQueryFilter filter = query.filters.get(0);
        Assert.assertEquals(filter.operation, "AND");

        Assert.assertTrue(filter instanceof BulletQueryFilterLogicalRule);
        BulletQueryFilterLogicalRule logical = (BulletQueryFilterLogicalRule) filter;
        Assert.assertEquals(logical.clauses.size(), 2);

        BulletQueryFilter subfilter1 = logical.clauses.get(0);
        BulletQueryFilter subfilter2 = logical.clauses.get(1);

        Assert.assertTrue(subfilter1 instanceof BulletQueryFilterRelationalRule);
        BulletQueryFilterRelationalRule relational = (BulletQueryFilterRelationalRule) subfilter1;
        Assert.assertEquals(relational.field, "price");
        Assert.assertEquals(relational.operation, "<");
        Assert.assertEquals(relational.values.size(), 1);
        Assert.assertEquals(relational.values.get(0), "10.25");

        Assert.assertEquals(subfilter2.operation, "OR");
        Assert.assertTrue(subfilter2 instanceof BulletQueryFilterLogicalRule);
        BulletQueryFilterLogicalRule logical2 = (BulletQueryFilterLogicalRule) subfilter2;
        Assert.assertEquals(logical2.clauses.size(), 3);
        BulletQueryFilterRelationalRule relational1 = (BulletQueryFilterRelationalRule) logical2.clauses.get(0);
        BulletQueryFilterRelationalRule relational2 = (BulletQueryFilterRelationalRule) logical2.clauses.get(1);
        BulletQueryFilterRelationalRule relational3 = (BulletQueryFilterRelationalRule) logical2.clauses.get(2);

        Assert.assertEquals(relational1.field, "category");
        Assert.assertEquals(relational1.operation, "==");
        Assert.assertEquals(relational1.values.size(), 1);
        Assert.assertEquals(relational1.values.get(0), "2");

        Assert.assertEquals(relational2.field, "category._A");
        Assert.assertEquals(relational2.operation, "==");
        Assert.assertEquals(relational2.values.size(), 1);
        Assert.assertEquals(relational2.values.get(0), "1");

        Assert.assertEquals(relational3.field, "filter");
        Assert.assertEquals(relational3.operation, "!=");
        Assert.assertEquals(relational3.values.size(), 1);
        Assert.assertEquals(relational3.values.get(0), "NULL");
    }

    /**
     * Test the deletion of "filter_tag == null".
     */
    @Test
    public void testFilterTagIsNull() {
        Field field1 = new Field();
        field1.setFieldId(1L);
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setMeasurementType(Measurement.METRIC.measurementTypeCode);

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("F1");
        projection1.setAggregation(Aggregation.COUNT);

        Field field2 = new Field();
        field2.setFieldId(2L);
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("F2");
        projection2.setKey("_KEY");

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection1);
        projections.add(projection2);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("pipeline");
        pipeline.setPipelineDescription("pipeline description");
        pipeline.setProjections(projections);
        pipeline.setPipelineFilterJson("{\"condition\":\"AND\",\"rules\":[{\"id\":\"price\",\"field\":\"price\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"less\",\"value\":\"10.25\"},{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"is_null\",\"value\":null}]}");

        BulletQuery query = BulletQuery.createBulletQueryInstance(pipeline);

        // check filter
        Assert.assertEquals(query.filters.size(), 1);
        BulletQueryFilter filter = query.filters.get(0);
        Assert.assertEquals(filter.operation, "AND");

        // deleted filter_tag == null
        Assert.assertTrue(filter instanceof BulletQueryFilterLogicalRule);
        BulletQueryFilterLogicalRule logical = (BulletQueryFilterLogicalRule) filter;
        Assert.assertEquals(logical.clauses.size(), 1);

        BulletQueryFilter subfilter1 = logical.clauses.get(0);

        Assert.assertTrue(subfilter1 instanceof BulletQueryFilterRelationalRule);
        BulletQueryFilterRelationalRule relational = (BulletQueryFilterRelationalRule) subfilter1;
        Assert.assertEquals(relational.field, "price");
        Assert.assertEquals(relational.operation, "<");
        Assert.assertEquals(relational.values.size(), 1);
        Assert.assertEquals(relational.values.get(0), "10.25");
    }
}
