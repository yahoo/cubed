/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.filter.PipelineFilter;
import com.yahoo.cubed.model.filter.PipelineLogicalRule;
import com.yahoo.cubed.model.filter.PipelineRelationalRule;
import com.yahoo.cubed.util.Aggregation;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test Druid index json generation.
 */
public class DruidIndexJsonTest {
    DruidIndexJson tg;

    /**
     * Create a new Druid index.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new DruidIndexJson(true);
    }

    /**
     * Test generation of Druid index json file.
     */
    @Test
    public void testDruidIndexJson() throws Exception {
        long fid1 = 1L;
        Field field1 = new Field();
        field1.setFieldId(fid1);
        field1.setFieldName("newfield1");
        field1.setFieldType("string");

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("newfieldalias1");

        long fid2 = 2L;
        Field field2 = new Field();
        field2.setFieldId(fid2);
        field2.setFieldName("newfield2");
        field2.setFieldType("int");

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("newfieldalias2");
        projection2.setKey("_fieldkey2");

        long fid3 = 2L;
        Field field3 = new Field();
        field3.setFieldId(fid3);
        field3.setFieldName("newfield3");
        field3.setFieldType("int");

        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field3);
        projection3.setAlias("newfieldalias3");
        projection3.setKey("_fieldkey3");
        projection3.setAggregation(Aggregation.COUNT_DISTINCT);

        long fid4 = 4L;
        Field field4 = new Field();
        field4.setFieldId(fid4);
        field4.setFieldName("newfield4");
        field4.setFieldType("int");

        PipelineProjection projection4 = new PipelineProjection();
        projection4.setField(field4);
        projection4.setAlias("newfieldalias4");
        projection4.setKey("_fieldkey4");
        projection4.setAggregation(Aggregation.MIN);

        long fid5 = 5L;
        Field field5 = new Field();
        field5.setFieldId(fid5);
        field5.setFieldName("newfield5");
        field5.setFieldType("binary");

        PipelineProjection projection5 = new PipelineProjection();
        projection5.setField(field5);
        projection5.setAlias("newfieldalias5");
        projection5.setKey("_fieldkey5");
        projection5.setAggregation(Aggregation.THETA_SKETCH);

        PipelineRelationalRule filter1 = new PipelineRelationalRule();
        filter1.setField(field1);
        filter1.setOperator("greater");
        filter1.setValue("3");

        PipelineRelationalRule filter2 = new PipelineRelationalRule();
        filter2.setField(field2);
        filter2.setKey("_fieldkey2");
        filter2.setOperator("not_equal");
        filter2.setValue("NULL");

        List<PipelineFilter> filters = new ArrayList<>();
        filters.add(filter1);
        filters.add(filter2);

        PipelineLogicalRule filter3 = new PipelineLogicalRule();
        filter3.setCondition("OR");
        filter3.setRules(filters);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection1);
        projections.add(projection2);
        projections.add(projection3);
        projections.add(projection4);
        projections.add(projection5);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setProjections(projections);
        pipeline.setPipelineFilterObject(filter3);

        long version = System.currentTimeMillis();
        String template = tg.generateFile(pipeline, version);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/druid_index.json");

        Assert.assertEquals(template + "\n", expected);
    }
}
