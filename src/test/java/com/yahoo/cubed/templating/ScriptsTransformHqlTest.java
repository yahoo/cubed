/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
//import com.yahoo.cubed.model.filter.PipelineFilter;
import com.yahoo.cubed.model.filter.PipelineLogicalRule;
import com.yahoo.cubed.model.filter.PipelineRelationalRule;
import com.yahoo.cubed.util.Aggregation;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test Hive ETL script generator.
 */
public class ScriptsTransformHqlTest {
    ScriptsTransformHql tg;

    /**
     * Init Hive ETL script generator.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new ScriptsTransformHql();
    }

    /**
     * Test generation of Hive ETL script.
     */
    @Test
    public void testGenerateHiveTransformHqlTemplate() throws Exception {
        // Field
        long fid1 = 1L;
        Field field1 = new Field();
        field1.setFieldId(fid1);
        field1.setFieldName("string_field_1");
        field1.setFieldType("string");

        // Project field
        PipelineProjection projection1 = new PipelineProjection();
        projection1.setAlias("alpha");
        projection1.setField(field1);

        // Field
        long fid2 = 2L;
        Field field2 = new Field();
        field2.setFieldId(fid2);
        field2.setFieldName("bool_map_field_1");
        field2.setFieldType("map<string,boolean>");

        // Project field
        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("beta");
        projection2.setKey("key_1");

        // Field
        long fid3 = 3L;
        Field field3 = new Field();
        field3.setFieldId(fid3);
        field3.setFieldName("string_map_field_1");
        field3.setFieldType("map<string,string>");

        // Add projection
        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field3);
        projection3.setAlias("gamma");
        projection3.setKey("_fieldkey3");

        // Field
        long fid4 = 4L;
        Field field4 = new Field();
        field4.setFieldId(fid4);
        field4.setFieldName("string_map_field_2");
        field4.setFieldType("map<string,string>");

        // Field
        long fid5 = 5L;
        Field field5 = new Field();
        field5.setFieldId(fid5);
        field5.setFieldName("string_field_2");
        field5.setFieldType("string");

        // Field
        long fid6 = 6L;
        Field field6 = new Field();
        field6.setFieldId(fid6);
        field6.setFieldName("bool_map_field_6");
        field6.setFieldType("map<string,boolean>");

        // Field
        long fid7 = 7L;
        Field field7 = new Field();
        field7.setFieldId(fid7);
        field7.setFieldName("integer_column");
        field7.setFieldType("string");

        // Project field (to test sum projection)
        PipelineProjection projection4 = new PipelineProjection();
        projection4.setField(field7);
        projection4.setAlias("sum_test");
        projection4.setAggregation(Aggregation.SUM);

        // Add projection
        PipelineProjection projection5 = new PipelineProjection();
        projection5.setField(field5);
        projection5.setAlias("epsilon");
        projection5.setAggregation(Aggregation.THETA_SKETCH);

        // Filters
        PipelineRelationalRule filter1 = new PipelineRelationalRule();
        filter1.setField(field1);
        filter1.setOperator("greater");
        filter1.setValue("3");

        PipelineRelationalRule filter2 = new PipelineRelationalRule();
        filter2.setField(field2);
        filter2.setKey("_fieldkey2");
        filter2.setOperator("not_equal");
        filter2.setValue("0");

        PipelineRelationalRule filter3 = new PipelineRelationalRule();
        filter3.setField(field3);
        filter3.setOperator("is_not_null");
        filter3.setValue("NULL");

        PipelineRelationalRule filter4 = new PipelineRelationalRule();
        filter4.setField(field3);
        filter4.setOperator("equal");
        filter4.setValue("abc");

        PipelineRelationalRule filter5 = new PipelineRelationalRule();
        filter5.setField(field4);
        filter5.setKey("_filterkey5");
        filter5.setOperator("not_equal");
        filter5.setValue("def");

        PipelineRelationalRule filter6 = new PipelineRelationalRule();
        filter6.setField(field6);
        filter6.setOperator("is_null");

        PipelineRelationalRule filter7 = new PipelineRelationalRule();
        filter7.setField(field6);
        filter7.setOperator("is_not_null");

        PipelineLogicalRule logical1 = new PipelineLogicalRule();
        logical1.setCondition("AND");
        logical1.setRules(Arrays.asList(filter2, filter3));

        PipelineLogicalRule logical2 = new PipelineLogicalRule();
        logical2.setCondition("AND");
        logical2.setRules(Arrays.asList(filter4, filter5));

        PipelineLogicalRule logical3 = new PipelineLogicalRule();
        logical3.setCondition("OR");
        logical3.setRules(Arrays.asList(filter1, filter6, filter7, logical1, logical2));

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
        pipeline.setPipelineFilterObject(logical3);

        String template = tg.generateFile(pipeline, System.currentTimeMillis());
        String expected = TemplateTestUtils.loadTemplateInstance("templates/transform.hql");

        Assert.assertEquals(template, expected);
    }
}
