/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.filter.PipelineFilter;
import com.yahoo.cubed.model.filter.PipelineLogicalRule;
import com.yahoo.cubed.model.filter.PipelineRelationalRule;
import com.yahoo.cubed.settings.CLISettings;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test backfill bundle properties xml generation.
 */
public class BundlePropertiesBackfillXmlTest {
    BundlePropertiesBackfillXml tg;

    /**
     * Initalize backfill properties template.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new BundlePropertiesBackfillXml();
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generation of properties backfill.
     */
    @Test
    public void testGeneratePropertiesBackfillXmlTemplate() throws Exception {
        long fid = 123L;
        Field field = new Field();
        field.setFieldId(fid);
        field.setFieldName("newnewfield");
        field.setFieldType("string");

        PipelineProjection projection = new PipelineProjection();
        projection.setField(field);

        PipelineRelationalRule filter1 = new PipelineRelationalRule();
        filter1.setField(field);
        filter1.setOperator("greater");
        filter1.setValue("3");

        PipelineLogicalRule filter2 = new PipelineLogicalRule();
        filter2.setCondition("AND");
        List<PipelineFilter> filters2 = new ArrayList<>();
        filters2.add(filter1);
        filter2.setRules(filters2);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setPipelineSchemaName("schema1");
        pipeline.setProjections(projections);
        pipeline.setPipelineFilterObject(filter2);
        long version = 100L;

        String template = tg.generateFile(pipeline, version);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/bundle_properties_backfill.xml");

        Assert.assertEquals(template.replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }
}
