/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.App;
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

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Test template generator.
 */
public class DatamartTemplateGeneratorTest {
    DatamartTemplateGenerator tg;

    /**
     * Setup the template generator.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new DatamartTemplateGenerator();
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generation of pipeline package zip.
     */
    @Test
    public void testGeneratePackageDir() throws Exception {
        long fid1 = 1L;
        Field field1 = new Field();
        field1.setFieldId(fid1);
        field1.setFieldName("newfield1");
        field1.setFieldType("string");
        field1.setMeasurementType("DIM");

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);

        long fid2 = 2L;
        Field field2 = new Field();
        field2.setFieldId(fid2);
        field2.setFieldName("newfield2");
        field2.setFieldType("int");
        field2.setMeasurementType("MET");

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("newfieldalias2");
        projection2.setKey("_fieldkey2");

        long fid3 = 2L;
        Field field3 = new Field();
        field3.setFieldId(fid3);
        field3.setFieldName("newfield3");
        field3.setFieldType("int");
        field3.setMeasurementType("DIM");

        PipelineProjection projection3 = new PipelineProjection();
        projection3.setField(field3);
        projection3.setAlias("newfieldalias3");
        projection3.setKey("_fieldkey3");
        projection3.setAggregation(Aggregation.COUNT_DISTINCT);

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
        filter3.setCondition("AND");
        filter3.setRules(filters);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection1);
        projections.add(projection2);
        projections.add(projection3);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setPipelineSchemaName("schema1");
        pipeline.setProjections(projections);
        pipeline.setPipelineFilterObject(filter3);

        String fileOutput = "./target";
        long version = System.currentTimeMillis();
        tg.generateTemplateFiles(pipeline, fileOutput, version);

        String fileOutputTesting = "target/" + pipeline.getPipelineName() + "_v" + version + "/";
        String emailSubfolder = fileOutputTesting + "email/";
        String scriptSubfolder = fileOutputTesting + "scripts/";
        String regularSubfolder = fileOutputTesting + "regular_hour/";
        String backfillSubfolder = fileOutputTesting + "backfill_day/";
        String druidSubfolder = fileOutputTesting + "druid/";
        // List of all expected files
        List<File> fileList = new ArrayList<>();
        fileList.add(new File(fileOutputTesting + "bundle.xml"));
        fileList.add(new File(fileOutputTesting + "properties.xml"));
        fileList.add(new File(emailSubfolder + "workflow.xml"));
        fileList.add(new File(scriptSubfolder + "transform.hql"));
        fileList.add(new File(regularSubfolder + "workflow.xml"));
        fileList.add(new File(regularSubfolder + "coordinator.xml"));
        fileList.add(new File(regularSubfolder + "job.xml"));
        fileList.add(new File(regularSubfolder + "index.json"));
        fileList.add(new File(regularSubfolder + "druid_load.sh"));
        fileList.add(new File(backfillSubfolder + "index.json"));
        fileList.add(new File(backfillSubfolder + "druid_load.sh"));

        for (File f : fileList) {
            Assert.assertTrue(f.exists() && !f.isDirectory(), "Could not find " + f.getAbsolutePath());
        }
    }
}
