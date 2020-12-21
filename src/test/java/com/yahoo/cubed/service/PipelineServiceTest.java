/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.App;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test pipeline service.
 */
public class PipelineServiceTest {
    private final String schemaName = "schema1";

    /**
     * Create the database.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError1() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().update(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError2() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineId(100);
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().update(pipeline);
    }
    
    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError3() throws DataValidatorException, DatabaseException {
        Pipeline pipeline1 = new Pipeline();
        pipeline1.setPipelineName("newpipeline");
        pipeline1.setPipelineDescription("newpipeline description");
        pipeline1.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline1);
        long id = pipeline1.getPrimaryIdx();

        Pipeline pipeline2 = new Pipeline();
        pipeline2.setPipelineName(pipeline1.getPipelineName());
        pipeline2.setPipelineSchemaName("schema1");

        try {
            ServiceFactory.pipelineService().save(pipeline2);
        } finally {
            ServiceFactory.pipelineService().delete(id);
        }
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError4() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline);
        long id = pipeline.getPrimaryIdx();

        try {
            ServiceFactory.pipelineService().delete(id + 1);
        } finally {
            ServiceFactory.pipelineService().delete(id);
        }
    }
    
    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError5() throws DataValidatorException, DatabaseException {
        Field field = new Field();
        field.setFieldId(0);
        field.setFieldName("newfield");
        field.setFieldType("string");
        field.setSchemaName(schemaName);

        PipelineProjection projection = new PipelineProjection();
        projection.setField(field);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setProjections(projections);
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError6() throws DataValidatorException, DatabaseException {

        Field field = new Field();
        field.setFieldId(100);
        field.setFieldName("newfield");
        field.setFieldType("string");
        field.setSchemaName(schemaName);

        PipelineProjection projection = new PipelineProjection();
        projection.setField(field);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setProjections(projections);
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError7() throws DataValidatorException, DatabaseException {
        Field field1 = new Field();
        int fid = 1234;
        field1.setFieldName("newfield");
        field1.setFieldType("string");
        field1.setFieldId(fid);
        field1.setSchemaName(schemaName);

        ServiceFactory.fieldService().save(field1);

        Field field2 = new Field();
        field2.setFieldId(fid);
        field2.setFieldName("newfieldfield");
        field2.setFieldType("string");
        field2.setSchemaName(schemaName);

        PipelineProjection projection = new PipelineProjection();
        projection.setField(field2);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setProjections(projections);
        pipeline.setPipelineSchemaName("schema1");

        try {
            ServiceFactory.pipelineService().save(pipeline);
        } finally {
            ServiceFactory.fieldService().delete(schemaName, fid);
        }
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError8() throws DataValidatorException, DatabaseException {
        ServiceFactory.pipelineService().save(null);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError9() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(pipeline.getPrimaryName()).thenReturn(null);
        ServiceFactory.pipelineService().save(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError10() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(pipeline.getPrimaryName()).thenReturn("1nvalid");
        ServiceFactory.pipelineService().save(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError11() throws DataValidatorException, DatabaseException {
        ServiceFactory.pipelineService().update(null);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError12() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(pipeline.getPrimaryName()).thenReturn(null);
        ServiceFactory.pipelineService().update(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError13() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = Mockito.mock(Pipeline.class);
        Mockito.when(pipeline.getPrimaryName()).thenReturn("1nvalid");
        ServiceFactory.pipelineService().update(pipeline);
    }

    /**
     * Test data validation exception.
     */
    @Test(expectedExceptions = { DataValidatorException.class })
    public void testPipelineProjectionsValidationError14() throws DataValidatorException, DatabaseException {
        Pipeline p1 = new Pipeline();
        p1.setPipelineName("p1");
        p1.setPipelineDescription("test");
        p1.setPipelineSchemaName("schema1");
        Pipeline p2 = new Pipeline();
        p2.setPipelineName("p2");
        p2.setPipelineDescription("test");
        p2.setPipelineSchemaName("schema1");
        ServiceFactory.pipelineService().save(p1);
        long pid1 = p1.getPrimaryIdx();
        ServiceFactory.pipelineService().save(p2);
        long pid2 = p2.getPrimaryIdx();

        try {
            p2.setPipelineName("p1");
            ServiceFactory.pipelineService().update(p2);
        } finally {
            ServiceFactory.pipelineService().delete(pid1);
            ServiceFactory.pipelineService().delete(pid2);
        }
    }

    /**
     * Test pipeline fetch.
     */
    @Test
    public void testPipelineFetch() throws DataValidatorException, DatabaseException {
        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipelinedescription");
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline);
        long pid = pipeline.getPrimaryIdx();

        Pipeline pipeline2 = ServiceFactory.pipelineService().fetch(pid);
        Assert.assertEquals(pipeline2.getPipelineName(), pipeline.getPipelineName());
        Assert.assertEquals(pipeline2.getPipelineDescription(), pipeline.getPipelineDescription());

        ServiceFactory.pipelineService().delete(pid);
    }

    /**
     * Check that pipeline fetch works with multiple fields.
     */
    @Test
    public void testPipelineFetchWithFields() throws DataValidatorException, DatabaseException {
        Field field1 = new Field();
        int fid = 1234;
        field1.setFieldName("newnewfield");
        field1.setFieldType("string");
        field1.setFieldId(fid);
        field1.setSchemaName(schemaName);

        ServiceFactory.fieldService().save(field1);

        Field field2 = new Field();
        field2.setFieldId(fid);
        field2.setFieldName("newnewfield");
        field2.setFieldType("string");
        field2.setSchemaName(schemaName);

        PipelineProjection projection = new PipelineProjection();
        projection.setField(field2);

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection);

        Pipeline pipeline = new Pipeline();
        pipeline.setPipelineName("newpipeline");
        pipeline.setPipelineDescription("newpipeline description");
        pipeline.setProjections(projections);
        pipeline.setPipelineSchemaName("schema1");

        ServiceFactory.pipelineService().save(pipeline);
        long pid = pipeline.getPrimaryIdx();

        Pipeline pipeline2 = ServiceFactory.pipelineService().fetch(pid);
        Assert.assertEquals(pipeline2.getPipelineName(), pipeline.getPipelineName());
        Assert.assertEquals(pipeline2.getProjections().size(), pipeline.getProjections().size());

        ServiceFactory.pipelineService().delete(pid);
        ServiceFactory.fieldService().delete(schemaName, fid);
    }
}
