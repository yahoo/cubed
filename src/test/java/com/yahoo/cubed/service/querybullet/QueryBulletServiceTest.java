/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.querybullet;

import com.yahoo.cubed.App;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.service.querybullet.QueryBulletService.ResponseJson;
import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.ConfigurationLoader;
import com.yahoo.cubed.util.Aggregation;
import com.yahoo.cubed.util.Measurement;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.BasicHttpEntity;
import org.apache.http.message.BasicStatusLine;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Test of cardinality estimation service.
 */
public class QueryBulletServiceTest {
    private static HttpClient httpClient;
    private static QueryBulletService queryBulletService;
    private static Pipeline pipeline;
    private static String schemaName = "schema1";

    private static class MockQueryBulletServiceImpl extends QueryBulletServiceImpl {
        @Override
        protected HttpClient newHttpClientInstance() {
            return httpClient;
        }
    }

    /**
     * Mock HTTP responses.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
        ConfigurationLoader.load();

        // mock http response status line
        StatusLine statusLine = Mockito.mock(BasicStatusLine.class);
        Mockito.when(statusLine.getStatusCode()).thenReturn(HttpStatus.SC_OK);

        // mock http entity
        HttpEntity httpEntity = Mockito.mock(BasicHttpEntity.class);
        Mockito.when(httpEntity.getContent()).thenReturn(new ByteArrayInputStream("{ \"records\" : [{ \"COUNT DISTINCT\" : 10.0 }] }".getBytes(StandardCharsets.UTF_8)));

        // mock http response
        HttpResponse httpResponse = Mockito.mock(HttpResponse.class);
        Mockito.when(httpResponse.getStatusLine()).thenReturn(statusLine);
        Mockito.when(httpResponse.getEntity()).thenReturn(httpEntity);

        // mock http client
        HttpClient myHttpClient = Mockito.mock(HttpClient.class);
        Mockito.when(myHttpClient.execute(Mockito.any(HttpPost.class))).thenReturn(httpResponse);

        // mock pipeline
        Field field1 = new Field();
        field1.setFieldId(1L);
        field1.setFieldName("field1");
        field1.setFieldType("string");
        field1.setSchemaName(schemaName);
        field1.setMeasurementType(Measurement.METRIC.measurementTypeCode);

        PipelineProjection projection1 = new PipelineProjection();
        projection1.setField(field1);
        projection1.setAlias("F1");
        projection1.setAggregation(Aggregation.COUNT);

        Field field2 = new Field();
        field2.setFieldId(2L);
        field2.setFieldName("field2");
        field2.setFieldType("string");
        field2.setSchemaName(schemaName);
        field2.setMeasurementType(Measurement.DIMENSION.measurementTypeCode);

        PipelineProjection projection2 = new PipelineProjection();
        projection2.setField(field2);
        projection2.setAlias("F2");
        projection2.setKey("_KEY");

        List<PipelineProjection> projections = new ArrayList<>();
        projections.add(projection1);
        projections.add(projection2);

        Pipeline myPipeline = new Pipeline();
        myPipeline.setPipelineName("pipeline");
        myPipeline.setPipelineDescription("pipeline description");
        myPipeline.setProjections(projections);
        myPipeline.setPipelineFilterJson("{\"condition\":\"AND\",\"rules\":[{\"id\":\"price\",\"field\":\"price\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"less\",\"value\":\"10.25\"},{\"id\":\"filter_tag\",\"field\":\"filter_tag\",\"type\":\"double\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"null\"}]}");
        myPipeline.setPipelineSchemaName("schema1");

        pipeline = myPipeline;
        httpClient = myHttpClient;
        queryBulletService = new MockQueryBulletServiceImpl();
    }

    /**
     * Check cardinality API.
     */
    @Test
    public void testQueryBulletAPI() throws ClientProtocolException, IOException, BulletQueryFailException {
        String jsonRequest = "{\"name\":\"test2\",\"description\":\"test\",\"owner\":\"etsd\",\"schemaName\":\"schema1\",\"projections\":[{\"column_id\":3,\"key\":null,\"alias\":\"beaconcollector\",\"aggregate\":null,\"schema_name\":\"schema1\"}],\"projectionVMs\":[[]],\"filter\":{\"condition\":\"AND\",\"rules\":[{\"id\":\"geo_info[city]\",\"field\":\"geo_info[city]\",\"type\":\"string\",\"input\":\"radio\",\"operator\":\"equal\",\"value\":\"1\"},{\"condition\":\"AND\",\"rules\":[{\"id\":\"browser\",\"field\":\"browser\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"%a%\"},{\"id\":\"browser\",\"field\":\"browser\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"%b%\"},{\"id\":\"browser\",\"field\":\"browser\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"%c%\"}]}]},\"backfillEnabled\":true,\"backfillStartDate\":\"2018-03-22\",\"endTimeEnabled\":false,\"endTimeDate\":null}";
        ResponseJson response = queryBulletService.sendBulletQueryJson(jsonRequest);
        // make sure query passes, contains records, and isn't empty
        Assert.assertEquals(response.statusCode, HttpStatus.SC_OK);
        Assert.assertTrue(response.jsonResponse.contains("records"));
    }

    /**
     * Check cardinality API to catch exception.
     */
    @Test
    public void testQueryBulletAPICatchException() throws Exception {
        try {
            QueryBulletServiceImpl temp = new QueryBulletServiceImpl();
            temp.sendBulletQueryJson(null);
        } catch (BulletQueryFailException b) {
            Assert.assertEquals(b.getMessage(), "java.lang.IllegalArgumentException: argument \"content\" is null");
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }

    }
}
