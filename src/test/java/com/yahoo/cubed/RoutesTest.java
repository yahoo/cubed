/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed;

import com.yahoo.cubed.json.FunnelQueryResult;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.pipeline.launch.PipelineLauncher;
import com.yahoo.cubed.pipeline.launch.PipelineLauncherManager;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.bullet.query.BulletQuery;
import com.yahoo.cubed.service.cardinality.CardinalityEstimationService;
import com.yahoo.cubed.service.querybullet.QueryBulletService;
import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.HiveConnectionManager;
import com.yahoo.cubed.templating.FunnelGroupTemplateGenerator;
import com.yahoo.cubed.templating.TemplateTestUtils;
import com.yahoo.cubed.util.Status;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.CompletableFuture;

import org.apache.commons.io.FileUtils;
import org.apache.http.HttpStatus;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import spark.Request;
import spark.Response;
import spark.TemplateEngine;
import spark.ModelAndView;
import spark.template.thymeleaf.ThymeleafTemplateEngine;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletResponse;

import static org.mockito.Mockito.when;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.anyBoolean;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.times;

/**
 * Unit test for the Routes class.
 */
public class RoutesTest {
    static TemplateEngine templateEngine;
    /**
     * Setup database, schema, and operational params.
     */
    @BeforeClass
    public static void initialize() throws Exception {
        // Initialize necessary parameters
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        CLISettings.SCHEMA_FILES_DIR = "src/test/resources/schemas/";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
        Routes.init();
        // Create the new template engine
        templateEngine = new ThymeleafTemplateEngine();
    }

    /**
     * Setup template folder.
     */
    @BeforeMethod
    public static void prepareTemplateFolder() throws Exception {
        CLISettings.TEMPLATE_OUTPUT_FOLDER = "/tmp/funnelmart_test";
        File templateOutputFolder = new File(CLISettings.TEMPLATE_OUTPUT_FOLDER);
        if (!templateOutputFolder.exists()) {
            templateOutputFolder.mkdir();
        } else {
            FileUtils.deleteDirectory(templateOutputFolder);
            templateOutputFolder.mkdir();
        }
    }

    /**
     * Helper function to create a new data mart.
     * @param json Input JSON
     * @param expectedStatus Expected HTTP return status
     */
    private static String createNewDataMartHelper(String json, int expectedStatus) throws Exception {
        // Mock the request
        Request mockedRequest = mock(Request.class);

        // Replace single quote with double quote
        json = json.replaceAll("'", "\"");

        // Return the JSON when asked
        when(mockedRequest.body()).thenReturn(json);

        // Mock verify the response
        Response mockedResponse = mock(Response.class);

        // Create the new data mart
        String result = Routes.createNewDatamart(mockedRequest, mockedResponse);
        // Should receive expected status
        verify(mockedResponse).status(expectedStatus);

        return result;
    }

    private String createSampleDataMart() throws Exception {
        // JSON to parse
        String json =
                "{'name':'test_mart',                  " +
                " 'schemaName':'schema1',              " +
                " 'description':'simple description',  " +
                " 'owner':'john_doe',                  " +
                " 'projections':[                      " +
                "     {                                " +
                "       'column_id':15,                " +
                "       'alias':'col_alias',           " +
                "       'aggregate':'SUM',             " +
                "       'key':null,                    " +
                "       'schema_name':'schema1'        " +
                "     }                                " +
                " ],                                   " +
                " 'projectionVMs': [                   " +
                "     [                                " +
                "        [ 'value1', 'value_alias1', 'equal'],  " +
                "        [ 'value2', 'value_alias2', 'equal']   " +
                "     ]                                " +
                " ],                                   " +
                " 'filter':{                           " +
                "   'condition':'OR',                  " +
                "   'rules':[                          " +
                "     {                                " +
                "       'id':'network_status',         " +
                "       'field':'network_status',      " +
                "       'type':'string',               " +
                "       'input':'text',                " +
                "       'operator':'equal',            " +
                "       'value':'on'                   " +
                "     },                               " +
                "     {                                " +
                "       'condition':'AND',             " +
                "       'rules':[                      " +
                "         {                            " +
                "           'id':'cookie_one',         " +
                "           'field':'cookie_one',      " +
                "           'type':'string',           " +
                "           'input':'text',            " +
                "           'operator':'equal',        " +
                "           'value':'ab'               " +
                "         },                           " +
                "         {                            " +
                "           'id':'property',           " +
                "           'field':'property',        " +
                "           'type':'string',           " +
                "           'input':'text',            " +
                "           'operator':'equal',        " +
                "           'value':'de'               " +
                "         }                            " +
                "       ]                              " +
                "     }                                " +
                "   ]                                  " +
                " }                                    " +
                "}                                     ";

        // Should receive 201 status
        return createNewDataMartHelper(json, 201);
    }

    /**
     * Test listDatamart.
     */
    @Test
    public void listDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();
        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // list datamart
            ModelAndView listPage = Routes.listDatamart(mockedRequest, mockedResponse);

            Map<String, Object> params = new HashMap<>();
            LocalDateTime date = LocalDateTime.now();
            DateTimeFormatter formaters = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            String text = date.format(formaters);
            params.put("updateDate", text);
            params.put("dataHref", "/datamart/" + pipelineId);

            ModelAndView expected = new ModelAndView(params, "list_datamart_expected");

            Assert.assertEquals(templateEngine.render(listPage).replaceAll("\\s", ""),
                    templateEngine.render(expected).replaceAll("\\s", ""));

        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }

    }

    /**
     * Test listDeletedPage.
     */
    @Test
    public void listDeletedDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the new data mart
            Routes.deleteDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);
            ModelAndView listDeletedPage = Routes.listDeletedDatamart(mockedRequest, mockedResponse);

            Map<String, Object> params = new HashMap<>();
            LocalDateTime date = LocalDateTime.now();
            DateTimeFormatter formaters = DateTimeFormatter.ofPattern("yyyy/MM/dd");
            String text = date.format(formaters);
            params.put("deleteDate", text);
            params.put("dataHref", "/datamart/" + pipelineId);

            ModelAndView expected = new ModelAndView(params, "list_deleted_datamart_expected");

            Assert.assertEquals(templateEngine.render(listDeletedPage).replaceAll("\\s", ""),
                    templateEngine.render(expected).replaceAll("\\s", ""));

        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }

    /**
     * Test previewFunnelQuery.
     */
    @Test
    public void previewFunnelQueryTest() throws Exception {
        // Test success
        String jsonRequest = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        Request mockedRequest = mock(Request.class);
        when(mockedRequest.body()).thenReturn(jsonRequest);
        Response mockedResponse = mock(Response.class);
        Routes.previewFunnelQuery(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_OK);

        // Test when Utils.createAndValidateFunnelQuery fails
        when(mockedRequest.body()).thenReturn(anyString());
        Routes.previewFunnelQuery(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * Test previewDatamartEtl.
     */
    @Test
    public void previewDatamartEtlTest() {
        // Test success
        String jsonRequest = "{\n" +
                "  \"name\": \"name\",\n" +
                "  \"schemaName\": \"schema1\",\n" +
                "  \"description\": \"desc\",\n" +
                "  \"owner\": \"owner\",\n" +
                "  \"projections\": [{\n" +
                "    \"column_id\": \"2\",\n" +
                "    \"key\": \"\",\n" +
                "    \"alias\": \"cookie_one\",\n" +
                "    \"aggregate\": \"NONE\",\n" +
                "    \"schema_name\": \"schema1\"\n" +
                "  }],\n" +
                "  \"projectionVMs\": [\n" +
                "    []\n" +
                "  ],\n" +
                "  \"filter\": {\n" +
                "    \"condition\": \"AND\",\n" +
                "    \"rules\": [{\n" +
                "      \"id\": \"network_status\",\n" +
                "      \"field\": \"network_status\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"input\": \"text\",\n" +
                "      \"operator\": \"equal\",\n" +
                "      \"value\": \"on\"\n" +
                "    }, {\n" +
                "      \"id\": \"device\",\n" +
                "      \"field\": \"device\",\n" +
                "      \"type\": \"string\",\n" +
                "      \"input\": \"text\",\n" +
                "      \"operator\": \"equal\",\n" +
                "      \"value\": \"desktop\"\n" +
                "    }]\n" +
                "  },\n" +
                "  \"backfillEnabled\": true,\n" +
                "  \"backfillStartDate\": \"2018-08-06\",\n" +
                "  \"endTimeEnabled\": false,\n" +
                "  \"endTimeDate\": null\n" +
                "}";
        Request mockedRequest = mock(Request.class);
        when(mockedRequest.body()).thenReturn(jsonRequest);
        Response mockedResponse = mock(Response.class);
        Routes.previewDatamartEtl(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_OK);

        // Test when Utils.constructFunnelmartPipeline fails
        when(mockedRequest.body()).thenReturn(anyString());
        Routes.previewDatamartEtl(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * Test bulletQuery.
     */
    @Test
    public void queryBulletTest() throws BulletQueryFailException {
        // Test success
        // Create mocked response
        QueryBulletService.ResponseJson mockedResponseJson = mock(QueryBulletService.ResponseJson.class);
        mockedResponseJson.statusCode = HttpStatus.SC_OK;

        // Create mocked QueryBulletService and set the static with mock
        QueryBulletService mockedQueryBulletService = mock(QueryBulletService.class);
        ServiceFactory.setQueryBulletService(mockedQueryBulletService);

        // Mock API call
        when(mockedQueryBulletService.sendBulletQueryJson(anyString())).thenReturn(mockedResponseJson);

        // Mocked request and response
        Request mockedRequest = mock(Request.class);
        Response mockedResponse = mock(Response.class);
        Routes.queryBullet(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_OK);

        // Test fail
        mockedResponseJson.statusCode = HttpStatus.SC_INTERNAL_SERVER_ERROR;
        Routes.queryBullet(mockedRequest, mockedResponse);
        verify(mockedResponse).status(HttpStatus.SC_INTERNAL_SERVER_ERROR);
    }

    /**
     * Test launchDatamart.
     */
    @Test
    public void launchDatamartTest() throws Exception {
        String json =
                "{'name':'test_mart',                  " +
                        " 'schemaName':'schema1',              " +
                        " 'description':'simple description',  " +
                        " 'owner':'john_doe',                  " +
                        " 'projections':[                      " +
                        "     {                                " +
                        "       'column_id':1,                 " +
                        "       'alias':'col_alias',           " +
                        "       'schema_name':'schema1',       " +
                        "       'aggregate':'SUM',             " +
                        "       'key':'sample_map_key'         " +
                        "     },                               " +
                        "     {                                " +
                        "       'column_id':9,                 " +
                        "       'alias':'bcookie',          " +
                        "       'schema_name':'schema1',       " +
                        "       'aggregate':'null',            " +
                        "       'key':'null'                   " +
                        "     }                                " +
                        " ],                                   " +
                        " 'projectionVMs': [                   " +
                        "     [                                " +
                        "        [ 'value1', 'value_alias1', 'equal'],  " +
                        "        [ 'value2', 'value_alias2', 'equal']   " +
                        "     ]                                " +
                        " ],                                   " +
                        " 'filter':{                           " +
                        "   'condition':'OR',                  " +
                        "   'rules':[                          " +
                        "     {                                " +
                        "       'id':'network_status',         " +
                        "       'field':'network_status',      " +
                        "       'type':'string',               " +
                        "       'input':'text',                " +
                        "       'operator':'equal',            " +
                        "       'value':'on'                   " +
                        "     },                               " +
                        "     {                                " +
                        "       'condition':'AND',             " +
                        "       'rules':[                      " +
                        "         {                            " +
                        "           'id':'cookie_one',         " +
                        "           'field':'cookie_one',      " +
                        "           'type':'string',           " +
                        "           'input':'text',            " +
                        "           'operator':'equal',        " +
                        "           'value':'ab'               " +
                        "         },                           " +
                        "         {                            " +
                        "           'id':'property',           " +
                        "           'field':'property',        " +
                        "           'type':'string',           " +
                        "           'input':'text',            " +
                        "           'operator':'equal',        " +
                        "           'value':'de'               " +
                        "         }                            " +
                        "       ]                              " +
                        "     }                                " +
                        "   ]                                  " +
                        " }                                    " +
                        "}                                     ";
        String pipelineId = createNewDataMartHelper(json, 201);

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.pipelineLauncherManager = new PipelineLauncherManager();
            String errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(1)).status(500);

            // Clean test folder in /tmp/cubed
            File templateOutputFolder = new File(CLISettings.TEMPLATE_OUTPUT_FOLDER);
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            CardinalityEstimationService mockedCardinalityEstimationService = mock(CardinalityEstimationService.class);
            ServiceFactory.setCardinalityEstimationService(mockedCardinalityEstimationService);
            CardinalityEstimationService.Response mockedQueryResult = mock(CardinalityEstimationService.Response.class);
            when(mockedCardinalityEstimationService.sendBulletQuery(Mockito.any(Pipeline.class))).thenReturn(mockedQueryResult);
            mockedQueryResult.statusCode = HttpStatus.SC_BAD_REQUEST;
            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(2)).status(500);
            Assert.assertEquals(errMsg, "Failed to send the bullet query with response code [400]");

            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            mockedQueryResult.statusCode = HttpStatus.SC_OK;
            mockedQueryResult.aggregationValues = new HashMap<>();
            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(3)).status(500);
            Assert.assertEquals(errMsg, "Failed to fetch the value of aggregation 'COUNT DISTINCT' via Bullet query.");

            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            mockedQueryResult.statusCode = HttpStatus.SC_OK;
            mockedQueryResult.aggregationValues.put(BulletQuery.AGGREGATION_TYPE, CLISettings.CARDINALITY_CAP + 1.0);
            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(4)).status(500);
            Assert.assertEquals(errMsg, "Pipeline test_mart CANNOT be launched because it has cardinality 51.0, which exceeds the cap 50. Please modify the filter condition or remove projections.");

            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            mockedQueryResult.statusCode = HttpStatus.SC_OK;
            mockedQueryResult.aggregationValues = new HashMap<>();
            mockedQueryResult.aggregationValues.put(BulletQuery.AGGREGATION_TYPE, 10.0);
            PipelineLauncherManager mockedPipelineLauncherManager = mock(PipelineLauncherManager.class);

            PipelineLauncher.LaunchStatus mockedLaunchStatus = mock(PipelineLauncher.LaunchStatus.class);
            mockedLaunchStatus.hasError = true;
            mockedLaunchStatus.errorMsg = "Test Error Message";
            PowerMockito.when(mockedPipelineLauncherManager.
                    launchPipeline(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString(), anyString()))
                    .thenReturn(CompletableFuture.completedFuture(mockedLaunchStatus));
            Routes.pipelineLauncherManager = mockedPipelineLauncherManager;

            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(5)).status(500);
            Assert.assertEquals(errMsg, "Test Error Message");


            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            mockedQueryResult.statusCode = HttpStatus.SC_OK;
            mockedLaunchStatus.hasError = false;
            Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(1)).status(200);
            Assert.assertEquals(ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10)).getPipelineStatus(), "Deployed v1");

            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            mockedQueryResult.statusCode = HttpStatus.SC_OK;
            mockedLaunchStatus.hasError = false;
            mockedLaunchStatus.oozieJobIdOfBackfill = "testid";
            mockedQueryResult.aggregationValues = new HashMap<>();
            mockedQueryResult.aggregationValues.put(BulletQuery.AGGREGATION_TYPE, 0.0);
            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(2)).status(200);
            Assert.assertEquals(ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10)).getPipelineStatus(), "Deployed v1");
            Assert.assertEquals(errMsg, "No results found in Bullet for your pipeline, try adjusting your filters and projections. The pipeline has still been launched.");

            when(mockedRequest.params(":id")).thenReturn("");
            errMsg = Routes.launchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse, times(6)).status(500);
            Assert.assertEquals(errMsg, "For input string: \"\"");
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }

    /**
     * Test downloadDatamart.
     */
    @Test
    public void downloadDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            HttpServletResponse mockedResponseRaw = mock(HttpServletResponse.class);
            ServletOutputStream mockedOutstream = mock(ServletOutputStream.class);
            when(mockedResponseRaw.getOutputStream()).thenReturn(mockedOutstream);
            when(mockedResponse.raw()).thenReturn(mockedResponseRaw);
            Routes.downloadDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            deleted = true;

            Routes.downloadDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            }
        }
    }

    /**
     * Test stopDatamart.
     */
    @Test
    public void stopDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.stopPipeline(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            deleted = true;

            Routes.stopPipeline(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            }
        }
    }

    /**
     * Test relaunchDatamart.
     */
    @Test
    public void relaunchDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.relaunchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            pipeline.setPipelineStatus(String.format(Status.ACTIVE, Long.toString(pipeline.getPipelineVersion())));
            ServiceFactory.pipelineService().update(pipeline);
            Routes.relaunchDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);

        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));

        }
    }

    /**
     * Test resetDatamart.
     */
    @Test
    public void resetDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();
        boolean deleted = false;

        try {
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            pipeline.setPipelineStatus(String.format(Status.ACTIVE, Long.toString(pipeline.getPipelineVersion())));
            pipeline.setPipelineOozieJobId("test");
            pipeline.setPipelineOozieBackfillJobId("test");
            ServiceFactory.pipelineService().update(pipeline);

            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.resetDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(pipeline.getPipelineStatus(), Status.INACTIVE);
            Assert.assertEquals(pipeline.getPipelineOozieJobId(), null);
            Assert.assertEquals(pipeline.getPipelineOozieBackfillJobId(), null);

            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            deleted = true;

            Routes.resetDatamart(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            }

        }
    }

    /**
     * Test newDatamart.
     */
    @Test
    public void newDatamartTest() throws Exception {
        // Mock the request
        Request mockedRequest = mock(Request.class);
        when(mockedRequest.queryParams("schemaName")).thenReturn("schema1");
        // Mock verify the response
        Response mockedResponse = mock(Response.class);
        // View
        ModelAndView helpPage = Routes.newDatamart(mockedRequest, mockedResponse);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/new_view_datamart_expected.html");

        Assert.assertEquals(templateEngine.render(helpPage).replaceAll("\\s", ""), expected.replaceAll("\\s", ""));
    }

    /**
     * Test viewDatamart.
     */
    @Test
    public void viewDatamartTest() throws Exception {
        String pipelineId = createSampleDataMart();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Clone the pipeline
            ModelAndView modelAndView = Routes.viewDatamart(mockedRequest, mockedResponse);
            // Get params
            Map<String, Object> params = (Map<String, Object>) modelAndView.getModel();

            // Check the parameters
            Assert.assertEquals(params.get("viewPipeline"), "true");
            Assert.assertEquals(params.get("pipelineName"), "test_mart");
            Assert.assertEquals(params.get("pipelineDescription"), "simple description");
            Assert.assertEquals(params.get("pipelineOwner"), "john_doe");
            Assert.assertNotNull(params.get("projections"));
            Assert.assertNotNull(params.get("filters"));

            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            deleted = true;

            modelAndView = Routes.viewDatamart(mockedRequest, mockedResponse);
            // Get params
            params = (Map<String, Object>) modelAndView.getModel();
            // Check the parameters
            Assert.assertNotNull(params.get("error"));

            when(mockedRequest.params(":id")).thenReturn("invalid id");
            modelAndView = Routes.viewDatamart(mockedRequest, mockedResponse);
            // Get params
            params = (Map<String, Object>) modelAndView.getModel();
            // Check the parameters
            Assert.assertEquals(params.get("error"), "Error parsing pipeline id.");
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
            }

        }
    }

    /**
     * Simple update of pipeline.
     */
    @Test
    public void testUpdateDatamartSimple() throws Exception {
        String pipelineId = createSampleDataMart();

        try {
            // JSON to parse
            String json =
                            "{                                     " +
                            " 'schemaName':'schema1',              " +
                            " 'description':'simple description',  " +
                            " 'owner':'john_doe',                  " +
                            " 'filter':{                           " +
                            "   'description':'simple description'," +
                            "   'condition':'OR',                  " +
                            "   'rules':[                          " +
                            "     {                                " +
                            "       'id':'property',               " +
                            "       'field':'property',            " +
                            "       'type':'string',               " +
                            "       'input':'text',                " +
                            "       'operator':'equal',            " +
                            "       'value':'abc'                  " +
                            "     },                               " +
                            "     {                                " +
                            "       'condition':'AND',             " +
                            "       'rules':[                      " +
                            "         {                            " +
                            "           'id':'device',             " +
                            "           'field':'device',          " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'ab'               " +
                            "         },                           " +
                            "         {                            " +
                            "           'id':'browser',            " +
                            "           'field':'browser',         " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'de'               " +
                            "         }                            " +
                            "       ]                              " +
                            "     }                                " +
                            "   ]                                  " +
                            " },                                   " +
                            " 'projections': [                     " +
                            "     {                                " +
                            "       'column_id':4,                 " +
                            "       'aggregate':'SUM',             " +
                            "       'schema_name':'schema1',       " +
                            "       'key':'state',                 " +
                            "       'alias':'state_alias'          " +
                            "     }                                " +
                            " ],                                   " +
                            " 'projectionVMs': [                   " +
                            "     [                                " +
                            "        [ 'value3', 'value_alias3', 'equal'],  " +
                            "        [ 'value4', 'value_alias4', 'equal']   " +
                            "     ]                                " +
                            " ]                                    " +
                            "}                                     ";
            // Replace single quote with double quote
            json = json.replaceAll("'", "\"");

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Return the JSON when asked
            when(mockedRequest.body()).thenReturn(json);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the new data mart
            Routes.updateDatamart(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);

            // Fetch the data mart by id
            Pipeline updatedPipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(updatedPipeline.getProjections().size(), 1);
            // Check the projection value mappings are updated
            for (PipelineProjection p: updatedPipeline.getProjections()) {
                Assert.assertEquals(p.getProjectionVMs().size(), 2);
                for (PipelineProjectionVM v: p.getProjectionVMs()) {
                    if (v.getFieldValue().equals("value3")) {
                        Assert.assertEquals(v.getFieldValueMapping(), "value_alias3");
                    } else {
                        Assert.assertEquals(v.getFieldValue(), "value4");
                        Assert.assertEquals(v.getFieldValueMapping(), "value_alias4");
                    }
                }
            }
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }

    /**
     * Test using bad JSON.
     */
    @Test
    public void testUpdateDatamartBadJson() throws Exception {
        String pipelineId = createSampleDataMart();
        try {
            // valid filters
            String newFilters = "bad json";

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Return the JSON when asked
            when(mockedRequest.body()).thenReturn(newFilters);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the new data mart
            Routes.updateDatamart(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(500);
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }

    /**
     * Update data mart including original projections.
     */
    @Test
    public void testUpdateDatamartWithOriginalProjections() throws Exception {
        String pipelineId = createSampleDataMart();

        try {
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(pipeline.getProjections().size(), 1);

            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(0).getFieldValue(), "value1");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(0).getFieldValueMapping(), "value_alias1");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(1).getFieldValue(), "value2");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(1).getFieldValueMapping(), "value_alias2");

            // JSON to parse
            String json =
                            "{                                     " +
                            " 'schemaName':'schema1',              " +
                            " 'description':'simple description',  " +
                            " 'owner':'john_doe',                  " +
                            " 'filter':{                           " +
                            "   'condition':'OR',                  " +
                            "   'rules':[                          " +
                            "     {                                " +
                            "       'id':'property',               " +
                            "       'field':'property',            " +
                            "       'type':'string',               " +
                            "       'input':'text',                " +
                            "       'operator':'equal',            " +
                            "       'value':'xyz'                  " +
                            "     },                               " +
                            "     {                                " +
                            "       'condition':'AND',             " +
                            "       'rules':[                      " +
                            "         {                            " +
                            "           'id':'device',             " +
                            "           'field':'device',          " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'ab'               " +
                            "         },                           " +
                            "         {                            " +
                            "           'id':'network_status',     " +
                            "           'field':'network_status',  " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'de'               " +
                            "         }                            " +
                            "       ]                              " +
                            "     }                                " +
                            "   ]                                  " +
                            " },                                   " +
                            " 'projections': [                     " +
                            "     {                                " +
                            "       'column_id':15,                " +
                            "       'aggregate':'NONE',            " +
                            "       'key':'pty',                   " +
                            "       'schema_name':'schema1',       " +
                            "       'alias':'lalala_updated_alias' " +
                            "     },                               " +
                            "     {                                " +
                            "       'column_id':4,                 " +
                            "       'aggregate':'SUM',             " +
                            "       'schema_name':'schema1',       " +
                            "       'key':'country',               " +
                            "       'alias':'country_alias'        " +
                            "     }                                " +
                            "  ]                                   " +
                            "}                                     ";
            // Replace single quote with double quote
            json = json.replaceAll("'", "\"");

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Return the JSON when asked
            when(mockedRequest.body()).thenReturn(json);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the new data mart
            Routes.updateDatamart(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);

            // Fetch the data mart by id
            Pipeline updatedPipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(updatedPipeline.getProjections().size(), 2);
            Assert.assertEquals(updatedPipeline.getProjections().get(0).getAlias(), "lalala_updated_alias");
            Assert.assertEquals(updatedPipeline.getProjections().get(0).getProjectionVMs().size(), 0);
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().size(), 0);
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }


    /**
     * Update datamart with projections.
     */
    @Test
    public void testUpdateDatamartWithProjections() throws Exception {
        String pipelineId = createSampleDataMart();

        try {
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(pipeline.getProjections().size(), 1);
            pipeline.setPipelineOozieJobId("fake oozie id");
            // Update the data mart
            ServiceFactory.pipelineService().update(pipeline);

            // JSON to parse
            String json =
                            "{                                     " +
                            " 'schemaName':'schema1',              " +
                            " 'description':'simple description',  " +
                            " 'owner':'john_doe',                  " +
                            " 'filter':{                           " +
                            "   'condition':'OR',                  " +
                            "   'rules':[                          " +
                            "     {                                " +
                            "       'id':'property',               " +
                            "       'field':'property',            " +
                            "       'type':'string',               " +
                            "       'input':'text',                " +
                            "       'operator':'equal',            " +
                            "       'value':'us'                   " +
                            "     },                               " +
                            "     {                                " +
                            "       'condition':'AND',             " +
                            "       'rules':[                      " +
                            "         {                            " +
                            "           'id':'browser',            " +
                            "           'field':'browser',         " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'ab'               " +
                            "         },                           " +
                            "         {                            " +
                            "           'id':'user_event',         " +
                            "           'field':'user_event',      " +
                            "           'type':'string',           " +
                            "           'input':'text',            " +
                            "           'operator':'equal',        " +
                            "           'value':'de'               " +
                            "         }                            " +
                            "       ]                              " +
                            "     }                                " +
                            "   ]                                  " +
                            " },                                   " +
                            " 'projections': [                     " +
                            "     {                                " +
                            "       'column_id':1,                 " +
                            "       'alias':'col_alias',           " +
                            "       'schema_name':'schema1',       " +
                            "       'aggregate':'NONE',            " +
                            "       'key':'sample_map_key'         " +
                            "     },                               " +
                            "     {                                " +
                            "       'column_id':4,                 " +
                            "       'aggregate':'SUM',             " +
                            "       'schema_name':'schema1',       " +
                            "       'key':'country',               " +
                            "       'alias':'country_alias'        " +
                            "     }                                " +
                            "  ],                                  " +
                            " 'projectionVMs': [                   " +
                            "     [],                              " +
                            "     [                                " +
                            "        [ 'value3', 'value_alias3', 'equal'],  " +
                            "        [ 'value4', 'value_alias4', 'equal']   " +
                            "     ]                                " +
                            " ]                                    " +
                            "}                                     ";
            // Replace single quote with double quote
            json = json.replaceAll("'", "\"");

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Return the JSON when asked
            when(mockedRequest.body()).thenReturn(json);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the new data mart
            Routes.updateDatamart(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);

            // Fetch the data mart by id
            Pipeline updatedPipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            Assert.assertEquals(updatedPipeline.getProjections().size(), 2);
            Assert.assertEquals(updatedPipeline.getProjections().get(0).getProjectionVMs().size(), 0);
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().size(), 2);
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().get(0).getFieldValue(), "value3");
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().get(0).getFieldValueMapping(), "value_alias3");
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().get(1).getFieldValue(), "value4");
            Assert.assertEquals(updatedPipeline.getProjections().get(1).getProjectionVMs().get(1).getFieldValueMapping(), "value_alias4");
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
        }
    }

    /**
     * Test duplicate projections.
     */
    @Test
    public void testUpdateDatamartWithDuplicateProjections() throws Exception {
        String pipelineId = createSampleDataMart();

        Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
        Assert.assertEquals(pipeline.getProjections().size(), 1);
        pipeline.setPipelineOozieJobId("fake oozie id");
        // Update the data mart
        ServiceFactory.pipelineService().update(pipeline);

        // JSON to parse
        String json =
                "{                                     " +
                " 'schemaName':'schema1',                      " +
                " 'description':'simple description',  " +
                " 'owner':'john_doe',                  " +
                " 'filter':{                           " +
                "   'condition':'OR',                  " +
                "   'rules':[                          " +
                "     {                                " +
                "       'id':'pty_device',             " +
                "       'field':'pty_device',          " +
                "       'type':'string',               " +
                "       'input':'text',                " +
                "       'operator':'equal',            " +
                "       'value':'us'                   " +
                "     },                               " +
                "     {                                " +
                "       'condition':'AND',             " +
                "       'rules':[                      " +
                "         {                            " +
                "           'id':'referrer_domain',    " +
                "           'field':'referrer_domain', " +
                "           'type':'string',           " +
                "           'input':'text',            " +
                "           'operator':'equal',        " +
                "           'value':'ab'               " +
                "         },                           " +
                "         {                            " +
                "           'id':'ip_version',         " +
                "           'field':'ip_version',      " +
                "           'type':'string',           " +
                "           'input':'text',            " +
                "           'operator':'equal',        " +
                "           'value':'de'               " +
                "         }                            " +
                "       ]                              " +
                "     }                                " +
                "   ]                                  " +
                " },                                   " +
                " 'projections': [                     " +
                "     {                                " +
                "       'column_id':3,                 " +
                "       'aggregate':'SUM',             " +
                        "       'schema_name':'schema1',       " +
                "       'key':'pty_country',           " +
                "       'alias':'col_alias'            " +
                "     },                               " +
                "     {                                " +
                "       'column_id':3,                 " +
                "       'aggregate':'SUM',             " +
                        "       'schema_name':'schema1',       " +
                "       'key':'pty_country',           " +
                "       'alias':'col_alias'            " +
                "     }                                " +
                "  ],                                  " +
                " 'projectionVMs': [                   " +
                "     [                                " +
                "        [ 'value3', 'value_alias3'],  " +
                "        [ 'value4', 'value_alias4']   " +
                "     ],                               " +
                "     [                                " +
                "        [ 'value3', 'value_alias3'],  " +
                "        [ 'value4', 'value_alias4']   " +
                "     ]                                " +
                " ]                                    " +
                "}                                     ";
        // Replace single quote with double quote
        json = json.replaceAll("'", "\"");

        // Mock the request
        Request mockedRequest = mock(Request.class);
        // Mock pipeline id
        when(mockedRequest.params(":id")).thenReturn(pipelineId);
        // Return the JSON when asked
        when(mockedRequest.body()).thenReturn(json);
        // Mock verify the response
        Response mockedResponse = mock(Response.class);
        // Update the new data mart
        Routes.updateDatamart(mockedRequest, mockedResponse);
        // Should receive expected status
        verify(mockedResponse).status(500);

        // Delete the pipeline
        ServiceFactory.pipelineService().delete(Long.parseLong(pipelineId, 10));
    }

    /**
     * Create a simple data mart should succeed.
     */
    @Test
    public void testCreateNewSimpleDatamart() throws Exception {
        // JSON to parse
        String json = "{'name':'Test_Mart',                  " +
                      " 'schemaName':'schema1',              " +
                      " 'description':'simple description',  " +
                      " 'owner':'john_doe',                  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'alias':'col_alias',           " +
                      "       'aggregate':'SUM',             " +
                      "       'schema_name':'schema1',       " +
                      "       'key':'sample_map_key'         " +
                      "     }                                " +
                      " ],                                   " +
                      " 'projectionVMs': [                   " +
                      "     [                                " +
                      "        [ 'value1', 'value_alias1', 'equal'],  " +
                      "        [ 'value2', 'value_alias2', 'equal']   " +
                      "     ]                                " +
                      " ],                                   " +
                      " 'filter':{                           " +
                      "   'condition':'OR',                  " +
                      "   'rules':[                          " +
                      "     {                                " +
                      "       'id':'network_status',         " +
                      "       'field':'network_status',      " +
                      "       'type':'string',               " +
                      "       'input':'text',                " +
                      "       'operator':'equal',            " +
                      "       'value':'us'                   " +
                      "     },                               " +
                      "     {                                " +
                      "       'condition':'AND',             " +
                      "       'rules':[                      " +
                      "         {                            " +
                      "           'id':'property',           " +
                      "           'field':'property',        " +
                      "           'type':'string',           " +
                      "           'input':'text',            " +
                      "           'operator':'equal',        " +
                      "           'value':'ab'               " +
                      "         },                           " +
                      "         {                            " +
                      "           'id':'user_event',         " +
                      "           'field':'user_event',      " +
                      "           'type':'string',           " +
                      "           'input':'text',            " +
                      "           'operator':'equal',        " +
                      "           'value':'de'               " +
                      "         }                            " +
                      "       ]                              " +
                      "     }                                " +
                      "   ]                                  " +
                      " }                                    " +
                      "}                                     ";

        // Should receive 201 status
        String pipelineId = createNewDataMartHelper(json, 201);

        try {
            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));

            // Check that the pipeline name is lowercase
            Assert.assertEquals(pipeline.getPipelineName(), "test_mart");

            // Check value mappings
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(0).getFieldValue(), "value1");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(0).getFieldValueMapping(), "value_alias1");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(1).getFieldValue(), "value2");
            Assert.assertEquals(pipeline.getProjections().get(0).getProjectionVMs().get(1).getFieldValueMapping(), "value_alias2");
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
        }
    }

    /**
     * Create a data mart with invalid JSON should fail.
     */
    @Test
    public void testCreateNewDatamartInvalid() throws Exception {
        // JSON to parse
        String json = "invalid json example";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Data mart requires a name.
     */
    @Test
    public void testCreateNewDatamartMissingName() throws Exception {
        // JSON to parse
        String json = "{'description':'simple description',  " +
                      " 'schemaName':'schema1',                      " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'alias':'col_alias',           " +
                "       'schema_name':'schema1',       " +
                      "       'aggregate':'SUM'              " +
                      "     }                                " +
                      " ]                                    " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Data mart requires a description.
     */
    @Test
    public void testCreateNewDatamartMissingDescription() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'alias':'col_alias',           " +
                "       'schema_name':'schema1',       " +
                      "       'aggregate':'SUM'              " +
                      "     }                                " +
                      " ]                                    " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Data mart requires projections.
     */
    @Test
    public void testCreateNewDatamartMissingProjections() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[]                     " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Data mart projections require a valid column.
     */
    @Test
    public void testCreateNewDatamartProjectionsMissingAndInvalidColumnId() throws Exception {
        // JSON to parse, no column id
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'aggregate':'SUM'              " +
                      "     }                                " +
                      " ]                                    " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);

        // JSON to parse
        json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':-1,                " +
                      "       'aggregate':'SUM'              " +
                      "     }                                " +
                      " ]                                    " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with empty filter should fail.
     */
    @Test
    public void testDatamartWithoutFilter() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'alias':'col_alias',           " +
                "       'schema_name':'schema1',       " +
                      "       'aggregate':'SUM',             " +
                      "       'key':'sample_map_key'         " +
                      "     }                                " +
                      " ],                                   " +
                      " 'filter':{                           " +
                      "   'condition':'AND',                 " +
                      "   'rules':[                          " +
                      "   ]                                  " +
                      " }                                    " +
                      "}                                     ";

        // Should receive 500 status
        createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with duplicate aliases.
     */
    @Test
    public void testCreateDatamartWithDuplicateAliases() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'alias':'ALPHA',               " +
                      "       'aggregate':'SUM',             " +
                "       'schema_name':'schema1',       " +
                      "       'key':'sample_map_key'         " +
                      "     },                               " +
                      "     {                                " +
                      "       'column_id':2,                 " +
                      "       'alias':'ALPHA',               " +
                "       'schema_name':'schema1',       " +
                      "       'aggregate':'SUM',             " +
                      "       'key':'sample_map_key'         " +
                      "     }                                " +
                      " ],                                   " +
                      " 'filter':{                           " +
                      "   'condition':'AND',                 " +
                      "   'rules':[                          " +
                      "     {                                " +
                      "       'id':'pty_family',             " +
                      "       'field':'pty_family',          " +
                      "       'type':'string',               " +
                      "       'input':'text',                " +
                      "       'operator':'equal',            " +
                      "       'value':'us'                   " +
                      "     }                                " +
                      "   ]                                  " +
                      " }                                    " +
                      "}                                     ";

        // Should receive 500 status
        String pipelineId = createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with default keyed duplicate aliases.
     */
    @Test
    public void testCreateDatamartWithDefaultKeyedDuplicateAliases() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'aggregate':'SUM',             " +
                "       'schema_name':'schema1',       " +
                      "       'key':'sample_map_key'         " +
                      "     },                               " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'aggregate':'SUM',             " +
                "       'schema_name':'schema1',       " +
                      "       'key':'sample_map_key'         " +
                      "     }                                " +
                      " ],                                   " +
                      " 'filter':{                           " +
                      "   'condition':'AND',                 " +
                      "   'rules':[                          " +
                      "     {                                " +
                      "       'id':'pty_family',             " +
                      "       'field':'pty_family',          " +
                      "       'type':'string',               " +
                      "       'input':'text',                " +
                      "       'operator':'equal',            " +
                      "       'value':'us'                   " +
                      "     }                                " +
                      "   ]                                  " +
                      " }                                    " +
                      "}                                     ";

        // Should receive 500 status
        String pipelineId = createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with duplicate aliases, key matches top level column.
     */
    @Test
    public void testCreateDatamartWithKeyDuplicateOfTopLevelColumn() throws Exception {
        // JSON to parse
        String json = "{'name':'test_mart',                  " +
                      " 'schemaName':'schema1',                      " +
                      " 'description':'simple description',  " +
                      " 'projections':[                      " +
                      "     {                                " +
                      "       'column_id':1,                 " +
                      "       'aggregate':'SUM',             " +
                "       'schema_name':'schema1',       " +
                      "       'key':'pty_country'            " +
                      "     },                               " +
                      "     {                                " +
                      "       'column_id':2,                 " +
                "       'schema_name':'schema1',       " +
                      "       'aggregate':'SUM'              " +
                      "     }                                " +
                      " ],                                   " +
                      " 'filter':{                           " +
                      "   'condition':'AND',                 " +
                      "   'rules':[                          " +
                      "     {                                " +
                      "       'id':'pty_family',             " +
                      "       'field':'pty_family',          " +
                      "       'type':'string',               " +
                      "       'input':'text',                " +
                      "       'operator':'equal',            " +
                      "       'value':'us'                   " +
                      "     }                                " +
                      "   ]                                  " +
                      " }                                    " +
                      "}                                     ";

        // Should receive 500 status
        String pipelineId = createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with value mapping in wrong format.
     */
    @Test
    public void testCreateDatamartWithWrongVM() throws Exception {
        // JSON to parse
        String json = "{'name':'Test_Mart',                  " +
                " 'schemaName':'schema1',              " +
                " 'description':'simple description',  " +
                " 'owner':'john_doe',                  " +
                " 'projections':[                      " +
                "     {                                " +
                "       'column_id':1,                 " +
                "       'alias':'col_alias',           " +
                "       'schema_name':'schema1',       " +
                "       'aggregate':'SUM',             " +
                "       'key':'sample_map_key'         " +
                "     }                                " +
                " ],                                   " +
                " 'projectionVMs': [                   " +
                "     [                                " +
                "        [ 'value1' ],                 " +
                "        [ 'value2', 'value_alias2']   " +
                "     ]                                " +
                " ]                                    " +
                "}                                     ";

        // Should receive 500 status
        String pipelineId = createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart with ambiguous value mappings.
     */
    @Test
    public void testCreateDatamartWithAmbiguousVM() throws Exception {
        // JSON to parse
        String json = "{'name':'Test_Mart',                  " +
                " 'schemaName':'schema1',              " +
                " 'description':'simple description',  " +
                " 'owner':'john_doe',                  " +
                " 'projections':[                      " +
                "     {                                " +
                "       'column_id':1,                 " +
                "       'alias':'col_alias',           " +
                "       'aggregate':'SUM',             " +
                "       'schema_name':'schema1',       " +
                "       'key':'sample_map_key'         " +
                "     }                                " +
                " ],                                   " +
                " 'projectionVMs': [                   " +
                "     [                                " +
                "        [ 'value1', 'value_alias1', 'equal'], " +
                "        [ 'value1', 'value_alias2', 'equal']   " +
                "     ]                                " +
                " ]                                    " +
                "}                                     ";

        // Should receive 500 status
        String pipelineId = createNewDataMartHelper(json, 500);
    }

    /**
     * Create a data mart, delete the datamart, then restore the datamart.
     */
    @Test
    public void testDeleteAndRestoreDatamart() throws Exception {
        String pipelineId = createSampleDataMart();

        // Fetch the data mart by id
        Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
        Assert.assertFalse(pipeline.getPipelineIsDeleted());

        // Mock the request
        Request mockedRequest = mock(Request.class);
        // Mock pipeline id
        when(mockedRequest.params(":id")).thenReturn(pipelineId);
        // Mock verify the response
        Response mockedResponse = mock(Response.class);

        // Delete the pipeline
        Routes.deleteDatamart(mockedRequest, mockedResponse);
        // Should receive expected status
        verify(mockedResponse).status(200);

        pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
        Assert.assertTrue(pipeline.getPipelineIsDeleted());

        // Delete the pipeline
        ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
    }

    /**
     * Check data mart reset functionality.
     */
    @Test
    public void testResetDatamart() throws Exception {
        String pipelineId = createSampleDataMart();

        // Fetch the data mart by id
        Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
        pipeline.setPipelineStatus(Status.INACTIVE);
        pipeline.setPipelineOozieJobId("A123");
        pipeline.setPipelineOozieBackfillJobId("B123");

        // Mock the request
        Request mockedRequest = mock(Request.class);
        // Mock pipeline id
        when(mockedRequest.params(":id")).thenReturn(pipelineId);
        // Mock verify the response
        Response mockedResponse = mock(Response.class);

        // Reset the pipeline
        Routes.restoreDatamart(mockedRequest, mockedResponse);
        // Should receive expected status
        verify(mockedResponse).status(200);
        pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));

        // Check pipeline info
        Assert.assertEquals(pipeline.getPipelineStatus(), Status.INACTIVE);
        Assert.assertNull(pipeline.getPipelineOozieJobId());
        Assert.assertNull(pipeline.getPipelineOozieBackfillJobId());

        // Delete the pipeline
        ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
    }

    /**
     * Test clone of a data mart.
     */
    @Test
    public void testCloneDatamart() throws Exception {
        // Create the test data mart
        String pipelineId = createSampleDataMart();
        boolean deleted = false;
        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(pipelineId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);

            // Clone the pipeline
            ModelAndView modelAndView = Routes.cloneDatamart(mockedRequest, mockedResponse);
            // Get params
            Map<String, Object> params = (Map<String, Object>) modelAndView.getModel();

            // Check the parameters
            Assert.assertEquals(params.get("pipelineName"), "test_mart");
            Assert.assertEquals(params.get("pipelineDescription"), "simple description");
            Assert.assertEquals(params.get("pipelineOwner"), "john_doe");
            Assert.assertNotNull(params.get("projections"));
            Assert.assertNotNull(params.get("filters"));

            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
            deleted = true;

            modelAndView = Routes.cloneDatamart(mockedRequest, mockedResponse);
            // Get params
            params = (Map<String, Object>) modelAndView.getModel();
            // Check the parameters
            Assert.assertNotEquals(params.get("error"), null);

            when(mockedRequest.params(":id")).thenReturn("invalid id");
            modelAndView = Routes.cloneDatamart(mockedRequest, mockedResponse);
            // Get params
            params = (Map<String, Object>) modelAndView.getModel();
            // Check the parameters
            Assert.assertEquals(params.get("error"), "Error parsing pipeline id.");
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
            }
        }
    }

    /**
     * Create a simple data mart and get the ETL code.
     */
    @Test
    public void testGetETLCode() throws Exception {
        // Create a new sample data mart
        String pipelineId = createSampleDataMart();

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);

            // Return the JSON when asked
            when(mockedRequest.params(":id")).thenReturn(pipelineId);

            // Mock verify the response
            Response mockedResponse = mock(Response.class);

            // Create the new data mart
            String result = Routes.viewDatamartEtl(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
        }
    }

    /**
     * View all bundle ids.
     */
    @Test
    public void testViewBundleIds() throws Exception {
        // Create a new sample data mart
        String pipelineId = createSampleDataMart();

        try {
            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(Long.parseLong(pipelineId, 10));
            // Set the bundle ids
            pipeline.setPipelineOozieJobId("0000000-000000000000000-oozie_AA-B");
            pipeline.setPipelineOozieBackfillJobId("1111111-111111111111111-oozie_AA-B");
            // Update the data mart
            ServiceFactory.pipelineService().update(pipeline);

            // Mock the request
            Request mockedRequest = mock(Request.class);

            // Mock verify the response
            Response mockedResponse = mock(Response.class);

            // Retrive bundle ids
            ModelAndView modelAndView = Routes.allBundleIds(mockedRequest, mockedResponse);

            // Get params
            Map<String, Object> params = (Map<String, Object>) modelAndView.getModel();


            // Load example text file
            String expected = new String(Files.readAllBytes(Paths.get("src/test/resources/all-bundle-ids.txt")));

            // Should receive expected status
            Assert.assertEquals(params.get("bundle"), expected);
        } finally {
            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.valueOf(pipelineId));
        }
    }

    /**
     * Helper function to create a new funnel.
     * @param json Input JSON
     * @param expectedStatus Expected HTTP return status
     */
    private static String createNewFunnelHelper(String json, int expectedStatus) throws Exception {
        // Mock the request
        Request mockedRequest = mock(Request.class);

        // Replace single quote with double quote
        json = json.replaceAll("'", "\"");

        // Return the JSON when asked
        when(mockedRequest.body()).thenReturn(json);

        // Mock verify the response
        Response mockedResponse = mock(Response.class);

        // Create the new data mart
        String result = Routes.createNewFunnel(mockedRequest, mockedResponse);

        // Should receive expected status
        verify(mockedResponse).status(expectedStatus);

        return result;
    }

    private String createSampleFunnel() throws Exception {
        // JSON to parse
        String json = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        // Should receive 201 status
        return createNewFunnelHelper(json, 201);
    }

    /**
     * stopFunnel test.
     */
    @Test
    public void stopFunnelTest() throws Exception {
        String funnelId = createSampleFunnel();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(funnelId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.stopPipeline(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            // Delete the pipeline
            ServiceFactory.pipelineService().delete(Long.parseLong(funnelId, 10));
            deleted = true;

            Routes.stopPipeline(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(funnelId, 10));
            }
        }
    }

    /**
     * Test newFunnelGroup.
     */
    @Test
    public void newFunnelGroupTest() throws Exception {

        // Mock the request
        Request mockedRequest = mock(Request.class);
        when(mockedRequest.queryParams("schemaName")).thenReturn("schema1");
        // Mock verify the response
        Response mockedResponse = mock(Response.class);
        // View
        ModelAndView newFunnelGroupPage = Routes.newFunnelGroupGet(mockedRequest, mockedResponse);
        String expected = TemplateTestUtils.loadTemplateInstance("templates/new_view_funnel_group_expected.html");

        Assert.assertEquals(templateEngine.render(newFunnelGroupPage).replaceAll("\\s", ""), expected.replaceAll("\\s", ""));

    }

    /**
     * Test runFunnelQuery with zipped results.
     */
    @Test
    public void runFunnelQueryZipResultsTest() throws Exception {
        String json = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        // Mock the request
        Request mockedRequest1 = mock(Request.class);
        when(mockedRequest1.body()).thenReturn(json);
        // Mock verify the response
        Response mockedResponse1 = mock(Response.class);
        HiveConnectionManager mockedHiveConnectionManager = mock(HiveConnectionManager.class);
        Routes.hiveConnector = mockedHiveConnectionManager;
        Routes.SHOULD_RESET_HIVE_CONNECTOR = false;
        // Test normal scenario
        FunnelQueryResult funnelQueryResult1 = new FunnelQueryResult();
        funnelQueryResult1.getKeys().add("4");
        funnelQueryResult1.getKeys().add("ios");
        funnelQueryResult1.getValues().add("100");
        funnelQueryResult1.getValues().add("50");
        FunnelQueryResult funnelQueryResult2 = new FunnelQueryResult();
        funnelQueryResult2.getKeys().add("4");
        funnelQueryResult2.getKeys().add("ios");
        funnelQueryResult2.getValues().add("300");
        ArrayList<FunnelQueryResult> listFunnelQueryResult1 = new ArrayList<>();
        listFunnelQueryResult1.add(funnelQueryResult1);
        listFunnelQueryResult1.add(funnelQueryResult2);
        when(mockedHiveConnectionManager.execute(anyString())).thenReturn(listFunnelQueryResult1);
        String result = Routes.runFunnelQuery(mockedRequest1, mockedResponse1);
        verify(mockedResponse1).status(200);
        Assert.assertEquals(result, "[{\"keys\":[\"4\",\"ios\"],\"values\":[\"400\",\"50\"]}]");
        // Test the edge cases that each step is zero
        // The request and response for this case
        Request mockedRequest2 = mock(Request.class);
        when(mockedRequest2.body()).thenReturn(json);
        Response mockedResponse2 = mock(Response.class);
        FunnelQueryResult funnelQueryResult3 = new FunnelQueryResult();
        funnelQueryResult3.getKeys().add("4");
        funnelQueryResult3.getKeys().add("ios");
        funnelQueryResult3.getValues().add("");
        FunnelQueryResult funnelQueryResult4 = new FunnelQueryResult();
        funnelQueryResult4.getKeys().add("4");
        funnelQueryResult4.getKeys().add("ios");
        funnelQueryResult4.getValues().add("");
        ArrayList<FunnelQueryResult> listFunnelQueryResult2 = new ArrayList<>();
        listFunnelQueryResult2.add(funnelQueryResult3);
        listFunnelQueryResult2.add(funnelQueryResult4);
        when(mockedHiveConnectionManager.execute(anyString())).thenReturn(listFunnelQueryResult2);
        result = Routes.runFunnelQuery(mockedRequest2, mockedResponse2);
        verify(mockedResponse2).status(200);
        Assert.assertEquals(result, "[{\"keys\":[\"4\",\"ios\"]}]");
    }


    /**
     * Test runFunnelQuery.
     */
    @Test
    public void runFunnelQueryTest() throws Exception {
        String json = TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json");
        // Mock the request
        Request mockedRequest = mock(Request.class);
        when(mockedRequest.body()).thenReturn(json);
        // Mock verify the response
        Response mockedResponse = mock(Response.class);
        HiveConnectionManager mockedHiveConnectionManager = mock(HiveConnectionManager.class);
        Routes.hiveConnector = mockedHiveConnectionManager;
        Routes.SHOULD_RESET_HIVE_CONNECTOR = false;
        FunnelQueryResult funnelQueryResult = new FunnelQueryResult();
        funnelQueryResult.getValues().add("100");
        funnelQueryResult.getValues().add("50");
        ArrayList<FunnelQueryResult> listFunnelQueryResult = new ArrayList<>();
        listFunnelQueryResult.add(funnelQueryResult);
        when(mockedHiveConnectionManager.execute(anyString())).thenReturn(listFunnelQueryResult);
        String result = Routes.runFunnelQuery(mockedRequest, mockedResponse);
        verify(mockedResponse).status(200);
        Assert.assertEquals(result, "[{\"values\":[\"100\",\"50\"]}]");

        when(mockedHiveConnectionManager.execute(anyString())).thenReturn(null);
        result = Routes.runFunnelQuery(mockedRequest, mockedResponse);
        verify(mockedResponse, times(1)).status(500);
        Assert.assertEquals(result, "Hive connector could not fetch results");

    }

    /**
     * Test getStatus.
     */
    @Test
    public void getStatusTest() throws Exception {
        Request mockedRequest = mock(Request.class);
        Response mockedResponse = mock(Response.class);
        String status = Routes.getStatus(mockedRequest, mockedResponse);
        Assert.assertEquals(status, Routes.OK_STATUS);
    }

    /**
     * Helper function to create a new funnel.
     * @param json Input JSON
     * @param expectedStatus Expected HTTP return status
     */
    private static String createNewFunnelGroupHelper(String json, int expectedStatus) {
        // Mock the request
        Request mockedRequest = mock(Request.class);

        // Return the JSON when asked
        when(mockedRequest.body()).thenReturn(json);

        // Mock verify the response
        Response mockedResponse = mock(Response.class);

        // Create the new funnel group
        String result = Routes.createNewFunnelGroup(mockedRequest, mockedResponse);

        // Should receive expected status
        verify(mockedResponse).status(expectedStatus);

        return result;
    }

    private String createSampleFunnelGroup() throws Exception {
        // JSON to parse
        //                     START
        //                       |
        //                     step1
        //             /                 \
        //          step2       ->      step3
        //             \                 /
        //                     step4
        // There are 3 funnels:
        // 1) START -> step1 -> step2 -> step4
        // 2) START -> step1 -> step2 -> step3 -> step4
        // 3) START -> step1 -> step3 -> step4
        String json = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_multi_funnel_json_req_1.json");

        // Should receive 201 status
        return createNewFunnelGroupHelper(json, 201);
    }

    /**
     * Create a simple funnel group should succeed.
     * @throws Exception
     */
    @Test
    public void testCreateNewSimpleFunnelGroup() throws Exception {

        long funnelGroupId = Long.parseLong(createSampleFunnelGroup(), 10);

        // Fetch the funnel group by id
        FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

        // Check that the funnel group name is lowercase
        Assert.assertEquals(funnelGroup.getFunnelGroupName(), "test");

        // Check the projections and funnels count
        Assert.assertEquals(funnelGroup.getProjections().size(), 2);
        Assert.assertEquals(funnelGroup.getPipelines().size(), 3);

        // Check each funnel
        Pipeline pipeline0 = ServiceFactory.pipelineService().fetchByName("test_funnel1");
        Pipeline pipeline1 = ServiceFactory.pipelineService().fetchByName("test_funnel2");
        Pipeline pipeline2 = ServiceFactory.pipelineService().fetchByName("test_funnel3");

        String projectionsExpected = "[geo_info['country'] AS country, user_logged_in AS logged_in]";

        // pipeline0: step1 -> step3 -> step4
        String funnelGroupStepsJsonExpected0 = TestUtils.STEP1 + TestUtils.STEP3 + TestUtils.STEP4;
        Assert.assertEquals(pipeline0.getPipelineName(), "test_funnel1");
        Assert.assertEquals(pipeline0.getPipelineSchemaName(), "schema1");
        Assert.assertEquals(pipeline0.getPipelineDescription(), "test");
        Assert.assertEquals(pipeline0.getPipelineOwner(), "john_doe");
        Assert.assertEquals(pipeline0.getFunnelUserIdField(), "cookie_one");
        Assert.assertEquals(pipeline0.getPipelineBackfillStartTime(), "2020-05-07");
        Assert.assertNull(pipeline0.getPipelineEndTime());
        Assert.assertEquals(pipeline0.getFunnelQueryRange(), "1");
        Assert.assertEquals(pipeline0.getFunnelRepeatInterval(), "1");
        Assert.assertEquals(pipeline0.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline0.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected0.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline0.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep3\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline0.getProjections().toString(), projectionsExpected);

        // pipeline1: step1 -> step2 -> step4
        String funnelGroupStepsJsonExpected1 = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP4;
        Assert.assertEquals(pipeline1.getPipelineName(), "test_funnel2");
        Assert.assertEquals(pipeline1.getPipelineSchemaName(), "schema1");
        Assert.assertEquals(pipeline1.getPipelineDescription(), "test");
        Assert.assertEquals(pipeline1.getPipelineOwner(), "john_doe");
        Assert.assertEquals(pipeline1.getFunnelUserIdField(), "cookie_one");
        Assert.assertEquals(pipeline1.getPipelineBackfillStartTime(), "2020-05-07");
        Assert.assertNull(pipeline1.getPipelineEndTime());
        Assert.assertEquals(pipeline1.getFunnelQueryRange(), "1");
        Assert.assertEquals(pipeline1.getFunnelRepeatInterval(), "1");
        Assert.assertEquals(pipeline1.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline1.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected1.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline1.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline1.getProjections().toString(), projectionsExpected);

        // pipeline2: step1 -> step2 -> step3 -> step4
        String funnelGroupStepsJsonExpected2 = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP3 + TestUtils.STEP4;
        Assert.assertEquals(pipeline2.getPipelineName(), "test_funnel3");
        Assert.assertEquals(pipeline2.getPipelineSchemaName(), "schema1");
        Assert.assertEquals(pipeline2.getPipelineDescription(), "test");
        Assert.assertEquals(pipeline2.getPipelineOwner(), "john_doe");
        Assert.assertEquals(pipeline2.getFunnelUserIdField(), "cookie_one");
        Assert.assertEquals(pipeline2.getPipelineBackfillStartTime(), "2020-05-07");
        Assert.assertNull(pipeline2.getPipelineEndTime());
        Assert.assertEquals(pipeline2.getFunnelQueryRange(), "1");
        Assert.assertEquals(pipeline2.getFunnelRepeatInterval(), "1");
        Assert.assertEquals(pipeline2.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline2.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected2.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline2.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep3\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline2.getProjections().toString(), projectionsExpected);

        // Delete the funnel group
        ServiceFactory.funnelGroupService().delete(funnelGroupId);
    }

    /**
     * Test update a funnel group with different steps and funnels.
     * @throws Exception
     */
    @Test
    public void testUpdateFunnelGroup() throws Exception {
        String funnelGroupId = createSampleFunnelGroup();
        boolean deleted = false;
        try {
            // The old funnel group:
            //                     START
            //                       |
            //                     step1
            //                 /         \
            //              step2  ->   step3
            //                 \          /
            //                     step4
            // There are 3 funnels:
            // 1) START -> step1 -> step2 -> step4
            // 2) START -> step1 -> step2 -> step3 -> step4
            // 3) START -> step1 -> step3 -> step4
            //
            // The new funnel group:
            //                                START
            //                                  |
            //                                step1
            //                             /        \
            //                          step2  <-   step3
            //                         /      \         /
            //                       step5         step4
            // There are 4 funnels:
            // 1) START -> step1 -> step2 -> step4
            // 2) START -> step1 -> step2 -> step5
            // 3) START -> step1 -> step3 -> step4
            // 4) START -> step1 -> step3 -> step2 -> step4
            // 5) START -> step1 -> step3 -> step2 -> step5

            String json = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_update_json_req.json");

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock pipeline id
            when(mockedRequest.params(":id")).thenReturn(funnelGroupId);
            // Return the JSON when asked
            when(mockedRequest.body()).thenReturn(json);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Update the funnel group
            String errMsg = Routes.updateFunnelGroup(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);

            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(Long.parseLong(funnelGroupId, 10));

            // Verifications
            Assert.assertEquals(funnelGroup.getFunnelGroupDescription(), "test updated");

            // Check the projections and funnels count
            Assert.assertEquals(funnelGroup.getProjections().size(), 2);
            Assert.assertEquals(funnelGroup.getPipelines().size(), 5);

            // Check each funnel
            Pipeline pipeline0 = ServiceFactory.pipelineService().fetchByName("test_funnel1");
            Pipeline pipeline1 = ServiceFactory.pipelineService().fetchByName("test_funnel2");
            Pipeline pipeline2 = ServiceFactory.pipelineService().fetchByName("test_funnel3");
            Pipeline pipeline3 = ServiceFactory.pipelineService().fetchByName("test_funnel4");
            Pipeline pipeline4 = ServiceFactory.pipelineService().fetchByName("test_funnel5");

            // pipeline0: step1 -> step2 -> step4
            String funnelGroupStepsJsonExpected0 = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP4;
            Assert.assertEquals(pipeline0.getPipelineName(), "test_funnel1");
            Assert.assertEquals(pipeline0.getPipelineSchemaName(), "schema1");
            Assert.assertEquals(pipeline0.getPipelineDescription(), "test updated");
            Assert.assertEquals(pipeline0.getPipelineOwner(), "john_doe");
            Assert.assertEquals(pipeline0.getFunnelUserIdField(), "cookie_one");
            Assert.assertEquals(pipeline0.getPipelineBackfillStartTime(), "2020-06-01");
            Assert.assertNull(pipeline0.getPipelineEndTime());
            Assert.assertEquals(pipeline0.getFunnelQueryRange(), "1");
            Assert.assertEquals(pipeline0.getFunnelRepeatInterval(), "1");
            Assert.assertEquals(pipeline0.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline0.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected0.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline0.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep4".replaceAll("\\s", ""));
            Assert.assertEquals(pipeline0.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

            // pipeline1: step1 -> step2 -> step5
            String funnelGroupStepsJsonExpected1 = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP5;
            Assert.assertEquals(pipeline1.getPipelineName(), "test_funnel2");
            Assert.assertEquals(pipeline1.getPipelineSchemaName(), "schema1");
            Assert.assertEquals(pipeline1.getPipelineDescription(), "test updated");
            Assert.assertEquals(pipeline1.getPipelineOwner(), "john_doe");
            Assert.assertEquals(pipeline1.getFunnelUserIdField(), "cookie_one");
            Assert.assertEquals(pipeline1.getPipelineBackfillStartTime(), "2020-06-01");
            Assert.assertNull(pipeline1.getPipelineEndTime());
            Assert.assertEquals(pipeline1.getFunnelQueryRange(), "1");
            Assert.assertEquals(pipeline1.getFunnelRepeatInterval(), "1");
            Assert.assertEquals(pipeline1.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline1.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected1.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline1.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep5".replaceAll("\\s", ""));
            Assert.assertEquals(pipeline1.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

            // pipeline2: step1 -> step3 -> step4
            String funnelGroupStepsJsonExpected2 = TestUtils.STEP1 + TestUtils.STEP3 + TestUtils.STEP4;
            Assert.assertEquals(pipeline2.getPipelineName(), "test_funnel3");
            Assert.assertEquals(pipeline2.getPipelineSchemaName(), "schema1");
            Assert.assertEquals(pipeline2.getPipelineDescription(), "test updated");
            Assert.assertEquals(pipeline2.getPipelineOwner(), "john_doe");
            Assert.assertEquals(pipeline2.getFunnelUserIdField(), "cookie_one");
            Assert.assertEquals(pipeline2.getPipelineBackfillStartTime(), "2020-06-01");
            Assert.assertNull(pipeline2.getPipelineEndTime());
            Assert.assertEquals(pipeline2.getFunnelQueryRange(), "1");
            Assert.assertEquals(pipeline2.getFunnelRepeatInterval(), "1");
            Assert.assertEquals(pipeline2.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline2.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected2.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline2.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep3\n\nstep4".replaceAll("\\s", ""));
            Assert.assertEquals(pipeline2.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

            // pipeline3: START -> step1 -> step3 -> step2 -> step4
            String funnelGroupStepsJsonExpected3 = TestUtils.STEP1 + TestUtils.STEP3 + TestUtils.STEP2 + TestUtils.STEP4;
            Assert.assertEquals(pipeline3.getPipelineName(), "test_funnel4");
            Assert.assertEquals(pipeline3.getPipelineSchemaName(), "schema1");
            Assert.assertEquals(pipeline3.getPipelineDescription(), "test updated");
            Assert.assertEquals(pipeline3.getPipelineOwner(), "john_doe");
            Assert.assertEquals(pipeline3.getFunnelUserIdField(), "cookie_one");
            Assert.assertEquals(pipeline3.getPipelineBackfillStartTime(), "2020-06-01");
            Assert.assertNull(pipeline3.getPipelineEndTime());
            Assert.assertEquals(pipeline3.getFunnelQueryRange(), "1");
            Assert.assertEquals(pipeline3.getFunnelRepeatInterval(), "1");
            Assert.assertEquals(pipeline3.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline3.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected3.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline3.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep3\n\nstep2\n\nstep4".replaceAll("\\s", ""));
            Assert.assertEquals(pipeline3.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

            // pipeline4: START -> step1 -> step3 -> step2 -> step5
            String funnelGroupStepsJsonExpected4 = TestUtils.STEP1 + TestUtils.STEP3 + TestUtils.STEP2 + TestUtils.STEP5;
            Assert.assertEquals(pipeline4.getPipelineName(), "test_funnel5");
            Assert.assertEquals(pipeline4.getPipelineSchemaName(), "schema1");
            Assert.assertEquals(pipeline4.getPipelineDescription(), "test updated");
            Assert.assertEquals(pipeline4.getPipelineOwner(), "john_doe");
            Assert.assertEquals(pipeline4.getFunnelUserIdField(), "cookie_one");
            Assert.assertEquals(pipeline4.getPipelineBackfillStartTime(), "2020-06-01");
            Assert.assertNull(pipeline4.getPipelineEndTime());
            Assert.assertEquals(pipeline4.getFunnelQueryRange(), "1");
            Assert.assertEquals(pipeline4.getFunnelRepeatInterval(), "1");
            Assert.assertEquals(pipeline4.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline4.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected4.replaceAll("\\s", ""));
            Assert.assertEquals(pipeline4.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep3\n\nstep2\n\nstep5".replaceAll("\\s", ""));
            Assert.assertEquals(pipeline4.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

            // Delete the funnel group
            ServiceFactory.funnelGroupService().delete(Long.parseLong(funnelGroupId, 10));
            deleted = true;

            // Update the deleted data mart
            Routes.updateFunnelGroup(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.funnelGroupService().delete(Long.parseLong(funnelGroupId, 10));
            }
        }

    }

    /**
     * Create a funnel group, delete the funnel group, then restore the funnel group.
     */
    @Test
    public void testDeleteAndRestoreFunnelGroup() throws Exception {

        String funnelGroupId = createSampleFunnelGroup();

        try {
            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(Long.parseLong(funnelGroupId, 10));
            Assert.assertFalse(funnelGroup.getFunnelGroupIsDeleted());

            // Mock the request
            Request mockedRequest = mock(Request.class);
            // Mock funnel group id
            when(mockedRequest.params(":id")).thenReturn(funnelGroupId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);

            // Delete the funnel group
            Routes.deleteFunnelGroup(mockedRequest, mockedResponse);
            // Should receive expected status
            verify(mockedResponse).status(200);

            funnelGroup = ServiceFactory.funnelGroupService().fetch(Long.parseLong(funnelGroupId, 10));
            Assert.assertTrue(funnelGroup.getFunnelGroupIsDeleted());

            Pipeline pipeline0 = ServiceFactory.pipelineService().fetchByName("test_funnel1");
            Assert.assertTrue(pipeline0.getPipelineIsDeleted());
            Pipeline pipeline1 = ServiceFactory.pipelineService().fetchByName("test_funnel2");
            Assert.assertTrue(pipeline1.getPipelineIsDeleted());
            Pipeline pipeline2 = ServiceFactory.pipelineService().fetchByName("test_funnel3");
            Assert.assertTrue(pipeline2.getPipelineIsDeleted());
        } finally {
            // Delete the funnel group
            ServiceFactory.funnelGroupService().delete(Long.valueOf(funnelGroupId));
        }
    }

    /**
     * Test stop funnel group.
     */
    @Test
    public void testStopFunnelGroup() throws Exception {
        String funnelGroupId = createSampleFunnelGroup();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(funnelGroupId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            Routes.stopFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            // Delete the pipeline
            ServiceFactory.funnelGroupService().delete(Long.parseLong(funnelGroupId, 10));
            deleted = true;

            Routes.stopFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the pipeline
                ServiceFactory.pipelineService().delete(Long.parseLong(funnelGroupId, 10));
            }
        }
    }

    /**
     * Test launch funnel group.
     */
    @Test
    public void testLaunchFunnelGroup() throws Exception {
        // Create a new sample funnel group
        String funnelGroupId = createSampleFunnelGroup();

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(funnelGroupId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            FunnelGroupTemplateGenerator mockedFunnelGroupTemplateGenerator = mock(FunnelGroupTemplateGenerator.class);

            Routes.funnelGroupTemplateGenerator = mockedFunnelGroupTemplateGenerator;
            Routes.pipelineLauncherManager = new PipelineLauncherManager();
            when(mockedFunnelGroupTemplateGenerator.generateTemplateFiles(any(FunnelGroup.class), anyString(), anyInt())).thenReturn("test dir");

            // Launch funnel group
            String errMsg = Routes.launchFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse, times(1)).status(500);

            // Clean test folder in /tmp/cubed
            File templateOutputFolder = new File(CLISettings.TEMPLATE_OUTPUT_FOLDER);
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }

            PipelineLauncherManager mockedPipelineLauncherManager = mock(PipelineLauncherManager.class);
            PipelineLauncher.LaunchStatus mockedLaunchStatus = mock(PipelineLauncher.LaunchStatus.class);
            mockedLaunchStatus.hasError = true;
            mockedLaunchStatus.errorMsg = "Test Error Message";
            PowerMockito.when(mockedPipelineLauncherManager.
                    launchPipeline(anyString(), anyString(), anyString(), anyString(), anyString(), anyBoolean(), anyString(), anyString()))
                    .thenReturn(CompletableFuture.completedFuture(mockedLaunchStatus));
            Routes.pipelineLauncherManager = mockedPipelineLauncherManager;
            errMsg = Routes.launchFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse, times(2)).status(500);
            Assert.assertEquals(errMsg, "Test Error Message");

            // Clean test folder in /tmp/cubed
            if (templateOutputFolder.exists()) {
                FileUtils.deleteDirectory(templateOutputFolder);
                templateOutputFolder.mkdir();
            }
            mockedLaunchStatus.hasError = false;
            mockedLaunchStatus.oozieJobId = "test id";
            errMsg = Routes.launchFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse, times(1)).status(200);
            Assert.assertEquals(errMsg, "");
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(Long.valueOf(funnelGroupId));
            Assert.assertEquals(funnelGroup.getFunnelGroupStatus(), "Deployed v1");
            Assert.assertEquals(funnelGroup.getFunnelGroupOozieJobId(), "test id");

            when(mockedRequest.params(":id")).thenReturn("");
            errMsg = Routes.launchFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse, times(3)).status(500);
            Assert.assertEquals(errMsg, "For input string: \"\"");
        } finally {
            // Delete the pipeline
            ServiceFactory.funnelGroupService().delete(Long.valueOf(funnelGroupId));
        }
    }

    /**
     * Download funnel group test.
     */
    @Test
    public void testDownloadFunnelGroup() throws Exception {
        // Create a new sample funnel group
        String funnelGroupId = createSampleFunnelGroup();
        boolean deleted = false;

        try {
            // Mock the request
            Request mockedRequest = mock(Request.class);
            when(mockedRequest.params(":id")).thenReturn(funnelGroupId);
            // Mock verify the response
            Response mockedResponse = mock(Response.class);
            // Mock response and outstream
            HttpServletResponse mockedResponseRaw = mock(HttpServletResponse.class);
            ServletOutputStream mockedOutstream = mock(ServletOutputStream.class);
            when(mockedResponseRaw.getOutputStream()).thenReturn(mockedOutstream);
            when(mockedResponse.raw()).thenReturn(mockedResponseRaw);
            // Download funnel group
            Routes.downloadFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse).status(200);

            // Delete the pipeline
            ServiceFactory.funnelGroupService().delete(Long.valueOf(funnelGroupId));
            deleted = true;

            Routes.downloadFunnelGroup(mockedRequest, mockedResponse);
            verify(mockedResponse).status(500);
        } finally {
            if (!deleted) {
                // Delete the funnel group
                ServiceFactory.funnelGroupService().delete(Long.valueOf(funnelGroupId));
            }
        }
    }
}
