/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.yahoo.cubed.App;
import com.yahoo.cubed.json.FunnelQuery;
import com.yahoo.cubed.json.NewDatamart;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.source.ConfigurationLoader;
import com.yahoo.cubed.templating.TemplateTestUtils;
import com.yahoo.cubed.TestUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Collections;

/**
 * Test for Utils.
 */
public class UtilsTest {
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
     * validateProjections Test With Empty Projections.
     * @throws Exception
     */
    @Test
    public void validateProjectionsTestWithEmptyProjections() throws Exception {
        String result = Utils.validateProjections(new ArrayList<>());
        Assert.assertEquals(result, "Need at least one dimension or metric");
    }

    /**
     * validateProjections Test With Invalid Projections.
     * @throws Exception
     */
    @Test
    public void validateProjectionsTestWithInvalidProjections() throws Exception {
        List<Map<String, String>> projections = new ArrayList<>();
        Map<String, String> projection = new HashMap<>();
        projections.add(projection);
        String result = Utils.validateProjections(projections);
        Assert.assertEquals(result, "Error while parsing new data mart");
    }

    /**
     * validateProjections Test With various column alias.
     * @throws Exception
     */
    @Test
    public void validateProjectionsTestWithDuplicateColumnAlias() throws Exception {
        List<Map<String, String>> projections = new ArrayList<>();

        Map<String, String> projection1 = new HashMap<>();
        projection1.put(NewDatamart.COLUMN_ID, "2");
        projection1.put(NewDatamart.SCHEMA_NAME, "schema1");
        projections.add(projection1);

        Map<String, String> projection2 = new HashMap<>();
        projection2.put(NewDatamart.COLUMN_ID, "7");
        projection2.put(NewDatamart.KEY, "cookie_one");
        projection2.put(NewDatamart.SCHEMA_NAME, "schema1");
        projections.add(projection2);

        String result = Utils.validateProjections(projections);
        Assert.assertEquals(result, "Duplicate column alias:cookie_one");
    }

    /**
     * projectionsHaveAtLeastOneAggregate Test.
     * @throws Exception
     */
    @Test
    public void projectionsHaveAtLeastOneAggregateTest() throws Exception {
        List<Map<String, String>> projections = new ArrayList<>();
        Map<String, String> projection = new HashMap<>();
        projections.add(projection);
        String result = Utils.projectionsHaveAtLeastOneAggregate(projections);
        Assert.assertEquals(result, "Projection must have at least one aggregate");

        projections.clear();
        projection.put(NewDatamart.AGGREGATE, "SUM");
        projections.add(projection);
        result = Utils.projectionsHaveAtLeastOneAggregate(projections);
        Assert.assertNull(result);
    }

    /**
     * setAlias Test.
     * @throws Exception
     */
    @Test
    public void setAliasTest() throws Exception {
        List<Map<String, String>> projections = new ArrayList<>();
        Map<String, String> projection = new HashMap<>();
        projection.put(FunnelQuery.COLUMN_ID, "9");
        projection.put(FunnelQuery.KEY, "test");
        projection.put(FunnelQuery.SCHEMA_NAME, "schema1");
        projections.add(projection);
        projections = Utils.setAliases(projections);
        Assert.assertEquals(projections.size(), 1);
        Assert.assertEquals(projections.get(0).get(FunnelQuery.ALIAS), "random_info_two_test");
    }

    /**
     * generateStepNamesForEachFunnel Test.
     */
    @Test
    public void generateStepNamesForEachFunnelTest() {
        //   case #1
        //   1   4
        //    \ /
        //     2
        //    / \
        //   3  5
        List<String[]> list = new ArrayList<>();
        list.add(new String[]{"1", "2"});
        list.add(new String[]{"2", "3"});
        list.add(new String[]{"4", "2"});
        list.add(new String[]{"2", "5"});

        List<String> result = Utils.generateStepNamesForEachFunnel(list);
        Collections.sort(result);
        List<String> expected = new ArrayList<>(Arrays.asList("1\n\n2\n\n3", "1\n\n2\n\n5", "4\n\n2\n\n3", "4\n\n2\n\n5"));
        Collections.sort(expected);

        Assert.assertEquals(result, expected);

        //           case #2
        //       1        2       3
        //    /    \    /      /     \
        //  4        5       6   ->   7
        //         /   \   /
        //       8       9
        list = new ArrayList<>();
        list.add(new String[]{"1", "4"});
        list.add(new String[]{"1", "5"});
        list.add(new String[]{"2", "5"});
        list.add(new String[]{"3", "6"});
        list.add(new String[]{"3", "7"});
        list.add(new String[]{"6", "7"});
        list.add(new String[]{"5", "8"});
        list.add(new String[]{"5", "9"});
        list.add(new String[]{"6", "9"});

        result = Utils.generateStepNamesForEachFunnel(list);
        Collections.sort(result);
        expected = new ArrayList<>(Arrays.asList("1\n\n4", "1\n\n5\n\n8", "1\n\n5\n\n9", "2\n\n5\n\n8", "2\n\n5\n\n9", "3\n\n6\n\n9", "3\n\n6\n\n7", "3\n\n7"));
        Collections.sort(expected);

        Assert.assertEquals(result, expected);
    }

    /**
     * getFunnelJsonReq Test.
     */
    @Test
    public void getFunnelJsonReqTest() throws Exception {

        String funnelGroupJsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_single_funnel_json_req.json");

        String step1 = "{\n" +
                "      \"name\": \"step3\",\n" +
                "      \"condition\": \"AND\",\n" +
                "      \"rules\": [\n" +
                "        {\n" +
                "          \"id\": \"user_event\",\n" +
                "          \"field\": \"user_event\",\n" +
                "          \"type\": \"string\",\n" +
                "          \"input\": \"text\",\n" +
                "          \"operator\": \"equal\",\n" +
                "          \"value\": \"event3\"\n" +
                "        },\n" +
                "        {\n" +
                "          \"condition\": \"AND\",\n" +
                "          \"rules\": [\n" +
                "            {\n" +
                "              \"id\": \"cookie_one_age\",\n" +
                "              \"field\": \"cookie_one_age\",\n" +
                "              \"type\": \"string\",\n" +
                "              \"input\": \"text\",\n" +
                "              \"operator\": \"equal\",\n" +
                "              \"value\": \"10\"\n" +
                "            },\n" +
                "            {\n" +
                "              \"id\": \"cookie_one_info\",\n" +
                "              \"field\": \"cookie_one_info\",\n" +
                "              \"type\": \"string\",\n" +
                "              \"input\": \"text\",\n" +
                "              \"operator\": \"is_not_null\",\n" +
                "              \"value\": null\n" +
                "            }\n" +
                "          ]\n" +
                "        }\n" +
                "      ]\n" +
                "    }";


        String step2 = "{\n" +
                "      \"name\": \"step4\",\n" +
                "      \"condition\": \"AND\",\n" +
                "      \"rules\": [\n" +
                "        {\n" +
                "          \"id\": \"user_event\",\n" +
                "          \"field\": \"user_event\",\n" +
                "          \"type\": \"string\",\n" +
                "          \"input\": \"text\",\n" +
                "          \"operator\": \"equal\",\n" +
                "          \"value\": \"event4\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }";

        String step2WithBlank = "{\n" +
                "      \"name\": \"step4\",\n" +
                "      \"condition\": \"AND\",\n" +
                "      \"rules\": [\n" +
                "        {\n" +
                "          \"id\": \"user_event\",\n" +
                "          \"field\": \"user_event\",\n" +
                "          \"type\": \"string\",\n" +
                "          \"input\": \"text\",\n" +
                "          \"operator\": \"equal\",\n" +
                "          \"value\": \"event number 4\"\n" +
                "        }\n" +
                "      ]\n" +
                "    }";


        // Case 1: there are no spaces in user input string
        String name = "test_funnel1";
        String stepNames = "step1\n\nstep2";
        String description = "test";
        List<String> steps = new ArrayList<>(Arrays.asList(step1, step2));

        String result = Utils.getFunnelJsonReq(funnelGroupJsonReq, name, stepNames, steps, description);
        String expectedStr = "{\"name\":\"test_funnel1\",\"schemaName\":\"schema1\",\"description\":\"test\",\"owner\":\"john_doe\",\"projections\":[{\"column_id\":4,\"key\":\"country\",\"alias\":\"country\",\"schema_name\":\"schema1\"},{\"column_id\":5,\"key\":null,\"alias\":\"logged_in\",\"schema_name\":\"schema1\"}],\"steps\":[{\"name\":\"step3\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event3\",\"subfield\":null},{\"name\":null,\"condition\":\"AND\",\"rules\":[{\"id\":\"cookie_one_age\",\"field\":\"cookie_one_age\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"10\",\"subfield\":null},{\"id\":\"cookie_one_info\",\"field\":\"cookie_one_info\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}]},{\"name\":\"step4\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event4\",\"subfield\":null}]}],\"filter\":{\"condition\":\"AND\",\"rules\":[{\"id\":\"network_status\",\"field\":\"network_status\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"},{\"id\":\"device\",\"field\":\"device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"mobile\"},{\"id\":\"cookie_one\",\"field\":\"cookie_one\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null}]},\"startDate\":\"20200507\",\"queryRange\":\"1\",\"repeatInterval\":\"1\",\"endDate\":\"20200508\",\"userIdColumn\":\"cookie_one\",\"stepNames\":[\"step1\",\"step2\"]}";
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode node = (ObjectNode) mapper.readTree(expectedStr);
        String expected = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        Assert.assertEquals(result, expected);

        // Case 2: there are spaces in user input string
        steps = new ArrayList<>(Arrays.asList(step1, step2WithBlank));
        result = Utils.getFunnelJsonReq(funnelGroupJsonReq, name, stepNames, steps, description);
        expectedStr = "{\"name\":\"test_funnel1\",\"schemaName\":\"schema1\",\"description\":\"test\",\"owner\":\"john_doe\",\"projections\":[{\"column_id\":4,\"key\":\"country\",\"alias\":\"country\",\"schema_name\":\"schema1\"},{\"column_id\":5,\"key\":null,\"alias\":\"logged_in\",\"schema_name\":\"schema1\"}],\"steps\":[{\"name\":\"step3\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event3\",\"subfield\":null},{\"name\":null,\"condition\":\"AND\",\"rules\":[{\"id\":\"cookie_one_age\",\"field\":\"cookie_one_age\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"10\",\"subfield\":null},{\"id\":\"cookie_one_info\",\"field\":\"cookie_one_info\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}]},{\"name\":\"step4\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event number 4\",\"subfield\":null}]}],\"filter\":{\"condition\":\"AND\",\"rules\":[{\"id\":\"network_status\",\"field\":\"network_status\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"on\"},{\"id\":\"device\",\"field\":\"device\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"mobile\"},{\"id\":\"cookie_one\",\"field\":\"cookie_one\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null}]},\"startDate\":\"20200507\",\"queryRange\":\"1\",\"repeatInterval\":\"1\",\"endDate\":\"20200508\",\"userIdColumn\":\"cookie_one\",\"stepNames\":[\"step1\",\"step2\"]}";
        node = (ObjectNode) mapper.readTree(expectedStr);
        expected = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(node);
        Assert.assertEquals(result, expected);
    }

    /**
     * constructFunnelGroup Test for single funnel.
     */
    @Test
    public void constructFunnelGroupSingleTest() throws Exception {

        String funnelGroupJsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_single_funnel_json_req.json");

        String funnelGroupStepsJsonExpected = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP3 + TestUtils.STEP4;

        FunnelGroup funnelGroup = Utils.constructFunnelGroup(funnelGroupJsonReq, -1);

        Assert.assertEquals(funnelGroup.getFunnelGroupName(), "test");
        Assert.assertEquals(funnelGroup.getFunnelGroupStatus(), Status.INACTIVE);
        Assert.assertEquals(funnelGroup.getFunnelGroupVersion(), 1L);
        Assert.assertEquals(funnelGroup.getFunnelGroupSchemaName(), "schema1");
        Assert.assertEquals(funnelGroup.getFunnelGroupDescription(), "test");
        Assert.assertEquals(funnelGroup.getFunnelGroupOwner(), "john_doe");
        Assert.assertEquals(funnelGroup.getFunnelGroupUserIdField(), "cookie_one");
        Assert.assertEquals(funnelGroup.getFunnelGroupBackfillStartTime(), "2020-05-07");
        Assert.assertNull(funnelGroup.getFunnelGroupEndTime());
        Assert.assertEquals(funnelGroup.getFunnelGroupQueryRange(), "1");
        Assert.assertEquals(funnelGroup.getFunnelGroupRepeatInterval(), "1");
        Assert.assertEquals(funnelGroup.getFunnelGroupFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(funnelGroup.getFunnelGroupStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected.replaceAll("\\s", ""));
        Assert.assertEquals(funnelGroup.getFunnelGroupStepNames(), "[[START,step1],[step1,step2],[step2,step3],[step3,step4]]");
        Assert.assertEquals(funnelGroup.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

        List<Pipeline> pipelines = funnelGroup.getPipelines();
        Assert.assertEquals(pipelines.size(), 1);

        Pipeline pipeline = pipelines.get(0);
        Assert.assertEquals(pipeline.getPipelineName(), "test_single_funnel");
        Assert.assertEquals(pipeline.getPipelineSchemaName(), "schema1");
        Assert.assertEquals(pipeline.getPipelineDescription(), "test");
        Assert.assertEquals(pipeline.getPipelineOwner(), "john_doe");
        Assert.assertEquals(pipeline.getFunnelUserIdField(), "cookie_one");
        Assert.assertEquals(pipeline.getPipelineBackfillStartTime(), "2020-05-07");
        Assert.assertNull(pipeline.getPipelineEndTime());
        Assert.assertEquals(pipeline.getFunnelQueryRange(), "1");
        Assert.assertEquals(pipeline.getFunnelRepeatInterval(), "1");
        Assert.assertEquals(pipeline.getPipelineFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline.getFunnelStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected.replaceAll("\\s", ""));
        Assert.assertEquals(pipeline.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep3\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);
    }

    /**
     * constructFunnelGroup Test for multiple funnels.
     */
    @Test
    public void constructFunnelGroupMultiTest() throws Exception {

        String topologyExpected = "{'drawflow':{'Home':{'data':{'1':{'id':1,'name':'step1','data':{},'class':'step1','html':'step1','typenode':false,'inputs':{'input_1':{'connections':[]}},'outputs':{'output_1':{'connections':[{'node':'2','output':'input_1'},{'node':'3','output':'input_1'}]}},'pos_x':189,'pos_y':243},'2':{'id':2,'name':'step2','data':{},'class':'step2','html':'step2','typenode':false,'inputs':{'input_1':{'connections':[{'node':'1','input':'output_1'}]}},'outputs':{'output_1':{'connections':[{'node':'4','output':'input_1'},{'node':'3','output':'input_1'}]}},'pos_x':481,'pos_y':124},'3':{'id':3,'name':'step3','data':{},'class':'step3','html':'step3','typenode':false,'inputs':{'input_1':{'connections':[{'node':'1','input':'output_1'},{'node':'2','input':'output_1'}]}},'outputs':{'output_1':{'connections':[{'node':'4','output':'input_1'}]}},'pos_x':485,'pos_y':360},'4':{'id':4,'name':'step4','data':{},'class':'step4','html':'step4','typenode':false,'inputs':{'input_1':{'connections':[{'node':'2','input':'output_1'},{'node':'3','input':'output_1'}]}},'outputs':{'output_1':{'connections':[]}},'pos_x':808,'pos_y':245}}}}}";

        String funnelGroupStepsJsonExpected = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP3 + TestUtils.STEP4;

        //                     START
        //                       |
        //                      step1
        //             /                 \
        //           step2      ->      step3
        //             \                 /
        //                   step4
        // There are 3 funnels:
        // 1) START -> step1 -> step2 -> step4
        // 2) START -> step1 -> step2 -> step3 -> step4
        // 3) START -> step1 -> step3 -> step4
        String funnelGroupJsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_multi_funnel_json_req_1.json");

        FunnelGroup funnelGroup = Utils.constructFunnelGroup(funnelGroupJsonReq, -1);

        Assert.assertEquals(funnelGroup.getFunnelGroupName(), "test");
        Assert.assertEquals(funnelGroup.getFunnelGroupStatus(), Status.INACTIVE);
        Assert.assertEquals(funnelGroup.getFunnelGroupVersion(), 1L);
        Assert.assertEquals(funnelGroup.getFunnelGroupSchemaName(), "schema1");
        Assert.assertEquals(funnelGroup.getFunnelGroupDescription(), "test");
        Assert.assertEquals(funnelGroup.getFunnelGroupOwner(), "john_doe");
        Assert.assertEquals(funnelGroup.getFunnelGroupUserIdField(), "cookie_one");
        Assert.assertEquals(funnelGroup.getFunnelGroupBackfillStartTime(), "2020-05-07");
        Assert.assertNull(funnelGroup.getFunnelGroupEndTime());
        Assert.assertEquals(funnelGroup.getFunnelGroupQueryRange(), "1");
        Assert.assertEquals(funnelGroup.getFunnelGroupRepeatInterval(), "1");
        Assert.assertEquals(funnelGroup.getFunnelGroupFilterJson().replaceAll("\\s", ""), TestUtils.FUNNEL_GROUP_FILTER_JSON_EXPECTED.replaceAll("\\s", ""));
        Assert.assertEquals(funnelGroup.getFunnelGroupStepsJson().replaceAll("\\s", ""), funnelGroupStepsJsonExpected.replaceAll("\\s", ""));
        Assert.assertEquals(funnelGroup.getFunnelGroupStepNames(), "[[START,step1],[step1,step2],[step1,step3],[step2,step4],[step2,step3],[step3,step4]]");
        Assert.assertEquals(funnelGroup.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);
        Assert.assertEquals(funnelGroup.getFunnelGroupTopology(), topologyExpected);

        List<Pipeline> pipelines = funnelGroup.getPipelines();
        Assert.assertEquals(pipelines.size(), 3);

        // pipeline0: step1 -> step2 -> step4
        Pipeline pipeline0 = pipelines.get(0);
        String funnelGroupStepsJsonExpected0 = TestUtils.STEP1 + TestUtils.STEP2 + TestUtils.STEP4;
        Assert.assertEquals(pipeline0.getPipelineName(), "test_funnel2");
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
        Assert.assertEquals(pipeline0.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep2\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline0.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

        // pipeline1: step1 -> step3 -> step4
        Pipeline pipeline1 = pipelines.get(1);
        String funnelGroupStepsJsonExpected1 = TestUtils.STEP1 + TestUtils.STEP3 + TestUtils.STEP4;
        Assert.assertEquals(pipeline1.getPipelineName(), "test_funnel1");
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
        Assert.assertEquals(pipeline1.getFunnelStepNames().replaceAll("\\s", ""), "step1\n\nstep3\n\nstep4".replaceAll("\\s", ""));
        Assert.assertEquals(pipeline1.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

        // pipeline2: step1 -> step2 -> step3 -> step4
        Pipeline pipeline2 = pipelines.get(2);
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
        Assert.assertEquals(pipeline2.getProjections().toString(), TestUtils.PROJECTIONS_EXPECTED);

    }

    /**
     * constructFunnelGroup Test for multiple funnels.
     */
    @Test(expectedExceptions = Exception.class)
    public void constructFunnelGroupMultiNoCustomNameTest() throws Exception {
        String funnelGroupJsonReq = TemplateTestUtils.loadTemplateInstance("templates/funnel_group_multi_funnel_json_req_2.json");
        Utils.constructFunnelGroup(funnelGroupJsonReq, -1);
    }
}
