/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.templating.TemplateTestUtils;
import com.yahoo.cubed.util.Utils;

/**
 * Utities for tests.
 */
public class TestUtils {
    /** Funnel group filter json expected. */
    public static final String FUNNEL_GROUP_FILTER_JSON_EXPECTED = "{\n" +
            "  \"name\": null,\n" +
            "  \"condition\": \"AND\",\n" +
            "  \"rules\": [{\n" +
            "    \"id\": \"network_status\",\n" +
            "    \"field\": \"network_status\",\n" +
            "    \"type\": \"string\",\n" +
            "    \"input\": \"text\",\n" +
            "    \"operator\": \"equal\",\n" +
            "    \"value\": \"on\",\n" +
            "    \"subfield\": null\n" +
            "  },\n" +
            "  {\n" +
            "    \"id\": \"device\",\n" +
            "    \"field\": \"device\",\n" +
            "    \"type\": \"string\",\n" +
            "    \"input\": \"text\",\n" +
            "    \"operator\": \"equal\",\n" +
            "    \"value\": \"mobile\",\n" +
            "    \"subfield\": null\n" +
            "  },\n" +
            "  {\n" +
            "    \"id\": \"cookie_one\",\n" +
            "    \"field\": \"cookie_one\",\n" +
            "    \"type\": \"string\",\n" +
            "    \"input\": \"text\",\n" +
            "    \"operator\": \"is_not_null\",\n" +
            "    \"value\": null,\n" +
            "    \"subfield\": null\n" +
            "  }]\n" +
            "}";
    /** Step1. */
    public static final String STEP1 = "{\"name\":\"step1\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event1\",\"subfield\":null},{\"id\":\"cookie_one\",\"field\":\"cookie_one\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}\n";
    /** Step2. */
    public static final String STEP2 = "{\"name\":\"step2\",\"condition\":\"OR\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event2\",\"subfield\":null},{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"sth_else\",\"subfield\":null}]}\n";
    /** Step3. */
    public static final String STEP3 = "{\"name\":\"step3\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event3\",\"subfield\":null},{\"name\":null,\"condition\":\"AND\",\"rules\":[{\"id\":\"cookie_one_age\",\"field\":\"cookie_one_age\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"10\",\"subfield\":null},{\"id\":\"cookie_one_info\",\"field\":\"cookie_one_info\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"is_not_null\",\"value\":null,\"subfield\":null}]}]}\n";
    /** Step4. */
    public static final String STEP4 = "{\"name\":\"step4\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"event4\",\"subfield\":null}]}\n";
    /** Step5. */
    public static final String STEP5 = "{\"name\":\"step5\",\"condition\":\"AND\",\"rules\":[{\"id\":\"user_event\",\"field\":\"user_event\",\"type\":\"string\",\"input\":\"text\",\"operator\":\"equal\",\"value\":\"payment_made\",\"subfield\":null}]}\n";
    /** Projections expected. */
    public static final String PROJECTIONS_EXPECTED = "[geo_info['country'] AS country, user_logged_in AS logged_in]";

    /**
     * Create a sample funnel group without custom specified funnel names.
     */
    public static FunnelGroup constructSampleFunnelGroup() throws Exception {
        // Sample funnel group JSON to be parsed
        //                     START
        //                       |
        //                  enter_lobby
        //             /                 \
        //   enter_quick_match  ->   add_players
        //             \                 /
        //                 submit_success
        // There are 3 funnels:
        // 1) START -> enter_lobby -> enter_quick_match -> submit_success
        // 2) START -> enter_lobby -> enter_quick_match -> add_players -> submit_success
        // 3) START -> enter_lobby -> add_players -> submit_success
        return Utils.constructFunnelGroup(TemplateTestUtils.loadTemplateInstance("templates/funnel_group_multi_funnel_json_req_1.json"), -1);
    }

    /**
     * Create a sample funnel (daily source granularity).
     */
    public static Pipeline constructSampleFunnel() throws Exception {
        // Funnel steps: enter_lobby -> enter_quick_match -> add_players -> submit_success
        return Utils.constructFunnelmartPipeline(TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request.json"), -1, true, true);
    }

    /**
     * Create a sample funnel (hourly source granularity).
     */
    public static Pipeline constructSampleFunnelHourlySource() throws Exception {
        // Funnel steps: enter_lobby -> enter_quick_match -> add_players -> submit_success
        return Utils.constructFunnelmartPipeline(TemplateTestUtils.loadTemplateInstance("templates/funnel_json_request_hourly_source.json"), -1, true, true);
    }
}
