/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.cardinality;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.service.bullet.query.BulletQuery;
import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Implementation of cardinality estimation.
 */
@Slf4j
public class CardinalityEstimationServiceImpl implements CardinalityEstimationService {
    @Override
    public Response sendBulletQuery(Pipeline pipelineModel) throws BulletQueryFailException {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();

            Schema schema = ServiceFactory.schemaService().fetchByName(pipelineModel.getPipelineSchemaName());

            // build http post body
            BulletQuery queryBody = BulletQuery.createBulletQueryInstance(pipelineModel);
            String postBodyJson = jsonMapper.writeValueAsString(queryBody);

            HttpClient httpClient = this.newHttpClientInstance();

            HttpPost request = new HttpPost(schema.getSchemaBulletUrl());
            request.setHeader("Accept", "application/json");
            request.setHeader("Content-type", "text/plain");

            log.info("Bullet query request body:\n" + postBodyJson);

            request.setEntity(new StringEntity(postBodyJson));

            HttpResponse response = httpClient.execute(request);

            int statusCode = response.getStatusLine().getStatusCode();

            if (statusCode != HttpStatus.SC_OK) {
                return new Response(statusCode);
            }

            String jsonContent = this.readResponseContent(response);

            log.info("Bullet query response:\n" + jsonContent);

            Map<String, Double> aggregations = this.readAggregations(jsonMapper, jsonContent);
            return new Response(statusCode, aggregations);

        } catch (Exception e) {
            throw new BulletQueryFailException(e);
        }
    }

    /**
     * Create new HTTP client.
     */
    protected HttpClient newHttpClientInstance() {
        return HttpClientBuilder.create().build();
    }

    /**
     * Parse the JSON format of aggregations to a map.
     */
    protected Map<String, Double> readAggregations(ObjectMapper jsonMapper, String jsonContent) throws JsonParseException, JsonMappingException, IOException {
        JsonNode jsonObject = jsonMapper.readValue(jsonContent, JsonNode.class);

        String topLevelField = "records";

        if (!jsonObject.hasNonNull(topLevelField)) {
            throw new IOException("Cannot find field 'records' in the response content.");
        }

        Iterator<JsonNode> nodes = jsonObject.get(topLevelField).iterator();

        Map<String, Double> result = new HashMap<>();

        while (nodes.hasNext()) {
            JsonNode elem = nodes.next();
            Iterator<String> fields = elem.fieldNames();
            while (fields.hasNext()) {
                String fieldName = fields.next();
                result.put(fieldName, elem.get(fieldName).asDouble());
            }
        }

        return result;
    }

    /**
     * Read HTTP response content.
     */
    protected String readResponseContent(HttpResponse response) throws UnsupportedOperationException, IOException {
        BufferedReader rd =
                new BufferedReader(
                        new InputStreamReader(response.getEntity().getContent(), "UTF-8"));

        StringBuffer result = new StringBuffer();
        String line = null;
        while ((line = rd.readLine()) != null) {
            result.append(line);
            result.append('\n');
        }

        return result.toString();
    }
}
