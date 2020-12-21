/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.querybullet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.json.NewDatamart;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.bullet.query.BulletQuery;
import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import com.yahoo.cubed.util.Utils;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpResponse;
import org.apache.http.HttpStatus;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Implementation of querying bullet.
 */
@Slf4j
public class QueryBulletServiceImpl implements QueryBulletService {
    @Override
    public ResponseJson sendBulletQueryJson(String jsonRequest) throws BulletQueryFailException {
        try {
            ObjectMapper jsonMapper = new ObjectMapper();
            NewDatamart datamart = jsonMapper.readValue(jsonRequest, NewDatamart.class);
            List<PipelineProjection> projections = Utils.buildProjections(datamart.getProjections(), true);
            String filterJson = Filter.toJson(datamart.getFilter());

            Schema schema = ServiceFactory.schemaService().fetchByName(datamart.getSchemaName());

            // build http post body
            BulletQuery queryBody = BulletQuery.createPreviewBulletQueryInstance(projections, filterJson);
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
                return new ResponseJson(statusCode);
            }

            String jsonContent = this.readResponseContent(response);

            log.info("Bullet query response:\n" + jsonContent);

            return new ResponseJson(statusCode, jsonContent);

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
     * Read HTTP response content.
     */
    private String readResponseContent(HttpResponse response) throws UnsupportedOperationException, IOException {
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
