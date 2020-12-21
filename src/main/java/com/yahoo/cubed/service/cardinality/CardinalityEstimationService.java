/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.cardinality;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;
import java.util.Map;

/**
 * Estimate cardinality of a pipeline.
 */
public interface CardinalityEstimationService {
    /**
     * Bullet response.
     */
    public static class Response {
        /** Status code. */
        public int statusCode;
        /** Aggregation values. */
        public Map<String, Double> aggregationValues;

        /**
         * Create reponse with status code.
         */
        public Response(int statusCode) {
            this.statusCode = statusCode;
        }

        /**
         * Create response with status code and aggregation values.
         */
        public Response(int statusCode, Map<String, Double> aggregationValues) {
            this.statusCode = statusCode;
            this.aggregationValues = aggregationValues;
        }
    }

    /**
     * This is the method that converts a pipeline into Bullet query and send the query to Bullet backend.
     * @param pipelineModel pipeline model.
     * @return query result.
     * @throws BulletQueryFailException exception that fails to send query to Bullet.
     */
    Response sendBulletQuery(Pipeline pipelineModel) throws BulletQueryFailException;
}
