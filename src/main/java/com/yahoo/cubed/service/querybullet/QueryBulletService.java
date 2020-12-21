/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.querybullet;

import com.yahoo.cubed.service.bullet.query.BulletQueryFailException;

/**
 * Estimate cardinality of a pipeline.
 */
public interface QueryBulletService {
    /**
     * Bullet response JSON.
     */
    public static class ResponseJson {
        /** Status code. */
        public int statusCode;
        /** Aggregation values. */
        public String jsonResponse;

        /**
        * Create reponse with status code.
        */
        public ResponseJson(int statusCode) {
            this.statusCode = statusCode;
        }

        /**
        * Create response with status code and aggregation values.
        */
        public ResponseJson(int statusCode, String jsonResponse) {
            this.statusCode = statusCode;
            this.jsonResponse = jsonResponse;
        }

    }
    /**
     * This is the method that converts a pipeline into Bullet query and send the query to Bullet backend.
     * @param jsonRequest json request.
     * @return query result in json.
     * @throws BulletQueryFailException exception that fails to send query to Bullet.
     */
    ResponseJson sendBulletQueryJson(String jsonRequest) throws BulletQueryFailException;
}
