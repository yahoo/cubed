/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json.filter;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.filter.PipelineFilter;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

import lombok.Data;

/**
 * Filter JSON object.
 */
@Data
@JsonSerialize
@JsonDeserialize(using = FilterDeserializer.class)
public abstract class Filter {
    /**
     * Convert to JSON.
     */
    public static String toJson(Filter pipelineFilterJsonObject) throws JsonProcessingException {
        if (pipelineFilterJsonObject == null) {
            return null;
        }
        return (new ObjectMapper()).writeValueAsString(pipelineFilterJsonObject);
    }

    /**
     * Read JSON.
     */
    public static Filter fromJson(String pipelineFilterJsonString) throws JsonParseException, JsonMappingException, IOException {
        if (pipelineFilterJsonString == null || pipelineFilterJsonString.isEmpty()) {
            return null;
        }
        return (new ObjectMapper()).readValue(pipelineFilterJsonString, Filter.class);
    }

    /**
     * Convert to pipeline filter.
     */
    public abstract PipelineFilter toModel(String schemaName) throws NumberFormatException, DataValidatorException, DatabaseException;

    /**
     * Generate bullet query filter kernel.
     */
    protected abstract BulletQueryFilter toBulletQueryFilterKernel() throws NumberFormatException, DataValidatorException, DatabaseException;

    /**
     * Convert to Bullet query filter.
     */
    public BulletQueryFilter toBulletQueryFilter() throws NumberFormatException, DataValidatorException, DatabaseException {
        BulletQueryFilter rule = this.toBulletQueryFilterKernel();
        return rule.postTranslationProcess();
    }
}
