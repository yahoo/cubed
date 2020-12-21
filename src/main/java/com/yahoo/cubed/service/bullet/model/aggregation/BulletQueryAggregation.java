/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.aggregation;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.util.Measurement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * Bullet query aggregation.
 */
@Data @JsonSerialize
public class BulletQueryAggregation {
    /** List of fields. */
    public Map<String, String> fields;
    /** Aggregation type. */
    public String type;
    /** Limit record number. */
    public int size;

    private BulletQueryAggregation(String aggregationType, int recordLimit) {
        this.type = aggregationType;
        this.fields = new HashMap<>();
        this.size = recordLimit;
    }

    /**
     * Add field.
     */
    public void addField(String fieldName) {
        this.addField(fieldName, "");
    }

    /**
     * Add field with alias.
     */
    public void addField(String fieldName, String alias) {
        this.fields.put(fieldName, alias);
    }

    /**
     * Create Bullet query from an aggregate type and a list of projections.
     */
    public static BulletQueryAggregation createBulletQueryAggregationInstance(String aggregationType, int recordLimit, List<PipelineProjection> projections) {
        BulletQueryAggregation ret = new BulletQueryAggregation(aggregationType, recordLimit);

        if (projections == null) {
            return ret;
        }
        List<PipelineProjection> dimensionProjections = Measurement.getDimensionProjections(projections);
        for (PipelineProjection projection : dimensionProjections) {
            String fieldName = projection.getField().getFieldName();
            if (projection.getKey() != null && !projection.getKey().isEmpty()) {
                fieldName = fieldName + "." + projection.getKey();
            }
            ret.addField(fieldName);
        }
        return ret;
    }
}
