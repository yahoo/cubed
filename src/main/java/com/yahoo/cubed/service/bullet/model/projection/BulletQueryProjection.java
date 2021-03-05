/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.projection;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.util.Measurement;
import com.yahoo.cubed.util.Constants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * Bullet query aggregation.
 */
@Data @JsonSerialize
public class BulletQueryProjection {
    /** List of fields. */
    public Map<String, String> fields;

    private BulletQueryProjection() {
        this.fields = new HashMap<>();
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
    public static BulletQueryProjection createBulletQueryProjectionInstance(List<PipelineProjection> projections) {
        BulletQueryProjection ret = new BulletQueryProjection();
        if (projections == null) {
            return ret;
        }
        List<PipelineProjection> dimensionProjections = Measurement.getDimensionProjections(projections);
        // add in metrics, treat as dimensions
        dimensionProjections.addAll(Measurement.getMetricProjections(projections));
        for (PipelineProjection projection : dimensionProjections) {
            String fieldName = projection.getField().getFieldName();
            if (projection.getKey() != null && !projection.getKey().isEmpty()) {
                fieldName = fieldName + Constants.DOT + Constants.DOUBLE_QUOTE + projection.getKey() + Constants.DOUBLE_QUOTE;
            }
            // use alias if given
            if (projection.getAlias() != null) {
                ret.addField(fieldName, projection.getAlias());
            } else {
                ret.addField(fieldName);
            }
        }

        return ret;
    }
}
