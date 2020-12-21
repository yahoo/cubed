/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.query;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.service.bullet.model.aggregation.BulletQueryAggregation;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.bullet.model.projection.BulletQueryProjection;

import lombok.Data;
import lombok.Setter;

/**
 * Bullet query.
 */
@Data
@JsonSerialize
public class BulletQuery {
    /** Aggregation used in the query. */
    public static final String AGGREGATION_TYPE = "COUNT DISTINCT";
    /** Duration of the query. */
    public static final long DEFAULT_DURATION = CLISettings.BULLET_QUERY_DURATION * 1000; // 15 seconds = 15000 milliseconds
    /** Limit number of records. */
    public static final int RECORDS_LIMIT = 10;
    /** Query aggregation. */
    @Setter
    public BulletQueryAggregation aggregation;
    /** Query projection. */
    @Setter
    public BulletQueryProjection projection;
    /** List of filters. */
    public List<BulletQueryFilter> filters;
    /** Duration of Bullet query. In milliseconds. */
    public long duration;

    /**
     * Add filter.
     */
    public void addFilter (BulletQueryFilter filter) {
        this.filters.add(filter);
    }

    private BulletQuery() {
        this.filters = new ArrayList<>();
        this.duration = DEFAULT_DURATION;
    }

    /**
     * Create Bullet query for cardinality estimation.
     */
    public static BulletQuery createBulletQueryInstance(Pipeline pipelineModel) {
        BulletQuery ret = new BulletQuery();

        // aggregation - set max # records to CARDINALITY_CAP + 1
        BulletQueryAggregation aggregation = BulletQueryAggregation.createBulletQueryAggregationInstance(AGGREGATION_TYPE, CLISettings.CARDINALITY_CAP + 1, pipelineModel.getProjections());
        if (aggregation != null) {
            ret.setAggregation(aggregation);
        }

        // filter
        BulletQueryFilter filter = BulletQueryFilter.createBulletQueryFilterInstance(pipelineModel.getPipelineFilterJson());
        if (filter != null) {
            ret.addFilter(filter);
        }

        return ret;
    }

    /**
     * Create Bullet query for previewing records.
     */
    public static BulletQuery createPreviewBulletQueryInstance(List<PipelineProjection> projections, String pipelineFilterJson) throws Exception {
        BulletQuery ret = new BulletQuery();

        BulletQueryProjection projection = BulletQueryProjection.createBulletQueryProjectionInstance(projections);
        if (projection != null) {
            ret.setProjection(projection);
        }

        // aggregation to limit number of records returned to pre-defined size RECORDS_LIMIT
        BulletQueryAggregation aggregation = BulletQueryAggregation.createBulletQueryAggregationInstance("RAW", RECORDS_LIMIT, null);
        ret.setAggregation(aggregation);

        // filters
        BulletQueryFilter filter = BulletQueryFilter.createBulletQueryFilterInstance(pipelineFilterJson);
        if (filter != null) {
            ret.addFilter(filter);
        }

        return ret;
    }
}
