/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import com.yahoo.cubed.model.PipelineProjection;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Measurements are dimensions or metrics.
 */
public enum Measurement {
    
    /** Dimension measurement. */
    DIMENSION("DIM"),
    /** Metric measurement. */
    METRIC("MET");
    
    /** Measurement type. */
    public String measurementTypeCode;
    
    private Measurement(String measurementTypeCode) {
        this.measurementTypeCode = measurementTypeCode;
    }
    
    /**
     * Get measurement by code.
     */
    public static Measurement byName(String code) {
        if (code == null || code.isEmpty()) {
            return null;
        }
        for (Measurement measurement : Measurement.values()) {
            if (measurement.measurementTypeCode.equalsIgnoreCase(code)) {
                return measurement;
            }
        }
        return null;
    }
    
    /**
     * Filter out only metric projections from pipeline projections.
     */
    public static List<PipelineProjection> getMetricProjections(List<PipelineProjection> projections) {
        return projections.stream()
            .filter(p -> Measurement.METRIC.measurementTypeCode.equals(p.getField().getMeasurementType()) || p.isAggregation())
            .collect(Collectors.toList());
    }

    /**
     * Filter out only dimension projections from pipeline projections.
     */
    public static List<PipelineProjection> getDimensionProjections(List<PipelineProjection> projections) {
        return projections.stream()
            .filter(p -> !p.isAggregation())
            .collect(Collectors.toList());
    }
}
