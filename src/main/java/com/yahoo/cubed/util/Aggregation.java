/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import java.util.ArrayList;
import java.util.List;

/**
 * Aggregation functions.
 */
public enum Aggregation {
    /** Sum. */
    SUM("SUM",                       "COALESCE(SUM(%s), 0)"),
    /** Minimum. */
    MIN("MIN",                       "COALESCE(MIN(%s), 0)"),
    /** Maximum. */
    MAX("MAX",                       "COALESCE(MAX(%s), 0)"),
    /** Count. */
    COUNT("COUNT",                   "COALESCE(COUNT(%s), 0)"),
    /** Count distinct. */
    COUNT_DISTINCT("COUNT_DISTINCT", "COALESCE(COUNT(DISTINCT %s), 0)"),
    /** Theta sketch. */
    THETA_SKETCH("THETA_SKETCH",     "data_to_sketch(%s, %s, 1.0)");

    /** Aggregate name. */
    public final String name;
    /** Aggregate template. */
    public final String template;
    /** Default size for theta sketches. */
    public static final String THETA_SKETCH_SIZE = "2048";

    private Aggregation(String name, String template) {
        this.name = name;
        this.template = template;
    }

    /**
     * Convert aggregate to a string.
     */
    public String toString(String column) {
        if (this.name.equals("THETA_SKETCH")) {
            return String.format(this.template, column, THETA_SKETCH_SIZE);
        }
        return String.format(this.template, column);
    }

    /**
     * Get an aggregation by name.
     */
    public static Aggregation byName(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        for (Aggregation aggregation : Aggregation.values()) {
            if (aggregation.name.equalsIgnoreCase(name)) {
                return aggregation;
            }
        }
        return null;
    }

    /**
     * Get a list of all aggregations.
     */
    public static List<String> allNames() {
        List<String> names = new ArrayList<>();
        for (Aggregation aggregation : Aggregation.values()) {
            names.add(aggregation.name);
        }
        return names;
    }
}
