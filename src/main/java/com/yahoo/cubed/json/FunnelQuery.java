/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.util.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * Funnel query base JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public abstract class FunnelQuery extends Funnelmart {
    /** Column ID map key. */
    public static final String COLUMN_ID = "column_id";
    /** Key map key. */
    public static final String KEY = "key";
    /** Alias map key. */
    public static final String ALIAS = "alias";
    /** Aggregate map key. */
    public static final String AGGREGATE = "aggregate";
    /** Schema Name map key. */
    public static final String SCHEMA_NAME = "schema_name";
    /** Query Steps. */
    protected List<Filter> steps;
    /** Query Step Names. */
    protected List<String> stepNames;
    /** Query start date. */
    protected String startDate;
    /** Query time date. */
    protected String endDate;
    /** Query range. */
    protected String queryRange;
    /** Repeat interval. */
    protected String repeatInterval;
    /** Query userid column. */
    protected String userIdColumn;

    /**
     * Check if JSON is valid.
     * @return null if valid, error message if invalid
     */
    public String isValid() {
        if (userIdColumn == null) {
            return "Missing user id column";
        }

        // must have at least 2 steps
        if (steps.size() < 2) {
            return "Must have at least 2 steps";
        }

        // Validate steps
        for (Filter step : steps) {
            String filterValidationErrorMessage = Utils.validateFilters(step);
            if (filterValidationErrorMessage != null) {
                return filterValidationErrorMessage;
            }
        }

        if (projections.size() > 0) {
            String projectionValidationErrorMessage = Utils.validateProjections(projections);
            if (projectionValidationErrorMessage !=  null) {
                return projectionValidationErrorMessage;
            }
        }

        if (startDate == null || startDate.isEmpty()) {
            return "Missing start time date";
        }

        if (endDate == null || endDate.isEmpty()) {
            return "Missing end time date";
        }

        if (LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))
                .compareTo(LocalDate.parse(endDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))) > 0) {
            return "Start date must be before end date";
        }

        if (LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT)).plusDays(Utils.QUERY_MAX_NUM_DAYS)
                .compareTo(LocalDate.parse(endDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))) < 1) {
            return "Queries can only run for 30 days at most";
        }

        return null;
    }
}
