/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.json.filter.LogicalRule;
import com.yahoo.cubed.util.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * Funnel group query base JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public abstract class FunnelGroupQuery extends Funnelmart {

    /** Query Steps. */
    protected List<LogicalRule> steps;
    /** Query Step Names. A list of [prev_step, step]. */
    protected List<String[]> stepNames;
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
    /** Query topology. */
    protected String topology;
    /** Funnel names. */
    protected String funnelNames;

    /**
     * Check if JSON is valid.
     * @return null if valid, error message if invalid
     */
    public String isValid() {
        String regularTextFieldErrorMessage = ObjectUtils.firstNonNull(
                Utils.validateRegularTextJsonField(description, Utils.DESCRIPTION_JSON_FIELD),
                Utils.validateRegularTextJsonField(owner, Utils.OWNER_JSON_FIELD));
        if (regularTextFieldErrorMessage != null) {
            return regularTextFieldErrorMessage;
        }

        // userIdColumn should not be be null
        if (userIdColumn == null) {
            return "Missing user id column";
        }

        // queryRange should not be null
        if (queryRange == null) {
            return "Missing query range";
        }

        // repeatInterval should not be null
        if (repeatInterval == null) {
            return "Missing repeat interval";
        }

        // must have at least 2 steps
        if (steps.size() < 2) {
            return "Must have at least 2 steps";
        }

        // validate steps
        for (Filter step : steps) {
            String filterValidationErrorMessage = Utils.validateFilters(step);
            if (filterValidationErrorMessage != null) {
                return filterValidationErrorMessage;
            }
        }

        // validate step names
        String stepNamesErrorMessage = validateStepNames();
        if (stepNamesErrorMessage != null) {
            return stepNamesErrorMessage;
        }

        // validate individual funnel names
        String funnelNamesErrorMessage = Utils.validateFunnelNamesJsonField(funnelNames);
        if (funnelNamesErrorMessage != null) {
            return funnelNamesErrorMessage;
        }

        // projections should be valid
        if (projections.size() > 0) {
            String projectionValidationErrorMessage = Utils.validateProjections(projections);
            if (projectionValidationErrorMessage !=  null) {
                return projectionValidationErrorMessage;
            }
        }

        // startDate is not null or empty
        if (startDate == null || startDate.isEmpty()) {
            return "Missing start time date";
        }

        // endDate is not null or empty
        if (endDate == null || endDate.isEmpty()) {
            return "Missing end time date";
        }

        // start date must before end date
        if (LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))
                .compareTo(LocalDate.parse(endDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))) > 0) {
            return "Start date must be before end date";
        }

        // query date can not span longer than 30 days
        if (LocalDate.parse(startDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT)).plusDays(Utils.QUERY_MAX_NUM_DAYS)
                .compareTo(LocalDate.parse(endDate, DateTimeFormatter.ofPattern(Utils.QUERY_DATE_FORMAT))) < 1) {
            return "Queries can only run for 30 days at most";
        }

        return null;
    }

    private String validateStepNames() {
        if (stepNames == null || stepNames.size() < 2) {
            return "The stepNames field is not formatted correctly.";
        }
        // stepNames is a list of size-2 arrays ([from_step, to_step])
        for (String[] edge : stepNames) {
            if (edge.length != 2) {
                return "One edge does not have both a from_step and a to_step.";
            }
            String stepNameErrorMessage = ObjectUtils.firstNonNull(
                    Utils.validateRegularTextJsonField(edge[0], Utils.FUNNEL_STEP_NAME_JSON_FIELD),
                    Utils.validateRegularTextJsonField(edge[1], Utils.FUNNEL_STEP_NAME_JSON_FIELD));
            if (stepNameErrorMessage != null) {
                return stepNameErrorMessage;
            }
        }
        return null;
    }
}
