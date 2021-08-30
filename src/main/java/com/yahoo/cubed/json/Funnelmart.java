/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.util.Utils;
import org.apache.commons.lang3.StringUtils;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *  Funnelmart base JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public abstract class Funnelmart {
    /** Data mart description. */
    protected String description;
    /** Data mart owner. */
    protected String owner;
    /** Data mart projections. */
    protected List<Map<String, String>> projections;
    /** Value mappings of Porjections. */
    protected List<List<List<String>>> projectionVMs;
    /** Enable Backfill support. */
    protected boolean backfillEnabled;
    /** Backfill start date. */
    protected String backfillStartDate;
    /** Enable end time support. */
    protected boolean endTimeEnabled;
    /** End time date. */
    protected String endTimeDate;
    /** Data mart filters. */
    protected Filter filter;
    /** Schema Name. */
    @Getter
    protected String schemaName;

    /**
     * Check whether the json is valid.
     * @return
     */
    public abstract String isValid();

    /**
     * Validate the body json of data mart.
     * @return Error message, or null if no error found
     */
    String validateDatamartBody() {
        if (StringUtils.isEmpty(description)) {
            return "Missing data mart description";
        }

        if (StringUtils.isEmpty(owner)) {
            return "Missing data mart owner";
        }

        // Check for projection name collisions, etc
        String projectionValidationErrorMessage = Utils.validateProjections(projections);
        if (projectionValidationErrorMessage != null) {
            return projectionValidationErrorMessage;
        }

        // Validate filters if they exist
        if (filter != null) {
            String filterValidationErrorMessage = Utils.validateFilters(filter);
            if (filterValidationErrorMessage != null) {
                return filterValidationErrorMessage;
            }
        }

        // validate backfill
        if (backfillEnabled) {
            if (backfillStartDate == null || backfillStartDate.isEmpty()) {
                return "Missing backfill start date";
            }
        }

        // validate end time
        if (endTimeEnabled) {
            if (endTimeDate == null || endTimeDate.isEmpty()) {
                return "Missing end time date";
            }
        }

        // validate value mapping
        if (projectionVMs != null) {
            // Set of all VM values
            for (List<List<String>> p: projectionVMs) {
                Set<String> vAValueSet = new HashSet<>();
                if (p != null) {
                    for (List<String> v: p) {
                        if (v != null && v.size() != 3) {
                            return "Wrong format of value mapping";
                        }
                        if (vAValueSet.contains(v.get(0))) {
                            return "Ambiguous value mapping for value:" + v.get(0);
                        }
                        vAValueSet.add(v.get(0));
                    }
                }
            }
        }
        return null;
    }
}
