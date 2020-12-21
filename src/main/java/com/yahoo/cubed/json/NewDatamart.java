/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.ObjectUtils;
import com.yahoo.cubed.util.Utils;
import java.util.List;
import java.util.HashSet;
import java.util.Set;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * New datamart JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class NewDatamart extends Funnelmart {
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
    /** Data mart name. */
    private String name;

    /**
     * Check if JSON is valid.
     * @return null if valid, error message if invalid
     */
    public String isValid() {
        String regularTextFieldErrorMessage = ObjectUtils.firstNonNull(
                Utils.validateRegularTextJsonField(name, Utils.NAME_JSON_FIELD),
                Utils.validateRegularTextJsonField(description, Utils.DESCRIPTION_JSON_FIELD),
                Utils.validateRegularTextJsonField(owner, Utils.OWNER_JSON_FIELD));
        if (regularTextFieldErrorMessage != null) {
            return regularTextFieldErrorMessage;
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
            Set<String> vAValueSet = new HashSet<>();
            for (List<List<String>> p: projectionVMs) {
                if (p != null) {
                    for (List<String> v: p) {
                        if (v != null && v.size() != 2) {
                            return "Wrong format of value mapping";
                        }
                        String value = v.get(0);
                        String alias = v.get(1);
                        String vmFieldErrorMessage = ObjectUtils.firstNonNull(
                                Utils.validateRegularTextJsonField(value, Utils.VM_VALUE_JSON_FIELD),
                                Utils.validateRegularTextJsonField(alias, Utils.VM_ALIAS_JSON_FIELD));
                        if (vmFieldErrorMessage != null) {
                            return vmFieldErrorMessage;
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
