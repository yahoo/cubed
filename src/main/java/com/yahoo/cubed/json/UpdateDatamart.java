/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.util.Utils;

import java.util.List;
import java.util.HashSet;
import java.util.Set;
import lombok.Data;
import org.apache.commons.lang3.ObjectUtils;

/**
 * Update datamart JSON structure.
 */
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class UpdateDatamart extends Funnelmart {
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
