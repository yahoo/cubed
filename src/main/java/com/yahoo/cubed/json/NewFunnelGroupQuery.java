/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.util.Utils;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * New funnel group query JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class NewFunnelGroupQuery extends FunnelGroupQuery {

    /** Funnel group Name. */
    private String name;

    /**
     * Check if JSON is valid.
     * @return null if valid, error message if invalid
     */
    public String isValid() {
        String nameErrorMessage = Utils.validateRegularTextJsonField(name, Utils.NAME_JSON_FIELD);
        if (nameErrorMessage != null) {
            return nameErrorMessage;
        }
        return super.isValid();
    }
}
