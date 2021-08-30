/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;

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
        return validateDatamartBody();
    }
}
