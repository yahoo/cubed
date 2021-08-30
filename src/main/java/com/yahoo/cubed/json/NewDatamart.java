/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.apache.commons.lang3.StringUtils;
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
        if (StringUtils.isEmpty(name)) {
            return "Missing data mart name";
        }
        return validateDatamartBody();
    }
}
