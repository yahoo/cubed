/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.json.filter.Filter;
import lombok.Data;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

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
}
