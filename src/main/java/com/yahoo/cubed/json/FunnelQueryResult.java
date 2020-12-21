/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * New datamart JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class FunnelQueryResult {
    /**
     * Default constructor.
     */
    public FunnelQueryResult() {
        this.values = new ArrayList<>();
        this.keys = new ArrayList<>();
    }

    /**
     * Constructor with existing list.
     */
    public FunnelQueryResult(List<String> keys, List<String> values) {
        this.keys = new ArrayList<>(keys);
        this.values = new ArrayList<>(values);
    }

    /** Query key. */
    private List<String> keys;
    /** Query values. */
    private List<String> values;
}
