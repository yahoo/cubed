/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;


/**
 * Tables attribute in schema with JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class SchemaTables {
    /** Hourly table. */
    private String hourlyTable;
    /** Hourly partition. */
    private String hourlyPartition;
    /** Daily table. */
    private String dailyTable;
    /** Daily partition. */
    private String dailyPartition;

}
