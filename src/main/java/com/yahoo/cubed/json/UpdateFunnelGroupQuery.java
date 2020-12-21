/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

/**
 * Funnel query JSON structure.
 */
@Slf4j
@Data
@JsonSerialize(include = JsonSerialize.Inclusion.NON_EMPTY)
public class UpdateFunnelGroupQuery extends FunnelGroupQuery {

}
