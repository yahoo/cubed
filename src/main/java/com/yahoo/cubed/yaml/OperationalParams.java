/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * Yaml operational parameter format definition.
 */
public class OperationalParams {

    @Getter @Setter
    private String bulletUrl;

    @Getter @Setter
    private boolean disableBullet;

    @Getter @Setter
    private boolean disableFunnel;

    @Getter @Setter
    private String funnelTargetTable;

    @Getter @Setter
    private List<String> userIdFields;

    @Getter @Setter
    private String oozieJobType;

    @Getter @Setter
    private String oozieBackfillJobType;

    @Getter @Setter
    private List<Map<String, String>> defaultFilters;

    @Getter @Setter
    private String timestampColumnParam;
}
