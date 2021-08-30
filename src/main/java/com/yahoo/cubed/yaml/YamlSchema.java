/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import java.util.List;
import java.util.Map;

import lombok.Getter;
import lombok.Setter;

/**
 * Yaml schema definition.
 */
public class YamlSchema {
    @Getter @Setter
    private String name;

    @Getter @Setter
    Map<String, String> tables;

    @Getter
    @Setter
    private List<YamlField> fields;

    @Getter @Setter
    private String database;

    @Getter @Setter
    private String datetimePartitionColumn;
}
