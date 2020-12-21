/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import java.util.List;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

/**
 * Field in the YAML schema.
 */
public class YamlField {
    @JsonInclude(Include.NON_NULL)
    @Getter @Setter
    private Integer id;
    @Getter @Setter
    private String name;
    @Getter @Setter
    private String type;
    @JsonInclude(Include.NON_NULL)
    @Getter @Setter
    private List<YamlKey> keys;
}
