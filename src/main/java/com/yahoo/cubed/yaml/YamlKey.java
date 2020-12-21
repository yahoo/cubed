/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Getter;
import lombok.Setter;

/**
 * Key in a map field.
 */
public class YamlKey {
    @JsonInclude(Include.NON_NULL)
    @Getter @Setter
    private Integer id;
    @Getter @Setter
    private String name;
}
