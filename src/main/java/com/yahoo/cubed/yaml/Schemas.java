/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import lombok.Getter;
import lombok.Setter;
import java.util.List;

/**
 * Yaml schema definition.
 */
public class Schemas {
    @Getter @Setter
    private List<YamlSchema> schemas;
}
