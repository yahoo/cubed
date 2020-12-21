/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model.filter;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.util.OperatorMapper;
import lombok.Getter;
import lombok.Setter;

/**
 * Store pipeline relation rule.
 */
public class PipelineRelationalRule extends PipelineFilter {
    @Setter
    private Field field;
    @Getter @Setter
    private String key;
    @Getter @Setter
    private String operator;
    @Getter @Setter
    private String value;

    /**
     * Get field with key.
     */
    public Field getField() {
        // set the key to the field
        field.setKey(this.getKey());
        return field;
    }

    /**
     * Convert to string.
     */
    @Override
    public String toString() {
        String columnName = this.getField().strOfSimpleName();
        String operator = OperatorMapper.mapOperator(this.getOperator());
        String value = OperatorMapper.validateValue(this.getValue(), this.getField());

        return String.format("%s %s %s", columnName, operator, value);
    }
}
