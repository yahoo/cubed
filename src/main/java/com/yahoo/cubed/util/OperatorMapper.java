/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import java.util.HashMap;
import com.yahoo.cubed.model.Field;

/**
 * Map operator to representation.
 */
public class OperatorMapper {
    private static final HashMap<String, String> OPERATORS;
    private static final HashMap<String, String> BOOLEAN_OPERATORS;

    static {
        OPERATORS = new HashMap<>();
        OPERATORS.put("equal", "==");
        OPERATORS.put("not_equal", "!=");
        OPERATORS.put("less", "<");
        OPERATORS.put("less_or_equal", "<=");
        OPERATORS.put("greater", ">");
        OPERATORS.put("greater_or_equal", ">=");
        OPERATORS.put("is_null", "is");
        OPERATORS.put("is_not_null", "is not");
        OPERATORS.put("rlike", "rlike");
        OPERATORS.put("like", "like");

        BOOLEAN_OPERATORS = new HashMap<>();
        BOOLEAN_OPERATORS.put("0", "false");
        BOOLEAN_OPERATORS.put("1", "true");
        BOOLEAN_OPERATORS.put("false", "false");
        BOOLEAN_OPERATORS.put("true", "true");
    }

    /**
     * Get map operator.
     */
    public static String mapOperator(String operatorWord) {
        return OPERATORS.get(operatorWord);
    }

    /**
     * Generate string representation of string field.
     */
    public static String validateString(String fieldValue, Field field) {
        if (Constants.NULL.equals(fieldValue)) {
            return fieldValue;
        }

        return String.format("'%s'", fieldValue);
    }

    /**
     * Generate string representation of boolean field.
     */
    public static String validateBoolean(String fieldValue, Field field) {
        // If the type is complex and not a key, return NULL as only filters are `IS NULL` and `IS NOT NULL`.
        if (field.isNotSimpleType() && !field.isSubField()) {
            return Constants.NULL;
        }
        // If the value passed in is not a valid boolean, return the default boolean value
        if (!BOOLEAN_OPERATORS.containsKey(fieldValue)) {
            return Constants.DEFAULT_BOOLEAN;
        }

        return BOOLEAN_OPERATORS.get(fieldValue);
    }

    /**
     * Validate value.
     */
    public static String validateValue(String fieldValue, Field field) {
        // Dispatch to the correct type
        switch (field.getFieldType()) {
            case Constants.BOOLEAN:
                return validateBoolean(fieldValue, field);
            case Constants.STRING:
                return validateString(fieldValue, field);
            default:
                // Return the passed in value by default
                return fieldValue;
        }
    }
}
