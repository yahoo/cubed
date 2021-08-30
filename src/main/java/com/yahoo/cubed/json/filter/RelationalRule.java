/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json.filter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.filter.PipelineRelationalRule;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilterRelationalRule;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.util.Constants;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Data;

/**
 * Relational JSON rule.
 */
@Data
@JsonSerialize
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize
public class RelationalRule extends Filter {
    /** Filter is null condition. */
    public static final String IS_NULL = "is_null";

    /** Filter is not null condition. */
    public static final String IS_NOT_NULL = "is_not_null";

    /** Operator and symbol mapping. */
    public static final Map<String, String> SYMBOLS = new HashMap<>();

    static {
        SYMBOLS.put("equal", "==");
        SYMBOLS.put("not_equal", "!=");
        SYMBOLS.put("less", "<");
        SYMBOLS.put("greater", ">");
        SYMBOLS.put("less_or_equal", "<=");
        SYMBOLS.put("greater_or_equal", ">=");
        SYMBOLS.put("is_null", "==");
        SYMBOLS.put("is_not_null", "!=");
        SYMBOLS.put("rlike", "RLIKE");
        SYMBOLS.put("like", "LIKE");
        SYMBOLS.put("not_like", "NOT LIKE");
        SYMBOLS.put("not_rlike", "NOT RLIKE");
    }

    private String id;
    private String field;
    private String type;
    private String input;
    private String operator;
    private String value;
    private List<String> subfield;

    private static class FieldNameInfo {
        public FieldNameInfo(String fieldName, String fieldKey) {
            this.fieldName = fieldName;
            this.fieldKey = fieldKey;
        }
        public String fieldName;
        public String fieldKey;
    }

    // parse field name by id
    private FieldNameInfo parseFieldName() {
        String fieldName = this.id;
        String fieldKey = null;
        if (this.id.contains("[") && this.id.endsWith("]")) {
            fieldName = this.id.substring(0, this.id.indexOf('['));
            fieldKey = this.id.substring(this.id.indexOf('[') + 1, this.id.indexOf(']'));
        }
        // Set the subfield key (if found)
        if (this.subfield != null && this.subfield.size() > 0) {
            String key = this.subfield.get(0);
            if (key != null && key.trim().length() > 0) {
                fieldKey = key;
            }
        }
        return new FieldNameInfo(fieldName, fieldKey);
    }

    // process value
    private String preprocessValue(String nullValue) {
        String value = this.getValue();

        // Special condition for is_null and is_not_null
        if (this.operator != null && (this.operator.equals(IS_NULL) || this.operator.equals(IS_NOT_NULL))) {
            value = nullValue;
        }

        return value;
    }

    /**
     * Convert relational filter rule to rule model.
     */
    @Override
    public PipelineRelationalRule toModel(String schemaName) throws NumberFormatException, DataValidatorException, DatabaseException {

        PipelineRelationalRule ruleModel = new PipelineRelationalRule();

        // get field name and key
        FieldNameInfo nameInfo = this.parseFieldName();
        String fieldName = nameInfo.fieldName;
        String fieldKey = nameInfo.fieldKey;

        // fetch field by name
        Field field = ServiceFactory.fieldService().fetchByName(schemaName + Field.FIELD_NAME_SEPARATOR + fieldName);
        ruleModel.setField(field);

        // Set the subfield key (if found)
        ruleModel.setKey(fieldKey);

        // Set the operator
        ruleModel.setOperator(this.operator);

        // Get the rules value
        String value = this.preprocessValue(Constants.NULL);

        // Set the value
        if (value != null && !value.trim().isEmpty()) {
            ruleModel.setValue(value);
        }

        return ruleModel;
    }

    @Override
    protected BulletQueryFilter toBulletQueryFilterKernel()
            throws NumberFormatException, DataValidatorException, DatabaseException {
        BulletQueryFilterRelationalRule ruleModel = new BulletQueryFilterRelationalRule();

        // get field name and key
        FieldNameInfo nameInfo = this.parseFieldName();
        String fieldName = nameInfo.fieldName;
        String fieldKey = nameInfo.fieldKey;

        String fullName = fieldKey == null ? fieldName : (fieldName + "." + fieldKey);

        // set field
        ruleModel.setField(fullName);

        // set operator
        ruleModel.setOperation(SYMBOLS.get(this.operator.toLowerCase()));

        // set value
        ruleModel.addValue(this.preprocessValue(Constants.NULL));

        return ruleModel;
    }
}
