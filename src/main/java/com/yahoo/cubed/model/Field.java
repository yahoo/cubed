/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import com.yahoo.cubed.util.Measurement;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.JoinColumns;
import javax.persistence.JoinColumn;
import javax.persistence.Id;
import javax.persistence.IdClass;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import com.yahoo.cubed.util.Constants;

/**
 * Field, used in projections and filters.
 */
@Entity
@Table(name = "field")
@IdClass(Field.FieldPrimaryKey.class)
public class Field implements AbstractModel {
    private static final Pattern MAP_TYPE_PATTERN = Pattern.compile("map<([^,]+),(.+)>");

    /** Separator for solving fetch by name for field name: SchemaName + Separator + FieldId. */
    public static final String FIELD_NAME_SEPARATOR = "-";

    static class FieldPrimaryKey implements Serializable {
        @Column(name = "field_id", nullable = false, updatable = false)
        private long fieldId;

        @Column(name = "schema_name", nullable = false, updatable = false)
        private String schemaName;
    }

    /** Field ID. */
    @Id
    @Getter @Setter
    @Column(name = "field_id", nullable = false, updatable = false)
    private long fieldId;
    
    /** Field name. */
    @Column(name = "field_name", nullable = false)
    @Getter @Setter
    private String fieldName;
    
    /** Field type. */
    @Column(name = "field_type", nullable = false)
    @Getter @Setter
    private String fieldType;

    /** Measurement type. */
    @Column(name = "measurement_type")
    @Getter @Setter
    private String measurementType;

    /** Schema Name. */
    @Id
    @Getter @Setter
    @Column(name = "schema_name", nullable = false, updatable = false)
    private String schemaName;
    
    /** Field keys for maps. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(cascade = {}, fetch = FetchType.EAGER)
    @JoinColumns({
        @JoinColumn(name = "schema_name", referencedColumnName = "schema_name", updatable = false),
        @JoinColumn(name = "field_id", referencedColumnName = "field_id", updatable = false)
        })
    @Getter @Setter
    private List<FieldKey> fieldKeys;
    
    /** Key for maps. */
    @Transient
    @Getter @Setter
    private String key;
    
    /** For UI. Eg: cookieInfo[count] */
    @Transient
    @Getter
    private String fieldNameId;
    
    /**
     * Create field.
     */
    public Field() {
        this.key = null;
    }

    /**
     * Get measurement.
     */
    public Measurement getMeasurement() {
        return Measurement.byName(this.measurementType);
    }
    
    /**
     * Get field name ID.
     */
    public String getFieldNameId() {
        return fieldNameId;
    }

    /**
     * Check if field a sub-field.
     */
    public boolean isSubField() {
        return this.key != null && !this.key.isEmpty();
    }

    /**
     * Filter fields by selected.
     */
    public static List<Field> filterByMeasurement(List<Field> fields, Measurement selected) {
        List<Field> ret = new ArrayList<>();
        for (Field field : fields) {
            if (selected.measurementTypeCode.equals(field.getMeasurementType())) {
                ret.add(field);
            }
        }
        return ret;
    }

    /**
     * Return composite name.
     */
    @Override
    public String getPrimaryName() {
        return schemaName + FIELD_NAME_SEPARATOR + fieldName;
    }
    
    /**
     * Convert field to string.
     */
    @Override
    public String toString() {
        if (this.isSubField()) {
            return String.format("%s['%s']", this.getFieldName(), this.getKey());
        } else {
            return String.format("%s", this.getFieldName());
        }
    }
    
    /**
     * Set field name by formatting field name and key, if map.
     * This is used in the UI.
     */
    public String setFieldNameId() {
        if (this.isSubField()) {
            this.fieldNameId = String.format("%s[%s]", this.getFieldName(), this.getKey());
        } else {
            this.fieldNameId = String.format("%s", this.getFieldName());
        }
        return this.fieldNameId;
    }
    
    /**
     * Get formatted string version of the field.
     */
    public String strOfSimpleName() {
        if (this.isSubField()) {
            return String.format("%s['%s']", this.getFieldName(), this.getKey());
        } else {
            return String.format("%s", this.getFieldName());
        }
    }

    /**
     * Get the type of the field. If a map, get the value type.
     */
    public String getFieldType() {
        if (this.fieldType != null) {
            Matcher matcher = MAP_TYPE_PATTERN.matcher(this.fieldType);
            if (matcher.find()) {
                return matcher.group(2);
            }
        }
        return this.fieldType;
    }

    /**
     * Return true if type is not simple (string, integer, float, boolean).
     */
    public boolean isNotSimpleType() {
        if (this.fieldType.equals(Constants.STRING) || 
            this.fieldType.equals(Constants.INTEGER) || 
            this.fieldType.equals(Constants.FLOAT) || 
            this.fieldType.equals(Constants.BOOLEAN)) {
            return false;
        }
        return true;
    }

    /**
     * Set schema id for a list of fields.
     */
    public static void setSchemaName(List<Field> fields, String schemaName) {
        if (fields == null) {
            return;
        }
        for (Field field: fields) {
            field.setSchemaName(schemaName);
        }
    }
}
