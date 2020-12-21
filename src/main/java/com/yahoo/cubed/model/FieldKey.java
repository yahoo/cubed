/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import java.io.Serializable;
import java.util.List;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.IdClass;
import javax.persistence.Id;

import lombok.Getter;
import lombok.Setter;

/**
 * Field key.
 */
@Entity
@Table(name = "field_key")
@IdClass(FieldKey.FieldKeyPrimaryKey.class)
public class FieldKey implements AbstractModel {
    /** Separator for fetch by field key name: SchemaName + Separator + FieldId + Separator + KeyId. */
    public static final String FIELD_KEY_NAME_SEPARATOR = "-";

    static class FieldKeyPrimaryKey implements Serializable {
        @Column(name = "key_id", nullable = false, updatable = false)
        private long keyId;

        @Column(name = "field_id", nullable = false, updatable = false)
        private long fieldId;

        @Column(name = "schema_name", nullable = false, updatable = false)
        private String schemaName;
    }

    /**
     * Id of field key.
     */
    @Id
    @Getter @Setter
    @Column(name = "key_id", nullable = false)
    private long keyId;

    /**
     * Name of field key.
     */
    @Getter @Setter
    @Column(name = "key_name", nullable = false)
    private String keyName;
    
    /**
     * Id of the field that the key belongs to.
     */
    @Id
    @Getter @Setter
    @Column(name = "field_id", nullable = false)
    private long fieldId;

    /** Schema Name. */
    @Id
    @Column(name = "schema_name", nullable = false)
    @Getter @Setter
    private String schemaName;

    /**
     * Return composite name.
     */
    @Override
    public String getPrimaryName() {
        return schemaName + FIELD_KEY_NAME_SEPARATOR + fieldId + FIELD_KEY_NAME_SEPARATOR + keyName;
    }
    
    /**
     * Set field id for a list of field keys.
     */
    public static void setFieldId(List<FieldKey> keys, long fieldId) {
        if (keys == null) {
            return;
        }
        for (FieldKey key : keys) {
            key.setFieldId(fieldId);
        }
    }

    /**
     * Set schema name for a list of field keys.
     */
    public static void setSchemaName(List<FieldKey> keys, String schemaName) {
        if (keys == null) {
            return;
        }
        for (FieldKey key : keys) {
            key.setSchemaName(schemaName);
        }
    }
}
