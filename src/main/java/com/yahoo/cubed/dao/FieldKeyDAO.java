/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.FieldKey;
import org.hibernate.Session;

/**
 * Field key data access object.
 */
public interface FieldKeyDAO extends AbstractAssociationDAO<FieldKey> {
    /**
     * Fetch the field according to the composite key.
     * @param session open session
     * @param schemaName schema name
     * @param fieldId field id
     * @param fieldKeyId field key id
     * @return field
     */
    public FieldKey fetchByCompositeKey(Session session, String schemaName, long fieldId, long fieldKeyId);

    /**
     * Fetch the field according to the schema name, field name and key name.
     * @param session open session
     * @param schemaName schema name
     * @param fieldId field Id
     * @param keyName key name
     * @return field
     */
    public FieldKey fetchBySchemaNameFieldIdKeyName(Session session, String schemaName, long fieldId, String keyName);
}
