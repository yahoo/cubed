/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.Field;
import java.util.List;
import org.hibernate.Session;

/**
 * Field data access object.
 */
public interface FieldDAO extends AbstractAssociationDAO<Field> {
    /**
     * Fetch all the fields, two fetch modes: eager and lazy.
     * @param session open session
     * @param mode eager or lazy
     * @return a list of pipelines
     */
    public List<Field> fetchAll(Session session, FetchMode mode);

    /**
     * Fetch all the fields according to the schemaId.
     * @param session open session
     * @param schemaName schema name
     * @return a list of fields
     */
    public List<Field> fetchBySchemaName(Session session, String schemaName);

    /**
     * Fetch the field according to the composite key.
     * @param session open session
     * @param schemaName schema name
     * @param fieldId field id
     * @return field
     */
    public Field fetchByCompositeKey(Session session, String schemaName, long fieldId);

    /**
     * Fetch the field according to the schema name and field name.
     * @param session open session
     * @param schemaName schema name
     * @param fieldName field name
     * @return field
     */
    public Field fetchBySchemaNameFieldName(Session session, String schemaName, String fieldName);
}
