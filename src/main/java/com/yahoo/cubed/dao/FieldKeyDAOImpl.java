/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.FieldKey;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

/**
 * Field key data access object implementation.
 */
public class FieldKeyDAOImpl extends AbstractAssociationDAOImpl<FieldKey> implements FieldKeyDAO {
    /**
     * Get the entity class: FieldKey.
     */
    @Override
    public Class<FieldKey> getEntityClass() {
        return FieldKey.class;
    }

    /**
     * Fetch entity by name for composite name.
     */
    @Override
    public FieldKey fetchByName(Session session, String name) {
        String[] parts = name.split(FieldKey.FIELD_KEY_NAME_SEPARATOR);
        String schemaName = parts[0];
        long fieldId = Long.valueOf(parts[1]);
        String keyName = parts[2];

        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldId", fieldId));
        criteria.add(Restrictions.eq("keyName", keyName));
        return (FieldKey) criteria.uniqueResult();
    }

    /**
     * Get field entity by several names.
     */
    public FieldKey fetchBySchemaNameFieldIdKeyName(Session session, String schemaName, long fieldId, String keyName) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldId", fieldId));
        criteria.add(Restrictions.eq("keyName", keyName));
        return (FieldKey) criteria.uniqueResult();
    }

    /**
     * Get field entity by composite key.
     */
    public FieldKey fetchByCompositeKey(Session session, String schemaName, long fieldId, long fieldKeyId) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldId", fieldId));
        criteria.add(Restrictions.eq("keyId", fieldKeyId));
        return (FieldKey) criteria.uniqueResult();
    }
}
