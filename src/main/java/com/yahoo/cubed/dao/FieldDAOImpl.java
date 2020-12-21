/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import java.util.List;

import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.FieldKey;

/**
 * Field data access object implementation.
 */
public class FieldDAOImpl extends AbstractAssociationDAOImpl<Field> implements FieldDAO {

    /**
     * Get field entity class.
     */
    @Override
    public Class<Field> getEntityClass() {
        return Field.class;
    }
    
    /**
     * Get field entity by composite name.
     */
    @Override
    public Field fetchByName(Session session, String name) {
        String[] parts = name.split(Field.FIELD_NAME_SEPARATOR);
        String schemaName = parts[0];
        String fieldName = parts[1];
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldName", fieldName));
        return (Field) criteria.uniqueResult();
    }

    /**
     * Get all field entities.
     */
    @Override
    public List<Field> fetchAll(Session session, FetchMode mode) {
        return super.fetchAll(session);
    }
    
    /**
     * Save field entity (core implementation). 
     */
    @Override
    protected void saveKernel(Session session, Field model) {
        super.saveKernel(session, model);
        FieldKey.setFieldId(model.getFieldKeys(), model.getFieldId());
        DAOFactory.fieldKeyDAO().save(session, model.getFieldKeys());
    }
    
    /**
     * Update field entity (core implementation).
     */
    @Override
    protected void updateKernel(Session session, Field oldModel, Field newModel) {
        DAOFactory.fieldKeyDAO().update(session, oldModel.getFieldKeys(), newModel.getFieldKeys());
        super.updateKernel(session, oldModel, newModel);
    }
    
    /**
     * Delete field entity (core implementation).
     */
    @Override
    public void deleteKernel(Session session, Field model) {
        List<FieldKey> keys = model.getFieldKeys();
        super.deleteKernel(session, model);
        if (keys != null) {
            FieldKey.setFieldId(keys, model.getFieldId());
            FieldKey.setSchemaName(keys, model.getSchemaName());
            DAOFactory.fieldKeyDAO().delete(session, keys);
        }
    }

    /**
     * Get field entity by schema.
     */
    public List<Field> fetchBySchemaName(Session session, String schemaName) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        return criteria.list();
    }

    /**
     * Get field entity by composite key.
     */
    public Field fetchByCompositeKey(Session session, String schemaName, long fieldId) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldId", fieldId));
        return (Field) criteria.uniqueResult();
    }


    /**
     * Get field entity by several names.
     */
    public Field fetchBySchemaNameFieldName(Session session, String schemaName, String fieldName) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", schemaName));
        criteria.add(Restrictions.eq("fieldName", fieldName));
        return (Field) criteria.uniqueResult();
    }
}
