/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Schema;
import org.hibernate.Criteria;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

import java.util.List;

/**
 * Schema data access object implementation.
 */
public class SchemaDAOImpl extends AbstractEntityDAOImpl<Schema> implements SchemaDAO {


    @Override
    public Class<Schema> getEntityClass() {
        return Schema.class;
    }

    @Override
    public Schema fetchByName(Session session, String name) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("schemaName", name));
        return (Schema) criteria.uniqueResult();
    }

    /**
     * Delete field entity (core implementation).
     */
    @Override
    public void deleteKernel(Session session, Schema model) {
        List<Field> fields = model.getFields();
        super.deleteKernel(session, model);
        if (fields != null) {
            Field.setSchemaName(fields, model.getPrimaryName());
            DAOFactory.fieldDAO().delete(session, fields);
        }
    }

    /**
     * Update the schema entity.
     */
    @Override
    public void update(Session session, Schema newModel) {
        Schema oldModel = this.fetchByName(session, newModel.getSchemaName());
        if (oldModel == null) {
            return;
        }
        super.update(session, oldModel, newModel);
    }
}
