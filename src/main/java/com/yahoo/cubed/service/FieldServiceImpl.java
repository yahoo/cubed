/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.dao.DAOFactory;
import com.yahoo.cubed.dao.FieldDAO;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import org.hibernate.Session;

/**
 * Field service implementation.
 */
public class FieldServiceImpl extends AbstractServiceImpl<Field> implements FieldService {
    @Override
    protected FieldDAO getDAO() {
        return DAOFactory.fieldDAO();
    }

    /**
     * Check if a composite name is valid.
     */
    protected boolean isNameValid(String s) {
        if (s == null) {
            return false;
        }
        String[] parts = s.split(Field.FIELD_NAME_SEPARATOR);
        if (parts.length != 2) {
            return false;
        }
        return NAME_PATTERN.matcher(parts[1]).matches();
    }

    /** Check before saving model. */
    protected void preSaveCheck(Session session, Field model) throws DataValidatorException {
        super.preSaveCheck(session, model);
        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check name is set
        if (model.getFieldName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }
    }

    /**
     * Check model before update.
     */
    protected Field preUpdateCheck(Session session, Field newModel) throws DataValidatorException {
        super.preUpdateCheck(session, newModel);
        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check name is set
        if (newModel.getFieldName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }

        // check if id exists
        Field oldModel = this.getDAO().fetchByCompositeKey(session, newModel.getSchemaName(), newModel.getFieldId());
        if (oldModel == null) {
            throw new DataValidatorException("The " + modelName + " with id [" + newModel.getFieldId() + "] does not exist.");
        }

        // check name unique
        Field modelWithSameName = this.getDAO().fetchBySchemaNameFieldName(session, newModel.getSchemaName(), newModel.getFieldName());
        if (modelWithSameName != null && modelWithSameName.getFieldId() != newModel.getFieldId()) {
            throw new DataValidatorException("The name of the " + modelName + " is already used by another " + modelName + ".");
        }
        return this.getDAO().fetchByCompositeKey(session, newModel.getSchemaName(), newModel.getFieldId());
    }

    /**
     * Model check before model delete.
     */
    protected Field preDeleteCheck(Session session, String schemaName, long fieldId) throws DataValidatorException {
        // check if id valid
        if (fieldId <= 0) {
            throw new DataValidatorException("The id of the field model is not provided.");
        }
        // check if id exists
        Field oldModel = this.getDAO().fetchByCompositeKey(session, schemaName, fieldId);
        if (oldModel == null) {
            throw new DataValidatorException("The " + this.getDAO().getEntityClass().getSimpleName() + " with id [" + fieldId + "] does not exist.");
        }
        return oldModel;
    }

    /**
     * Delete entity.
     */
    public void delete(String schemaName, long fieldId) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        Field model = this.preDeleteCheck(session, schemaName, fieldId);

        try {
            this.getDAO().delete(session, model);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: delete " + this.getDAO().getEntityClass().getSimpleName() + " failed.", e);
        } finally {
            this.reclaimSession(session);
        }
    }

    /**
     * Fetch entity.
     */
    public Field fetchByCompositeKey(String schemaName, long fieldId) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        Field model = null;
        try {
            model = this.getDAO().fetchByCompositeKey(session, schemaName, fieldId);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: cannot query " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + fieldId + "].", e);
        } finally {
            this.reclaimSession(session);
        }

        if (model == null) {
            throw new DataValidatorException("Cannot find " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + fieldId + "] and schema name [" + schemaName + "].");
        }

        return model;
    }
}
