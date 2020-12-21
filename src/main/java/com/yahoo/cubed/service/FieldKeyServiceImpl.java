/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.dao.DAOFactory;
import com.yahoo.cubed.dao.FieldKeyDAO;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.service.exception.DatabaseException;
import org.hibernate.Session;
import com.yahoo.cubed.service.exception.DataValidatorException;

/**
 * Field key service implementation.
 */
public class FieldKeyServiceImpl extends AbstractServiceImpl<FieldKey> implements FieldKeyService {
    @Override
    protected FieldKeyDAO getDAO() {
        return DAOFactory.fieldKeyDAO();
    }


    /** Check before saving model. */
    protected void preSaveCheck(Session session, FieldKey model) throws DataValidatorException {
        super.preSaveCheck(session, model);
        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check name is set
        if (model.getKeyName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }
    }

    /**
     * Check if a composite name is valid.
     */
    protected boolean isNameValid(String s) {
        if (s == null) {
            return false;
        }
        String[] parts = s.split(FieldKey.FIELD_KEY_NAME_SEPARATOR);
        if (parts.length != 3) {
            return false;
        }
        return NAME_PATTERN.matcher(parts[2]).matches();
    }

    /**
     * Check model before update.
     */
    protected FieldKey preUpdateCheck(Session session, FieldKey newModel) throws DataValidatorException {
        super.preUpdateCheck(session, newModel);
        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check name is set
        if (newModel.getKeyName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }

        // check if id exists
        FieldKey oldModel = this.getDAO().fetchByCompositeKey(session, newModel.getSchemaName(), newModel.getFieldId(), newModel.getKeyId());
        if (oldModel == null) {
            throw new DataValidatorException("The " + modelName + " with id [" + newModel.getKeyId() + "] does not exist.");
        }

        // check name unique
        FieldKey modelWithSameName = this.getDAO().fetchBySchemaNameFieldIdKeyName(session, newModel.getSchemaName(), newModel.getFieldId(), newModel.getKeyName());
        if (modelWithSameName != null && modelWithSameName.getKeyId() != newModel.getKeyId()) {
            throw new DataValidatorException("The name of the " + modelName + " is already used by another " + modelName + ".");
        }
        return this.getDAO().fetchByCompositeKey(session, newModel.getSchemaName(), newModel.getFieldId(), newModel.getKeyId());
    }

    /**
     * Model check before model delete.
     */
    protected FieldKey preDeleteCheck(Session session, String schemaName, long fieldId, long fieldKeyId) throws DataValidatorException {
        // check if id valid
        if (fieldKeyId <= 0) {
            throw new DataValidatorException("The id of the field key model is not provided.");
        }
        // check if id exists
        FieldKey oldModel = this.getDAO().fetchByCompositeKey(session, schemaName, fieldId, fieldKeyId);
        if (oldModel == null) {
            throw new DataValidatorException("The " + this.getDAO().getEntityClass().getSimpleName() + " with id [" + fieldKeyId + "] does not exist.");
        }
        return oldModel;
    }

    /**
     * Delete entity.
     */
    public void delete(String schemaName, long fieldId, long fieldKeyId) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        FieldKey model = this.preDeleteCheck(session, schemaName, fieldId, fieldKeyId);

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
    public FieldKey fetchByCompositeKey(String schemaName, long fieldId, long keyId) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        FieldKey model = null;
        try {
            model = this.getDAO().fetchByCompositeKey(session, schemaName, fieldId, keyId);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: cannot query " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + keyId + "].", e);
        } finally {
            this.reclaimSession(session);
        }

        if (model == null) {
            throw new DataValidatorException("Cannot find " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + keyId + "] and fieldId " + fieldId + " schema name [" + schemaName + "].");
        }

        return model;
    }

}
