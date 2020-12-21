/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.dao.AbstractEntityDAO;
import com.yahoo.cubed.dao.DAOFactory;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import org.hibernate.Session;

import java.util.stream.Collectors;
import java.util.List;


/**
 * Schema service implementation.
 */
public class SchemaServiceImpl extends AbstractServiceImpl<Schema> implements SchemaService {

    @Override
    protected AbstractEntityDAO<Schema> getDAO() {
        return DAOFactory.schemaDAO();
    }


    /**
     * Fetch all schema names.
     * @param requireActive whether or not require returned schemas are active (not deleted) in db.
     * @return A list of schema names.
     * @throws DatabaseException
     */
    @Override
    public List<String> fetchAllName(boolean requireActive) throws DatabaseException {
        List<Schema> schemas = super.fetchAll();
        return schemas.stream()
                .filter(s -> !requireActive || !s.getIsSchemaDeleted())
                .map(s -> s.getPrimaryName())
                .sorted()
                .collect(Collectors.toList());
    }

    /**
     * Fetch all schema with bullet.
     */
    public List<String> fetchAllBulletSchemaName() throws DatabaseException {
        List<Schema> schemas = super.fetchAll();
        List<String> schemaNameList = schemas.stream()
                .filter(s -> !s.getSchemaDisableBullet() && !s.getIsSchemaDeleted())
                .map(s -> s.getPrimaryName())
                .sorted()
                .collect(Collectors.toList());

        // Use null in web page to make judgment
        if (schemaNameList.size() == 0) {
            return null;
        }
        return schemaNameList;
    }

    /**
     * Fetch all schemas support funnel.
     */
    public List<String> fetchAllFunnelSchemaName() throws DatabaseException {
        List<Schema> schemas = super.fetchAll();
        List<String> schemaNameList = schemas.stream()
                .filter(s -> !s.getSchemaDisableFunnel() && !s.getIsSchemaDeleted())
                .map(s -> s.getPrimaryName())
                .sorted()
                .collect(Collectors.toList());

        // Use null in web page to make judgment
        if (schemaNameList.size() == 0) {
            return null;
        }
        return schemaNameList;
    }

    @Override
    protected Schema preUpdateCheck(Session session, Schema newModel) throws DataValidatorException {
        super.preUpdateCheck(session, newModel);
        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check name exist
        Schema modelWithSameName = this.getDAO().fetchByName(session, newModel.getPrimaryName());
        if (modelWithSameName == null) {
            throw new DataValidatorException("The name of the " + modelName + " does not exist.");
        }

        return modelWithSameName;
    }

    /**
     * Model check before model delete.
     */
    protected Schema preDeleteCheck(Session session, String schemaName) throws DataValidatorException {
        // check if name exists
        Schema oldModel = this.getDAO().fetchByName(session, schemaName);
        if (oldModel == null) {
            throw new DataValidatorException("The " + this.getDAO().getEntityClass().getSimpleName() + " with name [" + schemaName + "] does not exist.");
        }
        return oldModel;
    }

    /**
     * Fetch entity by name.
     */
    public Schema fetch(String name) throws DataValidatorException, DatabaseException {

        Session session = this.createSession();

        Schema model = null;
        try {
            model = this.getDAO().fetchByName(session, name);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: cannot query " + this.getDAO().getEntityClass().getSimpleName() + "  with name [" + name + "].", e);
        } finally {
            this.reclaimSession(session);
        }

        if (model == null) {
            throw new DataValidatorException("Cannot find " + this.getDAO().getEntityClass().getSimpleName() + "  with name [" + name + "].");
        }
        return model;
    }

    /**
     * Delete entity.
     */
    public void delete(String name) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        Schema model = this.preDeleteCheck(session, name);

        try {
            this.getDAO().delete(session, model);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: delete " + this.getDAO().getEntityClass().getSimpleName() + " failed.", e);
        } finally {
            this.reclaimSession(session);
        }
    }

}
