/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import java.util.List;
import java.util.regex.Pattern;

import org.hibernate.Session;

import com.yahoo.cubed.dao.AbstractEntityDAO;
import com.yahoo.cubed.model.AbstractModel;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.source.HibernateSessionFactoryManager;

/**
 * Abstract service implementation.
 * @param <T> Type
 */
public abstract class AbstractServiceImpl<T extends AbstractModel> implements AbstractService<T> {
    /** Name pattern for model. */
    protected static final Pattern NAME_PATTERN = Pattern.compile("^[a-zA-Z][a-zA-Z0-9_]*$");
    /** Entity data access object. */
    protected abstract AbstractEntityDAO<T> getDAO();

    /** Check before saving model. */
    protected void preSaveCheck(Session session, T model) throws DataValidatorException {
        String modelName = this.getDAO().getEntityClass().getSimpleName();

        // check model is not null
        if (model == null) {
            throw new DataValidatorException(modelName + " model is not provided.");
        }
        // check name is set
        if (model.getPrimaryName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }
        // check name is valid
        if (!this.isNameValid(model.getPrimaryName())) {
            throw new DataValidatorException("The name of the " + modelName + " should start with an English letter. It should contain only English letters and underscores.");
        }
        // check name is unique
        if (this.getDAO().fetchByName(session, model.getPrimaryName()) != null) {
            throw new DataValidatorException("Data mart name '" + model.getPrimaryName() + "' already exists.");
        }
    }

    /**
     * Check model before update.
     */
    protected T preUpdateCheck(Session session, T newModel) throws DataValidatorException {
        String modelName = this.getDAO().getEntityClass().getSimpleName();

        // check model is not null
        if (newModel == null) {
            throw new DataValidatorException(modelName + " is not provided.");
        }
        // check name is set
        if (newModel.getPrimaryName() == null) {
            throw new DataValidatorException("The name of the " + modelName + " is not provided.");
        }
        // check name is valid
        if (!this.isNameValid(newModel.getPrimaryName())) {
            throw new DataValidatorException("The name of the " + modelName + " should start with an English letter. It should contain only English letters and underscores.");
        }

        return null;
    }

    /**
     * Model check before model delete.
     */
    protected T preDeleteCheck(Session session, long id) throws DataValidatorException {
        // check if id valid
        if (id <= 0) {
            throw new DataValidatorException("The id of the field model is not provided.");
        }
        // check if id exists
        T oldModel = this.getDAO().fetch(session, id);
        if (oldModel == null) {
            throw new DataValidatorException("The " + this.getDAO().getEntityClass().getSimpleName() + " with id [" + id + "] does not exist.");
        }
        return oldModel;
    }

    /**
     * Create session.
     */
    protected Session createSession() throws DatabaseException {
        try {
            return HibernateSessionFactoryManager.getSessionFactory().openSession();
        } catch (RuntimeException e) {
            throw new DatabaseException("Cannot create a new session for database connection", e);
        }
    }

    /**
     * Reclaim session.
     */
    protected void reclaimSession(Session session) throws DatabaseException {
        try {
            session.close();
        } catch (RuntimeException e) {
            throw new DatabaseException("Cannot close the session", e);
        }
    }
    
    /**
     * Fetch entity by name.
     */
    @Override
    public T fetchByName(String name) throws DataValidatorException, DatabaseException {

        Session session = this.createSession();

        T model = null;
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
     * Fetch all entities in a table.
     */
    @Override
    public List<T> fetchAll() throws DatabaseException {
        Session session = this.createSession();

        List<T> models = null;

        try {
            models = this.getDAO().fetchAll(session);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: cannot list all for " + this.getDAO().getEntityClass().getSimpleName() + ".", e);
        } finally {
            this.reclaimSession(session);
        }

        return models;
    }

    /**
     * Save entity.
     */
    @Override
    public void save(T model) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        this.preSaveCheck(session, model);

        try {
            this.getDAO().save(session, model);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: save " + this.getDAO().getEntityClass().getSimpleName() + " failed.", e);
        } finally {
            this.reclaimSession(session);
        }
    }

    /**
     * Update entity.
     */
    @Override
    public void update(T newModel) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        T oldModel = this.preUpdateCheck(session, newModel);

        try {
            this.getDAO().update(session, oldModel, newModel);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: update " + this.getDAO().getEntityClass().getSimpleName() + " failed.", e);
        } finally {
            this.reclaimSession(session);
        }
    }

    /**
     * Check if a name is valid.
     */
    protected boolean isNameValid(String s) {
        if (s == null) {
            return false;
        }
        return NAME_PATTERN.matcher(s).matches();
    }
}
