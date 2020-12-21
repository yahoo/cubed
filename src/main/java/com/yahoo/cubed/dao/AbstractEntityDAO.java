/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.AbstractModel;
import java.util.List;
import org.hibernate.Session;

/**
 * Abstract entity data access object.
 * @param <T> Type.
 */
public interface AbstractEntityDAO<T extends AbstractModel> {
    
    /**
     * Fetch modes. Lazy and eager.
     */
    public static enum FetchMode {
        /** Eager fetch. */
        EAGER,
        /** Lazy fetch. */
        LAZY
    }

    /**
     * get the class of the entity.
     * @return the class of the entity
     */
    public Class<T> getEntityClass();

    /**
     * save a new model into database.
     * function is transactional, will rollback the save if some database operation fails.
     * @param session an open session
     * @param model to be saved into database
     */
    public void save(Session session, T model);

    /**
     * update a model that is already in database.
     * function is transactional, will rollback the update if some database operation fails.
     * @param session an open session
     * @param model to be updated
     */
    public void update(Session session, T model);

    /**
     * update a model that is already in database.
     * function is transactional, will rollback the update if some database operation fails.
     * @param session an open session
     * @param oldModel is the old model
     * @param newModel is the new model
     */
    public void update(Session session, T oldModel, T newModel);

    /**
     * delete a model that is already in database.
     * function is transactional, will rollback the delete if some database operation fails
     * @param session an open session
     * @param model is the model to be deleted
     */
    public void delete(Session session, T model);

    /**
     * fetch a model from database by ID.
     * eager fetch
     * @param session open session
     * @param id is the ID of the model
     * @return the model object if ID exists or null if ID does not exist
     */
    public T fetch(Session session, long id);

    /**
     * fetch all the models in a table.
     * lazy fetch
     * @param session open session
     * @return a list of models
     */
    public List<T> fetchAll(Session session);

    /**
     * fetch a model from database by name.
     * lazy fetch
     * @param session open session
     * @param name is the name of the model
     * @return the model object if name exists or null if name does not exist
     */
    public T fetchByName(Session session, String name);
}
