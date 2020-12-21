/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.AbstractModel;
import java.util.List;
import org.hibernate.Session;

/**
 * Abstract association data access object.
 * @param <T> Type.
 */
public interface AbstractAssociationDAO<T extends AbstractModel> extends AbstractEntityDAO<T> {
    /**
     * Delete a list of models in an association table.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param items a list of models to be deleted
     */
    public void delete(Session session, List<T> items);
    
    /**
     * Save a list of models into an association table.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param items a list of models to be saved
     */
    public void save(Session session, List<T> items);
    
    /**
     * Replace a list of old models in an association table into new ones.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param oldItems a list of old models to be deleted
     * @param newItems a list of new models to be added
     */
    public void update(Session session, List<T> oldItems, List<T> newItems);
}
