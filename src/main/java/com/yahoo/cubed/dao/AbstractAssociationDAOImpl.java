/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.AbstractModel;
import java.util.List;
import org.hibernate.Session;
/**
 * Abstract association data access object implementation.
 * @param <T> Type.
 */
public abstract class AbstractAssociationDAOImpl<T extends AbstractModel> extends AbstractEntityDAOImpl<T> implements AbstractAssociationDAO<T> {
    /**
     * Delete a list of entities of type T.
     */
    @Override
    public void delete(Session session, List<T> items) {
        if (items == null) {
            return;
        }
        for (T model : items) {
            session.delete(model);
        }
        session.flush();
    }
    
    /**
     * Save a list of entities of type T.
     */
    @Override
    public void save(Session session, List<T> items) {
        if (items == null) {
            return;
        }
        for (T model : items) {
            session.save(model);
        }
        session.flush();
    }
    
    /**
     * Update a list of entities: delete old entities and save new ones.
     */
    @Override
    public void update(Session session, List<T> oldItems, List<T> newItems) {
        this.delete(session, oldItems);
        this.save(session, newItems);
    }
}
