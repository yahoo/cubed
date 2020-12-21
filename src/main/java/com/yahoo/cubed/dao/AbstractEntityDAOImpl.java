/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.AbstractModel;
import java.util.ArrayList;
import java.util.List;
import org.hibernate.Session;
import org.hibernate.Transaction;

/**
 * Implementation of abstract entity data access object.
 * @param <T> Type.
 */
public abstract class AbstractEntityDAOImpl<T extends AbstractModel> implements AbstractEntityDAO<T> {
    /**
     * Save model kernel.
     */
    protected void saveKernel(Session session, T model) {
        session.save(model);
        session.flush();
    }

    /**
     * Update model kernel.
     */
    protected void updateKernel(Session session, T oldModel, T newModel) {
        session.merge(newModel);
        session.flush();
    }

    /**
     * Delete model kernel.
     */
    protected void deleteKernel(Session session, T model) {
        session.delete(model);
        session.flush();
    }
    
    /**
     * Fetch by ID.
     */
    @SuppressWarnings("unchecked")
    public T fetch(Session session, long id) {
        Object result = session.get(this.getEntityClass(), id);
        if (result == null) {
            return null;
        }
        return (T) result;
    }
    
    /**
     * Fetch all the entities in a table.
     */
    @Override
    @SuppressWarnings("unchecked")
    public List<T> fetchAll(Session session) {
        List<?> results = session.createCriteria(this.getEntityClass()).list();
        if (results == null) {
            return new ArrayList<>();
        }
        return (List<T>) results;
    }

    /**
     * Save model. It is transaction. Will roll back if it fails.
     */
    @Override
    public void save(Session session, T model) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            this.saveKernel(session, model);
            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            throw e;
        }
    }
    
    /**
     * Update method overloading.
     */
    @Override
    public void update(Session session, T newModel) {
        this.update(session, null, newModel);
    }

    /**
     * Update model. It is transaction. Will roll back if it fails.
     */
    @Override
    public void update(Session session, T oldModel, T newModel) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            this.updateKernel(session, oldModel, newModel);
            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            throw e;
        }
    }

    /**
     * Delete model. It is transaction. Will roll back if it fails.
     */
    @Override
    public void delete(Session session, T model) {
        Transaction tx = null;
        try {
            tx = session.beginTransaction();
            this.deleteKernel(session, model);
            tx.commit();
        } catch (RuntimeException e) {
            if (tx != null) {
                tx.rollback();
            }
            throw e;
        }
    }
}
