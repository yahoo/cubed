/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import java.util.List;

import com.yahoo.cubed.model.AbstractModel;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

/**
 * Abstract service interface.
 * @param <T> Type
 */
public interface AbstractService<T extends AbstractModel> {
    /** Fetch by name. */
    public T fetchByName(String name) throws DataValidatorException, DatabaseException;
    /** Fetch all. */
    public List<T> fetchAll() throws DatabaseException;
    /** Save model. */
    public void save(T model) throws DataValidatorException, DatabaseException;
    /** Update model. */
    public void update(T model) throws DataValidatorException, DatabaseException;
}
