/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

import java.util.List;


/**
 * Schema service.
 */
public interface SchemaService extends AbstractService<Schema> {
    /** Fetch all schemas to get list of name.*/
    public List<String> fetchAllName(boolean requireNotDeleted) throws DatabaseException;
    /** Fetch all schemas which support bullet.*/
    public List<String> fetchAllBulletSchemaName() throws DatabaseException;
    /** Fetch all schemas which support funnel.*/
    public List<String> fetchAllFunnelSchemaName() throws DatabaseException;
    /** Delete by name. */
    public void delete(String name) throws DataValidatorException, DatabaseException;
    /** Fetch by name.*/
    public Schema fetch(String name) throws DataValidatorException, DatabaseException;
}
