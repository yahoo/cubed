/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

/**
 * Field service.
 */
public interface FieldService extends AbstractService<Field> {
    /** Delete entity by composite key. */
    public void delete(String schemaName, long fieldId) throws DataValidatorException, DatabaseException;
    /** Fetch entity by composite key. */
    public Field fetchByCompositeKey(String schemaName, long fieldId) throws DataValidatorException, DatabaseException;
}
