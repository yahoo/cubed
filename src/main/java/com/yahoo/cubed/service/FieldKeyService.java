/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

/**
 * Field key service.
 */
public interface FieldKeyService extends AbstractService<FieldKey> {
    /** Delete entity by composite key. */
    public void delete(String schemaName, long fieldId, long fieldKeyId) throws DataValidatorException, DatabaseException;
    /** Fetch entity by composite key. */
    public FieldKey fetchByCompositeKey(String schemaName, long fieldId, long keyId) throws DataValidatorException, DatabaseException;
}
