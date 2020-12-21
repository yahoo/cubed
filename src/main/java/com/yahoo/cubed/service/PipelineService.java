/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

/**
 * Pipeline service.
 */
public interface PipelineService extends AbstractService<Pipeline> {
    /** Delete by ID. */
    public void delete(long id) throws DataValidatorException, DatabaseException;
    /** Fetch by ID.*/
    public Pipeline fetch(long id) throws DataValidatorException, DatabaseException;
}
