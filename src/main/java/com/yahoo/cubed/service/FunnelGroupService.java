/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;

/**
 * Funnel Group service.
 */
public interface FunnelGroupService extends AbstractService<FunnelGroup> {
    /** Delete by ID. */
    public void delete(long id) throws DataValidatorException, DatabaseException;
    /** Fetch by ID.*/
    public FunnelGroup fetch(long id) throws DataValidatorException, DatabaseException;
}
