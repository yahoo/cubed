/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.PipelineProjectionVM;
import org.hibernate.Session;

/**
 * Pipeline projection value mapping data access object implementation.
 */
public class PipelineProjectionVMDAOImpl extends AbstractAssociationDAOImpl<PipelineProjectionVM> implements PipelineProjectionVMDAO {

    /**
     * Get the entity class: PipelineProjectionVM.
     */
    @Override
    public Class<PipelineProjectionVM> getEntityClass() {
        return PipelineProjectionVM.class;
    }

    /**
     * Unsupported method.
     */
    @Override
    public PipelineProjectionVM fetchByName(Session session, String name) {
        throw new UnsupportedOperationException();
    }
}
