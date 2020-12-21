/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;

import org.hibernate.Session;
import java.util.List;

/**
 * Pipeline projection data access object implementation.
 */
public class PipelineProjectionDAOImpl extends AbstractAssociationDAOImpl<PipelineProjection> implements PipelineProjectionDAO {

    /**
     * Save pipeline projection entity (method overloading).
     */
    @Override
    public void save(Session session, List<PipelineProjection> model) {
        super.save(session, model);

        if (model != null) {
            for (PipelineProjection p: model) {
                if (p != null && p.getProjectionVMs() != null) {
                    for (PipelineProjectionVM v: p.getProjectionVMs()) {
                        v.setPipelineProjectionId(p.getPipelineProjectionId());
                    }
                    DAOFactory.pipelineProjectionVMDAO().save(session, p.getProjectionVMs());
                }
            }
        }
    }

    /**
     * Update pipeline projection entity (method overloading).
     */
    @Override
    public void update(Session session, List<PipelineProjection> oldModel, List<PipelineProjection> newModel) {
        if (oldModel != null) {
            for (PipelineProjection p: oldModel) {
                if (p != null && p.getProjectionVMs() != null) {
                    DAOFactory.pipelineProjectionVMDAO().delete(session, p.getProjectionVMs());
                }
            }
        }
        super.delete(session, oldModel);
        super.save(session, newModel);
        if (newModel != null) {
            for (PipelineProjection p: newModel) {
                if (p != null && p.getProjectionVMs() != null) {
                    for (PipelineProjectionVM v: p.getProjectionVMs()) {
                        v.setPipelineProjectionId(p.getPipelineProjectionId());
                    }
                    DAOFactory.pipelineProjectionVMDAO().save(session, p.getProjectionVMs());
                }
            }
        }
    }

    /**
     * Delete pipeline projection entity (method overloading).
     */
    @Override
    public void delete(Session session, List<PipelineProjection> model) {
        if (model != null) {
            for (PipelineProjection p: model) {
                if (p != null && p.getProjectionVMs() != null) {
                    DAOFactory.pipelineProjectionVMDAO().delete(session, p.getProjectionVMs());
                }
            }
        }
        super.delete(session, model);
    }

    /**
     * Get the entity class: PipelineProjection.
     */
    @Override
    public Class<PipelineProjection> getEntityClass() {
        return PipelineProjection.class;
    }

    /**
     * Unsupported method.
     */
    @Override
    public PipelineProjection fetchByName(Session session, String name) {
        throw new UnsupportedOperationException();
    }
}
