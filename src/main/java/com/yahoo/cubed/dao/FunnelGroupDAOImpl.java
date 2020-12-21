/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import java.util.List;

import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.Criteria;
import org.hibernate.criterion.Restrictions;

/**
 * Funnel Group data access object implementation.
 */
public class FunnelGroupDAOImpl extends AbstractEntityDAOImpl<FunnelGroup> implements FunnelGroupDAO {

    /**
     * Get the entity/model class: FunnelGroup.
     */
    @Override
    public Class<FunnelGroup> getEntityClass() {
        return FunnelGroup.class;
    }

    /**
     * Save the funnel group entity (core implementation).
     */
    @Override
    protected void saveKernel(Session session, FunnelGroup model) {
        super.saveKernel(session, model);
        PipelineProjection.setFunnelGroupId(model.getProjections(), model.getPrimaryIdx());
        DAOFactory.pipelineProjectionDAO().save(session, model.getProjections());

        Pipeline.setFunnelGroupId(model.getPipelines(), model.getPrimaryIdx());
        DAOFactory.pipelineDAO().saveAll(session, model.getPipelines());
    }

    /**
     * Update the funnel group entity (core implementation).
     * Update pipelines and projections as well.
     */
    @Override
    protected void updateKernel(Session session, FunnelGroup oldModel, FunnelGroup newModel) {
        // Update projections
        DAOFactory.pipelineProjectionDAO().update(session, oldModel.getProjections(), newModel.getProjections());
        // Don't update pipelines here. Pipeline updates are taken care of individually outside.
        super.updateKernel(session, oldModel, newModel);
    }

    /**
     * Update the funnel group entity (method overloading).
     */
    @Override
    public void update(Session session, FunnelGroup newModel) {
        FunnelGroup oldModel = this.fetch(session, newModel.getFunnelGroupId());
        if (oldModel == null) {
            return;
        }
        super.update(session, oldModel, newModel);
    }

    /**
     * Delete the funnel group entity (core implementation).
     */
    @Override
    public void deleteKernel(Session session, FunnelGroup model) {
        List<PipelineProjection> projections = model.getProjections();
        List<Pipeline> pipelines = model.getPipelines();

        super.deleteKernel(session, model);

        if (projections != null) {
            PipelineProjection.setFunnelGroupId(projections, model.getPrimaryIdx());
            DAOFactory.pipelineProjectionDAO().delete(session, projections);
        }

        if (pipelines != null) {
            Pipeline.setFunnelGroupId(pipelines, model.getPrimaryIdx());
            DAOFactory.pipelineDAO().deleteAll(session, pipelines);
        }
    }

    /**
     * Performs eager fetch.
     */
    @Override
    public FunnelGroup fetch(Session session, long id) {
        FunnelGroup funnelGroup = super.fetch(session, id);
        if (funnelGroup != null) {
            Hibernate.initialize(funnelGroup.getProjections());
            Hibernate.initialize(funnelGroup.getPipelines());
        }
        return funnelGroup;
    }

    /**
     * Performs lazy fetch.
     */
    @Override
    public List<FunnelGroup> fetchAll(Session session) {
        return this.fetchAll(session, FetchMode.LAZY);
    }

    /**
     * Fetch all the funnel groups.
     * If fetch mode is eager, it will fetch the projections and pipelines for each funnel group.
     * If fetch mode is lazy, projections and pipelines will not be fetched.
     */
    @Override
    public List<FunnelGroup> fetchAll(Session session, FetchMode mode) {
        List<FunnelGroup> groups = super.fetchAll(session);
        if (mode == FetchMode.EAGER) {
            if (groups != null) {
                for (FunnelGroup funnelGroup : groups) {
                    Hibernate.initialize(funnelGroup.getProjections());
                    Hibernate.initialize(funnelGroup.getPipelines());
                }
            }
            return groups;
        } else {
            // default is LAZY
            return FunnelGroup.simplifyFunnelGroupList(groups);
        }
    }

    /**
     * Fetch funnel group by name.
     */
    @Override
    public FunnelGroup fetchByName(Session session, String name) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("funnelGroupName", name));
        FunnelGroup funnelGroup = (FunnelGroup) criteria.uniqueResult();
        if (funnelGroup != null) {
            Hibernate.initialize(funnelGroup.getProjections());
            Hibernate.initialize(funnelGroup.getPipelines());
        }
        return funnelGroup;
    }
}
