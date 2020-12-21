/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import java.util.List;
import org.hibernate.Criteria;
import org.hibernate.Hibernate;
import org.hibernate.Session;
import org.hibernate.criterion.Restrictions;

/**
 * Pipeline data access object implementation.
 */
public class PipelineDAOImpl extends AbstractEntityDAOImpl<Pipeline> implements PipelineDAO {
    
    /**
     * Get the entity/model class: Pipeline.
     */
    @Override
    public Class<Pipeline> getEntityClass() {
        return Pipeline.class;
    }
    
    /**
     * Save pipeline entity (core implementation).
     */
    @Override
    protected void saveKernel(Session session, Pipeline model) {
        super.saveKernel(session, model);
        PipelineProjection.setPipelineId(model.getProjections(), model.getPrimaryIdx());
        DAOFactory.pipelineProjectionDAO().save(session, model.getProjections());
    }
    
    /**
     * Update pipeline entity (core implementation).
     */
    @Override
    protected void updateKernel(Session session, Pipeline oldModel, Pipeline newModel) {
        DAOFactory.pipelineProjectionDAO().update(session, oldModel.getProjections(), newModel.getProjections());
        super.updateKernel(session, oldModel, newModel);
    }

    /**
     * Update pipeline entity (method overloading).
     */
    @Override
    public void update(Session session, Pipeline newModel) {
        Pipeline oldModel = this.fetch(session, newModel.getPipelineId());
        if (oldModel == null) {
            return;
        }
        super.update(session, oldModel, newModel);
    }
    
    /**
     * Delete pipeline entity (core implementation).
     */
    @Override
    public void deleteKernel(Session session, Pipeline model) {
        List<PipelineProjection> projections = model.getProjections();
        super.deleteKernel(session, model);
        if (projections != null) {
            PipelineProjection.setPipelineId(projections, model.getPrimaryIdx());
            DAOFactory.pipelineProjectionDAO().delete(session, projections);
        }
    }
    
    /**
     * Performs eager fetch.
     */
    @Override
    public Pipeline fetch(Session session, long id) {
        Pipeline pipeline = super.fetch(session, id);
        if (pipeline != null) { 
            Hibernate.initialize(pipeline.getProjections());
        }
        return pipeline;
    }
    
    /**
     * Performs lazy fetch.
     */
    @Override
    public List<Pipeline> fetchAll(Session session) {
        return this.fetchAll(session, FetchMode.LAZY);
    }

    /**
     * Fetch all the pipelines.
     * If fetch mode is eager, it will fetch the projections for each pipeline.
     * If fetch mode is lazy, projections will not be fetched. 
     */
    @Override
    public List<Pipeline> fetchAll(Session session, FetchMode mode) {
        List<Pipeline> pipelines = super.fetchAll(session);
        if (mode == FetchMode.EAGER) {
            if (pipelines != null) {
                for (Pipeline pipeline : pipelines) {
                    Hibernate.initialize(pipeline.getProjections());
                }
            }
            return pipelines;
        } else {
            // default is LAZY
            return Pipeline.simplifyPipelineList(pipelines);
        }
    }
    
    /**
     * Fetch pipeline by name.
     */
    @Override
    public Pipeline fetchByName(Session session, String name) {
        Criteria criteria = session.createCriteria(this.getEntityClass());
        criteria.add(Restrictions.eq("pipelineName", name));
        Pipeline pipeline = (Pipeline) criteria.uniqueResult();
        if (pipeline != null) { 
            Hibernate.initialize(pipeline.getProjections());
        }
        return pipeline;
    }

    /**
     * Delete a list of pipelines.
     */
    @Override
    public void deleteAll(Session session, List<Pipeline> pipelines) {

        if (pipelines == null) {
            return;
        }
        for (Pipeline pipeline : pipelines) {
            // delete associated pipeline projections if there are any
            // pipelines affiliated with funnel groups do not have pipeline-specific projections
            if (pipeline != null && pipeline.getProjections() != null) {
                DAOFactory.pipelineProjectionDAO().delete(session, pipeline.getProjections());
            }
            // delete the pipeline
            session.delete(pipeline);
        }
        session.flush();
    }

    /**
     * Save a list of pipelines.
     */
    @Override
    public void saveAll(Session session, List<Pipeline> pipelines) {
        if (pipelines == null) {
            return;
        }
        for (Pipeline pipeline : pipelines) {
            // save the pipeline
            session.save(pipeline);

            // save associated pipeline projections if there are any
            // pipelines affiliated with funnel groups do not have pipeline-specific projections
            if (pipeline != null && pipeline.getProjections() != null) {
                for (PipelineProjection projection : pipeline.getProjections()) {
                    projection.setPipelineId(pipeline.getPipelineId());
                }
                DAOFactory.pipelineProjectionDAO().save(session, pipeline.getProjections());
            }
        }
        session.flush();
    }

    /**
     * Update a list of pipelines.
     */
    @Override
    public void updateAll(Session session, List<Pipeline> oldPipelines, List<Pipeline> newPipelines) {
        this.deleteAll(session, oldPipelines);
        this.saveAll(session, newPipelines);
    }
}
