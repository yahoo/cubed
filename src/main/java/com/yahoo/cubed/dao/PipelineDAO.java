/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.Pipeline;
import java.util.List;
import org.hibernate.Session;

/**
 * Pipeline data access object.
 */
public interface PipelineDAO extends AbstractEntityDAO<Pipeline> {
    /**
     * Fetch all the pipelines, two fetch modes: eager and lazy.
     * @param session open session
     * @param mode eager or lazy
     * @return a list of pipelines
     */
    public List<Pipeline> fetchAll(Session session, FetchMode mode);

    /**
     * Delete a list of models in a pipeline entity table.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param pipelines a list of models to be deleted
     */
    public void deleteAll(Session session, List<Pipeline> pipelines);
    
    /**
     * Save a list of models into a pipeline entity table.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param pipelines a list of models to be saved
     */
    public void saveAll(Session session, List<Pipeline> pipelines);
    
    /**
     * Replace a list of old models in a pipeline entity table into new ones.
     * Not transactional, need a transaction wrapper to make it transactional.
     * @param session an open session
     * @param oldPipelines a list of old models to be deleted
     * @param newPipelines a list of new models to be added
     */
    public void updateAll(Session session, List<Pipeline> oldPipelines, List<Pipeline> newPipelines);
}
