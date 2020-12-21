/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

import com.yahoo.cubed.model.FunnelGroup;
import java.util.List;
import org.hibernate.Session;

/**
 * Funnel Group data access object.
 */
public interface FunnelGroupDAO extends AbstractEntityDAO<FunnelGroup> {
    /**
     * Fetch all the funnel groups, two fetch modes: eager and lazy.
     * @param session open session
     * @param mode eager or lazy
     * @return a list of pipelines
     */
    public List<FunnelGroup> fetchAll(Session session, FetchMode mode);
}
