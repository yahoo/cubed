/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.dao;

/**
 * Data access object factory.
 */
public class DAOFactory {
    private static FieldDAO fieldDAO = null;
    private static PipelineDAO pipelineDAO = null;
    private static PipelineProjectionDAO pipelineProjectionDAO = null;
    private static FieldKeyDAO fieldKeyDAO = null;
    private static PipelineProjectionVMDAO pipelineProjectionVMDAO = null;
    private static SchemaDAO schemaDAO = null;
    private static FunnelGroupDAO funnelGroupDAO = null;
        
    /**
     * Field data access object.
     */
    public static FieldDAO fieldDAO() {
        if (fieldDAO == null) {
            fieldDAO = new FieldDAOImpl();
        }
        return fieldDAO;
    }

    /**
     * Pipeline data access object.
     */
    public static PipelineDAO pipelineDAO() {
        if (pipelineDAO == null) {
            pipelineDAO = new PipelineDAOImpl();
        }
        return pipelineDAO;
    }

    /**
     * Pipeline projection data access object.
     */
    public static PipelineProjectionDAO pipelineProjectionDAO() {
        if (pipelineProjectionDAO == null) {
            pipelineProjectionDAO = new PipelineProjectionDAOImpl();
        }
        return pipelineProjectionDAO;
    }
    
    /**
     * Field key data access object.
     */
    public static FieldKeyDAO fieldKeyDAO() {
        if (fieldKeyDAO == null) {
            fieldKeyDAO = new FieldKeyDAOImpl();
        }
        return fieldKeyDAO;
    }

    /**
     * Pipeline projection value mapping data access object.
     */
    public static PipelineProjectionVMDAO pipelineProjectionVMDAO() {
        if (pipelineProjectionVMDAO == null) {
            pipelineProjectionVMDAO = new PipelineProjectionVMDAOImpl();
        }
        return pipelineProjectionVMDAO;
    }

    /**
     * Schema data access object.
     */
    public static SchemaDAO schemaDAO() {
        if (schemaDAO == null) {
            schemaDAO = new SchemaDAOImpl();
        }
        return schemaDAO;
    }

    /**
     * FunnelGroup data access object.
     */
    public static FunnelGroupDAO funnelGroupDAO() {
        if (funnelGroupDAO == null) {
            funnelGroupDAO = new FunnelGroupDAOImpl();
        }
        return funnelGroupDAO;
    }
}
