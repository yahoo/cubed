/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.service.cardinality.CardinalityEstimationService;
import com.yahoo.cubed.service.cardinality.CardinalityEstimationServiceImpl;
import com.yahoo.cubed.service.querybullet.QueryBulletService;
import com.yahoo.cubed.service.querybullet.QueryBulletServiceImpl;
import lombok.Setter;

/**
 * Service factory.
 */
public class ServiceFactory {
    private static PipelineService pipelineService;
    private static FieldService fieldService;
    private static FieldKeyService fieldKeyService;
    private static SchemaService schemaService;
    private static FunnelGroupService funnelGroupService;

    @Setter
    private static CardinalityEstimationService cardinalityEstimationService;
    @Setter
    private static QueryBulletService queryBulletService;

    /**
     * Get schema service.
     */
    public static SchemaService schemaService() {
        if (schemaService == null) {
            schemaService = new SchemaServiceImpl();
        }
        return schemaService;
    }

    /**
     * Get pipeline service.
     */
    public static PipelineService pipelineService() {
        if (pipelineService == null) {
            pipelineService = new PipelineServiceImpl();
        }
        return pipelineService;
    }

    /**
     * Get field service.
     */
    public static FieldService fieldService() {
        if (fieldService == null) {
            fieldService = new FieldServiceImpl();
        }
        return fieldService;
    }

    /**
     * Get field key service.
     */
    public static FieldKeyService fieldKeyService() {
        if (fieldKeyService == null) {
            fieldKeyService = new FieldKeyServiceImpl();
        }
        return fieldKeyService;
    }

    /**
     * Get cardinality estimation service.
     */
    public static CardinalityEstimationService cardinalityEstimationService() {
        if (cardinalityEstimationService == null) {
            cardinalityEstimationService = new CardinalityEstimationServiceImpl();
        }
        return cardinalityEstimationService;
    }

    /**
     * Get querying bullet service.
     */
    public static QueryBulletService queryBulletService() {
        if (queryBulletService == null) {
            queryBulletService = new QueryBulletServiceImpl();
        }
        return queryBulletService;
    }

    /**
     * Get funnel group service.
     */
    public static FunnelGroupService funnelGroupService() {
        if (funnelGroupService == null) {
            funnelGroupService = new FunnelGroupServiceImpl();
        }
        return funnelGroupService;
    }
}
