/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.json.filter.Filter;
import lombok.Data;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Bullet query filter.
 */
@Data @JsonSerialize @Slf4j
public abstract class BulletQueryFilter {
    /** Filter operation. */
    @Setter
    public String operation;

    /**
     * Perform post translation process.
     */
    public BulletQueryFilter postTranslationProcess() {
        return this;
    }

    /**
     * Create Bullet query filter from JSON.
     */
    public static BulletQueryFilter createBulletQueryFilterInstance(String pipelineFilterJson) {
        if (pipelineFilterJson == null || pipelineFilterJson.isEmpty()) {
            return null;
        }
        try {
            // proxy
            return Filter.fromJson(pipelineFilterJson).toBulletQueryFilter();
        } catch (Exception e) {
            log.error(e.getMessage(), e);
            return null;
        }
    }
}
