/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import lombok.Data;

/**
 * Bullet filter rule.
 */
@Data
@JsonSerialize
public class BulletQueryFilterLogicalRule extends BulletQueryFilter {
    /** List of clauses in rule. */
    public List<BulletQueryFilter> clauses;

    /**
     * Add clause to list of rules.
     */
    public void addClause(BulletQueryFilter subFilter) {
        if (this.clauses == null) {
            this.clauses = new ArrayList<>();
        }
        this.clauses.add(subFilter);
    }
}
