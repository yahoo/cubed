/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.service.bullet.model.filter.tansformer.RuleTransformerFactory;
import java.util.ArrayList;
import java.util.List;
import lombok.Data;
import lombok.Setter;

/**
 * Bullet quert filter rule.
 */
@Data
@JsonSerialize
public class BulletQueryFilterRelationalRule extends BulletQueryFilter {
    /** Field. */
    @Setter
    public String field;
    /** List of values. */
    public List<String> values;

    /**
     * Add value to list of values in filter.
     */
    public void addValue(String value) {
        if (this.values == null) {
            this.values = new ArrayList<>();
        }
        this.values.add(value);
    }

    /**
     * Process the filter (i.e. delete filter_tag is NULL condition) after translation.
     */
    @Override
    public BulletQueryFilter postTranslationProcess() {
        return RuleTransformerFactory.filterTagNullRelationalRuleDeletion().transform(this);
    }
}
