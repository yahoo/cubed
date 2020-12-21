/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter.tansformer;

import com.yahoo.cubed.json.filter.RelationalRule;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilterRelationalRule;
import com.yahoo.cubed.util.Constants;

/**
 * Filter tag null rule removal.
 */
public class FilterTagNullRelationalRuleDeletion implements RuleTransformer<BulletQueryFilterRelationalRule> {

    /**
     * Delete the clause: filter_tag is equal to NULL.
     */
    @Override
    public BulletQueryFilter transform(BulletQueryFilterRelationalRule rule) {
        if ("filter_tag".equals(rule.field)
                && RelationalRule.SYMBOLS.get("equal").equals(rule.operation)
                && rule.values != null
                && rule.values.size() > 0
                && Constants.NULL.equals(rule.values.get(0))) {
            return null;
        }
        return rule;
    }
}
