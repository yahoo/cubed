/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter.tansformer;

/**
 * Factory for transform rules.
 */
public class RuleTransformerFactory {
    private static FilterTagNullRelationalRuleDeletion filterTagNullRelationalRuleDeletion = null;

    /**
     * Rule to remove filter_tag == null.
     */
    public static FilterTagNullRelationalRuleDeletion filterTagNullRelationalRuleDeletion() {
        if (filterTagNullRelationalRuleDeletion == null) {
            filterTagNullRelationalRuleDeletion = new FilterTagNullRelationalRuleDeletion();
        }
        return filterTagNullRelationalRuleDeletion;
    }
}
