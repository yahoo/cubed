/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.model.filter.tansformer;

import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;

/**
 * Rule transformer interface.
 * @param <T> Bullet transformation rule.
 */
public interface RuleTransformer<T extends BulletQueryFilter> {
    /**
     * This is the function to apply the transformation rule to the bullet query filter.
     * @param rule transformation rule.
     * @return transformed bullet query filter.
     */
    public BulletQueryFilter transform(T rule);
}
