/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.bullet.query;

/**
 * Bullet query failed exception.
 */
public class BulletQueryFailException extends Exception {
    private static final long serialVersionUID = 1L;

    /**
     * Create Bullet fail query exception.
     */
    public BulletQueryFailException(Exception e) {
        super(e);
    }
}
