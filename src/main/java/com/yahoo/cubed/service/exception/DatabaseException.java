/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.exception;

/**
 * Database exception.
 */
@SuppressWarnings("serial")
public class DatabaseException extends Exception {
    /**
     * Constructor.
     */
    public DatabaseException(String s, Throwable e) {
        super(s, e);
    }
}
