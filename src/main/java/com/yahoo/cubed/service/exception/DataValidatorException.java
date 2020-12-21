/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.exception;

/**
 * Data validator exception.
 */
@SuppressWarnings("serial")
public class DataValidatorException extends Exception {
    /**
     * Constructor.
     */
    public DataValidatorException(String s) {
        super(s);
    }
}
