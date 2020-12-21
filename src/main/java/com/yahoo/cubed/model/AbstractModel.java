/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

/**
 * Abstract model.
 */
public interface AbstractModel {
    
    /**
     * The function to get the unique name of an entity.
     * @return the name of an entity
     */
    public String getPrimaryName();
}
