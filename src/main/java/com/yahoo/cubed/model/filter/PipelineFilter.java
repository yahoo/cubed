/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model.filter;

/**
 * Pipeline filter.
 */
public abstract class PipelineFilter {
    /** Indent character. */
    protected static final String SINGLE_INDENT_SPACE = "  ";
    
    /**
     * Pretty print the filter.
     */
    public String prettyPrint() {
        return this.prettyPrint(0);
    }
    
    /**
     * Pretty print with an indent level.
     */
    protected String prettyPrint(int indentLevel) {
        return this.generateIndent(indentLevel) + this.toString();
    }
    
    /**
     * Generate indent line.
     */
    protected String generateIndent(int indentLevel) {
        StringBuilder ret = new StringBuilder();
        for (int i = 0; i < indentLevel; ++i) {
            ret.append(SINGLE_INDENT_SPACE);
        }
        return ret.toString();
    }
}
