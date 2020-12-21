/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model.filter;

import java.util.ArrayList;
import java.util.List;
import lombok.Getter;
import lombok.Setter;

/**
 * Stores list of filters.
 */
public class PipelineLogicalRule extends PipelineFilter {
    @Getter @Setter
    private String name;
    @Getter @Setter
    private String condition;
    @Getter @Setter
    private List<PipelineFilter> rules;
    
    /**
     * Add new filter rule.
     */
    public void addRule(PipelineFilter rule) {
        if (this.rules == null) {
            this.rules = new ArrayList<>();
        }
        this.rules.add(rule);
    }
    
    /**
     * Convert to string.
     */
    @Override
    public String toString() {
        if (this.getRules().size() == 1) {
            return this.getRules().get(0).toString();
        } else {
            String[] ruleStrArray = new String[this.getRules().size()];
            int i = 0;
            for (PipelineFilter filter : rules) {
                ruleStrArray[i] = filter.toString();
                ++i;
            }
            return "(" + String.join(" " + condition + " ", ruleStrArray) + ")";
        }
    }
    
    /**
     * Convert to pretty print string.
     */
    @Override
    protected String prettyPrint(int indentLevel) {
        String indent = this.generateIndent(indentLevel);
        if (this.getRules().size() == 1) {
            return indent + this.getRules().get(0).prettyPrint(indentLevel);
        } else {
            String[] ruleStrArray = new String[this.getRules().size()];
            int i = 0;
            for (PipelineFilter filter : rules) {
                ruleStrArray[i] = filter.prettyPrint(indentLevel + 1);
                ++i;
            }
            return indent + "(" + "\n" +
                   String.join(" " + condition + " \n", ruleStrArray) + "\n" +
                   indent + ")";
        }
    }
}
