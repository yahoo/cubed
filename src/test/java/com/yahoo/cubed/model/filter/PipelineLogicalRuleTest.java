/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model.filter;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;

/**
 * Test for PipelineLogicalRule.
 */
public class PipelineLogicalRuleTest {
    /**
     * toString test.
     * @throws Exception
     */
    @Test
    public void toStringTest() throws Exception {
        PipelineLogicalRule tg = new PipelineLogicalRule();
        tg.setRules(new ArrayList<>());
        Assert.assertEquals(tg.toString(), "()");
        PipelineLogicalRule subRule = new PipelineLogicalRule();
        subRule.setRules(new ArrayList<>());
        tg.addRule(subRule);
        Assert.assertEquals(tg.toString(), "()");
        PipelineLogicalRule subRule2 = new PipelineLogicalRule();
        subRule2.setRules(new ArrayList<>());
        tg.addRule(subRule2);
        Assert.assertEquals(tg.toString(), "(() null ())");
    }
}
