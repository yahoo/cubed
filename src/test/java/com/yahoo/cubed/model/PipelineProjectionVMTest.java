/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for PipelineProjectionVM.
 */
public class PipelineProjectionVMTest {
    /**
     * PipelineProjectionVM test.
     * @throws Exception
     */
    @Test
    public void pipelineProjectionVMTest() throws Exception {
        PipelineProjectionVM tg = new PipelineProjectionVM();
        Assert.assertEquals(tg.getPrimaryIdx(), 0);
        Assert.assertEquals(tg.getPrimaryName(), null);
    }
}
