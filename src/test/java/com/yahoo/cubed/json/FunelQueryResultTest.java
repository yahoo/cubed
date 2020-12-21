/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test funnel query result class.
 */
public class FunelQueryResultTest {
    FunnelQueryResult tg;

    /**
     * Test FunnelQueryResult constructor.
     */
    @Test
    public void testFunnelQueryResultConstructor() throws Exception {
        tg = new FunnelQueryResult();
        Assert.assertEquals(tg.getKeys().size(), 0);
        Assert.assertEquals(tg.getValues().size(), 0);
    }
}
