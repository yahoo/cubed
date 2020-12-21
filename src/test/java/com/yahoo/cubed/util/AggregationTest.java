/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

/**
 * Aggregation Test.
 */
public class AggregationTest {
    /**
     * byName Test.
     * @throws Exception
     */
    @Test
    public void byNameTest() throws Exception {
        try {
            List<String> result = Aggregation.allNames();

            Assert.assertEquals(result.size(), 6);

        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }
}
