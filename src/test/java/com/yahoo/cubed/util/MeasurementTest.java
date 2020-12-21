/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test enum Measurement.
 */
public class MeasurementTest {
    /**
     * byName test.
     * @throws Exception
     */
    @Test
    public void byNameTest() throws Exception {
        try {
            Measurement result = Measurement.byName("MET");

            Assert.assertNotNull(result);
            Assert.assertEquals(result.measurementTypeCode, "MET");

            result = Measurement.byName("invalid");
            Assert.assertNull(result);
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }
}
