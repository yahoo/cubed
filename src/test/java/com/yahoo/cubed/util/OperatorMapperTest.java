/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import org.testng.Assert;
import org.testng.annotations.Test;
import com.yahoo.cubed.model.Field;

/**
 * Test operator mapper.
 */
public class OperatorMapperTest {
    /**
     * Simple string formatting test.
     */
    @Test
    public void testStringFormatting() {
        Field field = new Field();
        field.setFieldType(Constants.BOOLEAN);
        Assert.assertEquals(OperatorMapper.validateString(Constants.NULL, field), Constants.NULL);
    }
}
