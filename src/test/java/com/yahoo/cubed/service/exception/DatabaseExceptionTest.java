/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.exception;

import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test DatabaseException.
 */
public class DatabaseExceptionTest {
    /**
     * Throw DatabaseException Test.
     * @throws Exception
     */
    @Test
    public void throwDatabaseExceptionTest() throws Exception {
        try {
            throw new DatabaseException("test error", new Exception());
        } catch (DatabaseException e) {
            Assert.assertEquals(e.getMessage(), "test error");
        } catch (Exception e) {
            Assert.fail("Exception: " + e.getMessage());
        }
    }
}
