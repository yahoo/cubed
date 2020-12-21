/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import org.testng.annotations.Test;

/**
 * Unit test ZipUtil.
 */
public class ZipUtilTest {
    /**
     * Zip a test folder.
     */
    @Test
    public void testZipSrcFolder() throws Exception {
        ZipUtil.zipDir("./target/src.zip", "./src");
    }
}
