/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service.querybullet;

import com.yahoo.cubed.service.bullet.model.projection.BulletQueryProjection;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Test for BulletQueryProjection.
 */
public class BulletQueryProjectionTest {
    /**
     * addField Test.
     * @throws Exception
     */
    @Test
    public void addFieldTest() throws Exception {
        BulletQueryProjection tg = BulletQueryProjection.createBulletQueryProjectionInstance(null);
        tg.addField("test");
        Assert.assertEquals(tg.fields.get("test"), "");
    }
}
