/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.stop;

import org.testng.Assert;
import org.testng.annotations.Test;


/**
 * Test pipeline stop script.
 */
public class StopperTest {
    
    /**
     * Stop pipeline script.
     */
    @Test
    public void testStopper() throws Exception {
        String scriptDir = StopperTest.class.getClassLoader().getResource("bin").getPath();
        PipelineStopper stopper = new PipelineStopper();
        stopper.setPipelineName("pipeline");
        stopper.setPipelineOwner("john_doe");
        stopper.setScriptFileDir(scriptDir);
        stopper.setScriptFileName("test-success-stop.sh");
        
        PipelineStopper.Status status1 = stopper.call();
        System.out.print(status1.errorMsg);
        Assert.assertFalse(status1.hasError);
        Assert.assertEquals(status1.infoMsg, "[DMART PIPELINE CD][INFO] OK\n");
        
        stopper.setScriptFileName("test-failure.sh");
        PipelineStopper.Status status2 = stopper.call();
        Assert.assertTrue(status2.hasError);
        Assert.assertEquals(status2.errorMsg, "[DMART PIPELINE CD][ERROR] failure\n");
        
        stopper.setScriptFileName("test-all.sh");
        PipelineStopper.Status status3 = stopper.call();
        Assert.assertTrue(status3.hasError);
        Assert.assertEquals(status3.errorMsg, "[DMART PIPELINE CD][ERROR] ERROR\n");
        Assert.assertEquals(status3.infoMsg, "[DMART PIPELINE CD][INFO] OK\n");
    }
    
}
