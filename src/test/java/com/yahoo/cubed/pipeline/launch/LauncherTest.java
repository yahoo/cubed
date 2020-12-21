/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.launch;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.settings.YamlSettings;

/**
 * Test pipeline launch script.
 */
public class LauncherTest {

    /**
     * Test that the launcher script is correctly populate from pipeline settings.
     */ 
    @Test
    public void testLauncher() throws Exception {
        String scriptDir = LauncherTest.class.getClassLoader().getResource("bin").getPath();
        PipelineLauncher launcher = new PipelineLauncher();
        launcher.setPipelineName("pipeline");
        launcher.setPipelineVersion("v1.0");
        launcher.setPipelineOwner(CLISettings.PIPELINE_OWNER);
        launcher.setPipelineResourcePath("/tmp");
        launcher.setScriptFileDir(scriptDir);
        launcher.setScriptFileName("test-success-launch.sh");
        launcher.setPipelineBackfillStartDate("0");
        launcher.setPipelineOozieJobType(YamlSettings.OOZIE_JOB_TYPE);
        launcher.setPipelineOozieBackfillJobType(YamlSettings.OOZIE_BACKFILL_JOB_TYPE);
        
        PipelineLauncher.LaunchStatus status1 = launcher.call();
        System.out.print(status1.errorMsg);
        Assert.assertFalse(status1.hasError);
        Assert.assertEquals(status1.infoMsg, "[DMART PIPELINE CD][INFO] OK\n");
        
        launcher.setScriptFileName("test-failure.sh");
        PipelineLauncher.LaunchStatus status2 = launcher.call();
        Assert.assertTrue(status2.hasError);
        Assert.assertEquals(status2.errorMsg, "[DMART PIPELINE CD][ERROR] failure\n");
        
        launcher.setScriptFileName("test-all.sh");
        PipelineLauncher.LaunchStatus status3 = launcher.call();
        Assert.assertTrue(status3.hasError);
        Assert.assertEquals(status3.errorMsg, "[DMART PIPELINE CD][ERROR] ERROR\n");
        Assert.assertEquals(status3.infoMsg, "[DMART PIPELINE CD][INFO] OK\n");
    }
    
}
