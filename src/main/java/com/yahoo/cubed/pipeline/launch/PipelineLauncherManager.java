/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.launch;

import lombok.extern.slf4j.Slf4j;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ExecutorService;
import com.yahoo.cubed.settings.CLISettings;

/**
 * Manager for launching pipelines.
 */
@Slf4j
public class PipelineLauncherManager {
    private static final String SCRIPT_NAME = "startPipeline.sh";
    private static final String FUNNEL_SCRIPT_NAME = "startFunnelPipeline.sh";
    private static final String SCRIPT_PATH = CLISettings.PIPELINE_SCRIPTS_PATH;
    private ExecutorService executorService;

    /**
     * Start thread for pipeline launcher.
     */
    public PipelineLauncherManager() {
        // create a thread to run the script
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Create pipeline launcher.
     */
    public static PipelineLauncher createLauncher(String pipelineName, String pipelineVersion, String pipelineOwner, String pipelineResourcePath, String pipelineBackfillStartDate, boolean isFunnel, String pipelineOozieJobType, String pipelineOozieBackfillJobType) {
        PipelineLauncher launcher = new PipelineLauncher();
        launcher.setScriptFileDir(SCRIPT_PATH);
        if (isFunnel) {
            launcher.setScriptFileName(FUNNEL_SCRIPT_NAME);
        } else {
            launcher.setScriptFileName(SCRIPT_NAME);
        }
        launcher.setPipelineName(pipelineName);
        launcher.setPipelineVersion(pipelineVersion);
        launcher.setPipelineOwner(pipelineOwner);
        launcher.setPipelineResourcePath(pipelineResourcePath);
        launcher.setPipelineBackfillStartDate(pipelineBackfillStartDate);
        launcher.setPipelineOozieJobType(pipelineOozieJobType);
        launcher.setPipelineOozieBackfillJobType(pipelineOozieBackfillJobType);
        return launcher;
    }

    /**
     * Launch pipeline.
     */
    public Future<PipelineLauncher.LaunchStatus> launchPipeline(String pipelineName, String pipelineVersion, String pipelineOwner, String pipelineResourcePath, String pipelineBackfillStartDate, boolean isFunnel, String pipelineOozieJobType, String pipelineOozieBackfillJobType) {
        Future<PipelineLauncher.LaunchStatus> returnValue = this.executorService.submit(createLauncher(pipelineName, pipelineVersion, pipelineOwner, pipelineResourcePath, pipelineBackfillStartDate, isFunnel, pipelineOozieJobType, pipelineOozieBackfillJobType));
        return returnValue;
    }
}
