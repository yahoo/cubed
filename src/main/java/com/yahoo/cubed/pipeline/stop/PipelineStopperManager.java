/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.stop;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import com.yahoo.cubed.settings.CLISettings;

/**
 * Manage pipeline stop.
 */
public class PipelineStopperManager {
    private static final String SCRIPT_NAME = "stopPipeline.sh";
    private static final String SCRIPT_PATH = CLISettings.PIPELINE_SCRIPTS_PATH;
    private ExecutorService executorService;

    /**
     * Create thread to stop pipeline.
     */
    public PipelineStopperManager() {
        // create a thread to run the script
        this.executorService = Executors.newSingleThreadExecutor();
    }

    /**
     * Create pipeline stopper.
     */
    public static PipelineStopper createStopper(String pipelineName, String pipelineOwner) {
        PipelineStopper stopper = new PipelineStopper();
        stopper.setScriptFileDir(SCRIPT_PATH);
        stopper.setScriptFileName(SCRIPT_NAME);
        stopper.setPipelineName(pipelineName);
        stopper.setPipelineOwner(pipelineOwner);
        return stopper;
    }

    /**
     * Stop a pipeline.
     */
    public Future<PipelineStopper.Status> stopPipeline(String pipelineName, String pipelineOwner) throws java.lang.InterruptedException {
        return this.executorService.submit(createStopper(pipelineName, pipelineOwner));
    }
}
