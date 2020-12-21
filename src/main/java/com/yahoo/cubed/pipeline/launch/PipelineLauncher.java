/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.launch;

import com.yahoo.cubed.pipeline.command.CommandExecutor;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * Launch pipeline.
 */
@Slf4j
public class PipelineLauncher extends CommandExecutor<PipelineLauncher.LaunchStatus> {
    private static final String OOZIE_JOB_ID_IDENTIFIER = "[[ID]]";
    private static final String OOZIE_JOB_ID_FOR_BACKFILL_IDENTIFIER = "[[BACKFILLID]]";

    /**
     * Stores launch status.
     */
    public static class LaunchStatus extends CommandExecutor.Status {
        /** Oozie job ID. */
        public String oozieJobId;
        /** Oozie backfill job ID. */
        public String oozieJobIdOfBackfill;
    }
    
    /**
     * Create a new pipeline launch status.
     */
    @Override
    protected PipelineLauncher.LaunchStatus newStatus() {
        return new PipelineLauncher.LaunchStatus();
    }

    @Getter @Setter
    private String pipelineName;
    @Getter @Setter
    private String pipelineVersion;
    @Getter @Setter
    private String pipelineOwner;
    @Getter @Setter
    private String pipelineResourcePath;
    @Getter @Setter
    private String pipelineBackfillStartDate;
    @Getter @Setter
    private String pipelineOozieJobType;
    @Getter @Setter
    private String pipelineOozieBackfillJobType;

    @Override
    protected void checkInfoHook(String line, PipelineLauncher.LaunchStatus status) {
        if (line.startsWith(OOZIE_JOB_ID_IDENTIFIER)) {
            status.oozieJobId = line.substring(OOZIE_JOB_ID_IDENTIFIER.length());
        } else if (line.startsWith(OOZIE_JOB_ID_FOR_BACKFILL_IDENTIFIER)) {
            status.oozieJobIdOfBackfill = line.substring(OOZIE_JOB_ID_FOR_BACKFILL_IDENTIFIER.length());
        }
    };
    
    @Override
    protected void setCommand(ProcessBuilder processBuilder) {
        processBuilder.command("sh", this.getScriptFileName(), this.getPipelineName(), this.getPipelineVersion(), this.getPipelineOwner(), this.getPipelineResourcePath(), this.pipelineBackfillStartDate, this.pipelineOozieJobType, this.pipelineOozieBackfillJobType);
    }
}
