/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.stop;

import com.yahoo.cubed.pipeline.command.CommandExecutor;
import lombok.Getter;
import lombok.Setter;

/**
 * Pipeline stopper.
 */
public class PipelineStopper extends CommandExecutor<CommandExecutor.Status> {
    @Getter @Setter
    private String pipelineName;
    @Getter @Setter
    private String pipelineOwner;

    /**
     * Set command.
     */
    @Override
    protected void setCommand(ProcessBuilder processBuilder) {
        processBuilder.command("sh", this.getScriptFileName(), this.getPipelineName(), this.getPipelineOwner());
    }
}
