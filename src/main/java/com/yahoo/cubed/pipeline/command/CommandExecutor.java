/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.pipeline.command;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import lombok.Getter;
import lombok.Setter;

/**
 * Generic class to run a script.
 * @param <T> Status.
 */
public abstract class CommandExecutor<T extends CommandExecutor.Status> implements Callable<T> {
    private static final int PROCESS_RUN_MAX_TIME = 90;
    @Getter @Setter
    private String scriptFileDir;
    @Getter @Setter
    private String scriptFileName;
    
    /**
     * Script status.
     */
    public static class Status {
        /** Error status. */
        public boolean hasError;
        /** Error message. */
        public String errorMsg;
        /** Info message. */
        public String infoMsg;
    }
    
    /**
     * Create new status.
     */
    @SuppressWarnings("unchecked")
    protected T newStatus() {
        return (T) new CommandExecutor.Status();
    }

    /**
     * Hook to check info.
     * Children will override.
     */
    protected void checkInfoHook(String line, T status) {
        return;
    };
    
    private void checkInfo(InputStream input, T status) throws IOException {
        BufferedReader reader = null;
        StringBuilder infoMsg = new StringBuilder();
        try {
            reader = new BufferedReader(new InputStreamReader(input));
            String line = null;
            while ((line = reader.readLine()) != null) {
                infoMsg.append(line);
                infoMsg.append("\n");
                // info message processing hook
                this.checkInfoHook(line, status);
            }
        } catch (Exception e) {
            infoMsg.append(e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        status.infoMsg = infoMsg.toString();
    }
    
    /**
     * Error hook.
     * Children will override.
     */
    protected void checkErrorHook(String line, T status) {
        return;
    };

    private void checkError(InputStream input, T status) throws IOException {
        BufferedReader reader = null;
        boolean hasError = false;
        StringBuilder errorMsg = new StringBuilder();
        try {
            reader = new BufferedReader(new InputStreamReader(input));
            String line = null;
            while ((line = reader.readLine()) != null) {
                hasError = true;
                errorMsg.append(line);
                errorMsg.append("\n");
                // error message processing hook
                this.checkErrorHook(line, status);
            }
        } catch (Exception e) {
            hasError = true;
            errorMsg.append(e);
        } finally {
            if (reader != null) {
                reader.close();
            }
        }
        status.hasError = hasError;
        status.errorMsg = errorMsg.toString();
    }
    
    /**
     * Set command.
     */
    protected void setCommand(ProcessBuilder processBuilder) {
        processBuilder.command("sh", this.getScriptFileName());
    }

    /**
     * Entry of the executor.
     */
    @Override
    public T call() throws Exception {
        T ret = this.newStatus();
        try {
            ProcessBuilder processBuilder = new ProcessBuilder();
            processBuilder.directory(new File(this.getScriptFileDir()));
            this.setCommand(processBuilder);
            Process process = processBuilder.start();
            this.checkError(process.getErrorStream(), ret);
            this.checkInfo(process.getInputStream(), ret);
            boolean isNormal = process.waitFor(PROCESS_RUN_MAX_TIME, TimeUnit.SECONDS);
            if (!isNormal) {
                process.destroy();
                ret.hasError = true;
                ret.errorMsg = "The process that ran the script was terminated forcibly.";
                return ret;
            }
            if (ret.hasError) {
                return ret;
            }
        } catch (Exception e) {
            ret.hasError = true;
            ret.errorMsg = e.getMessage();
            return ret;
        }
        ret.hasError = false;
        return ret;
    }
}
