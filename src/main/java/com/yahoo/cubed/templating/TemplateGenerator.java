/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import java.util.List;
import java.io.File;

/**
 * Template generator abstract class.
 * @param <T> Type.
 */
public abstract class TemplateGenerator<T> {
    /**
     * Generates a template file given a model and version.
     * @param model
     * @param outputPath
     * @param version
     */
    public abstract String generateTemplateFiles(T model, String outputPath, long version) throws Exception;

    /**
     * Create directory structure according to folder specifications.
     * @param folders
     * @throws Exception
     */
    protected void createDirectoryStructure(List<String> folders) throws Exception {
        for (String folder : folders) {
            if (!new File(folder).mkdir()) {
                throw new Exception("Unable to create directory " + folder);
            }
        }
    }
}
