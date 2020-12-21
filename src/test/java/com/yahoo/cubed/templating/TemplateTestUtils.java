/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Utils for template testing.
 */
public class TemplateTestUtils {

    /**
     * Load a template.
     */
    public static String loadTemplateInstance(String templateInstanceName) throws Exception {
        try (InputStream in = TemplateTestUtils.class.getClassLoader().getResourceAsStream(templateInstanceName)) {
            return IOUtils.toString(in, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new Exception("Unable to read template file " + templateInstanceName);
        }
    }
}
