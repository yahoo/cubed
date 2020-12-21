/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import org.apache.commons.io.IOUtils;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import com.yahoo.cubed.settings.CLISettings;

/**
 * Template utilities.
 */
public class TemplateUtils {
    /** Druid template directory. */
    public static final String DRUID_TEMPLATES_DIR = "templates/druid/";
    /** Oozie template directory. */
    public static final String OOZIE_TEMPLATES_DIR = "templates/pipeline/parametrized/";
    /** Product name. */
    public static final String PRODUCT_NAME_TEMPLATE = CLISettings.INSTANCE_NAME + "_%s";
    /** Location of static resources. */
    public static final String STATIC_RESOURCE_DIR = "templates/pipeline/static/";
    /** Funnel template directory. */
    public static final String FUNNEL_TEMPLATE_DIR = "templates/parametrized/";

    /**
     * Get template as string.
     */
    public static String getParametrizedTemplateAsString(String name) throws Exception {
        try (InputStream in = getResouceFileAsStream(name)) {
            return IOUtils.toString(in, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            throw new Exception("Unable to read template file " + name);
        }
    }

    /**
     * Get project name.
     */
    public static String getProductName(String name) {
        return String.format(PRODUCT_NAME_TEMPLATE, name);
    }

    /**
     * Get resource file as string.
     */
    public static InputStream getResouceFileAsStream(String name) {
        return TemplateUtils.class.getClassLoader().getResourceAsStream(name);
    }
}
