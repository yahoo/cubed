/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.settings;

import java.lang.reflect.Field;
import lombok.extern.slf4j.Slf4j;

/**
 * Helper methods to parse CLI args.
 */
@Slf4j
public class YamlSettings {

    /** Format of operational parameters yaml file. */
    public static final String OPERATIONAL_PARAMS_YAML_FILE_NAME_FORMAT = "operational";

    /** Format of schema yaml file. */
    public static final String SCHEMA_YAML_FILE_NAME_FORMAT = "schema";

    /** Oozie job type, daily or hourly. */
    public static String OOZIE_JOB_TYPE = "hourly"; // Just for unit test

    /** Oozie backfill job type, daily or hourly. */
    public static String OOZIE_BACKFILL_JOB_TYPE = "daily"; // Just for unit test

    /**
     * Print all the settings.
     */
    public void print() {
        // Get all fields in the class
        Field[] fields = this.getClass().getDeclaredFields();
        // Print the settings
        log.info("Printing all yaml settings...");
        for (Field field : fields) {
            try {
                // Print all columns (skip "log" and "HELP")
                if (!field.getName().equals("log") && !field.getName().equals("HELP")) {
                    log.info("{}: {}", field.getName(), field.get(this));
                }
            } catch (Exception e) {
                log.error("Error getting content of {}", field.getName());
            }
        }
        log.info("Done printing all settings...");
    }
}
