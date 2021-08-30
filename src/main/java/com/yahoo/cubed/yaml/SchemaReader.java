/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Set;
import java.util.HashSet;

/**
 * Utility to read a YAML schema file.
 */
@Slf4j
public class SchemaReader {
    /**
     * Read a schema YAML file.
     * @param filePath The path to the schema file.
     */
    public static Schemas readSchema(String filePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        Schemas schemas = mapper.readValue(new File(filePath), Schemas.class);
        // Validate the schema
        if (!SchemaReader.validate(schemas)) {
            log.error("Invalid schema!");
            throw new Exception("Invalid schema error.");
        }
        return schemas;
    }

    private static boolean validateFields(List<YamlField> fields) {
        Set<String> fieldNames = new HashSet<>();
        Set<Integer> fieldIds = new HashSet<>();
        Set<Integer> keyIds = new HashSet<>();

        for (YamlField field : fields) {
            // Check if there is a duplicate name
            if (fieldNames.contains(field.getName())) {
                log.error("Duplicate field name: {}", field.getName());
                return false;
            }
            fieldNames.add(field.getName());
            // Check if there is a duplicate field id
            if (field.getId() != null && fieldIds.contains(field.getId())) {
                log.error("Duplicate field id: {}", field.getId());
                return false;
            }
            fieldIds.add(field.getId());
            if (field.getKeys() != null) {
                // Only check keys names inside the field
                // as there can be duplicate key names
                Set<String> keyNames = new HashSet<>();
                for (YamlKey key : field.getKeys()) {
                    // Check if there is a duplicate name
                    if (keyNames.contains(key.getName())) {
                        log.error("Duplicate key name: {}", key.getName());
                        return false;
                    }
                    // Add the name to the set
                    keyNames.add(key.getName());
                    // Check if there is a duplicate field id
                    if (key.getId() != null && keyIds.contains(key.getId())) {
                        log.error("Duplicate key id: {}", key.getId());
                        return false;
                    }
                    // Add the id to the set
                    keyIds.add(key.getId());
                }
            }
        }
        return true;
    }

    private static boolean validateSchemas(List<YamlSchema> schemas) {
        // Check schema names, field names, and field ids
        Set<String> schemaNames = new HashSet<>();
        Set<String> tableNames = new HashSet<>();

        for (YamlSchema schemaDefintion : schemas) {
            // Check if there is a duplicate name
            if (schemaNames.contains(schemaDefintion.getName())) {
                log.error("Duplicate schema name: {}", schemaDefintion.getName());
                return false;
            }
            // Add the name to the set
            schemaNames.add(schemaDefintion.getName());
            if (!validateFields(schemaDefintion.getFields())) {
                log.error("Invalid fields in schema {}", schemaDefintion.getName());
                return false;
            }
            // Validate tables
            if (schemaDefintion.getTables().size() == 0) {
                log.error("Schema does not contain any tables {}", schemaDefintion.getName());
                return false;
            }
            // Must provide database name
            if (schemaDefintion.getDatabase() == null) {
                log.error("No database name provided.");
                return false;
            }
            // Must provide a datetime partition column
            if (schemaDefintion.getDatetimePartitionColumn() == null) {
                log.error("No datetime partition column provided.");
                return false;
            }
        }
        return true;
    }

    /**
     * Performs validation on a schema.
     * Fields can't have the same name or ID.
     * @param schemas Schema object
     * @return True if valid schema, false otherwise.
     */
    private static boolean validate(Schemas schemas) {
        return validateSchemas(schemas.getSchemas());
    }
}
