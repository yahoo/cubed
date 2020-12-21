/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.yaml;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import lombok.extern.slf4j.Slf4j;

import java.util.Set;
import java.util.HashSet;
import java.util.Map;
import java.util.List;

/**
 * Utility to read operational parameters file.
 */
@Slf4j
public class OperationalParamsReader {
    /**
     * Read a operational parameters YAML file.
     * @param operationalParamsFilePath The path to the operational parameters file.
     * @param schemaFilePath The path to the schemas file.
     */
    public static OperationalParams readOperationalParams(String operationalParamsFilePath, String schemaFilePath) throws Exception {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        OperationalParams operationalParams = mapper.readValue(new File(operationalParamsFilePath), OperationalParams.class);

        // Validate the operational parameters
        if (!OperationalParamsReader.validate(operationalParams, schemaFilePath)) {
            log.error("Invalid operational parameters in operational_params.yaml file!");
            throw new Exception("Invalid operational parameters error.");
        }
        return operationalParams;
    }

    /**
     * Performs validation on a schema.
     * Target database name should match the database name in yaml schema file.
     * defaultFilter ids should have no duplicates and be defined in schama file.
     * If disableFunnel == false (cubed is supported):
     *      - Target table should be one of the available tables in yaml schema file.
     *      - There should be at least one and no duplicate userIdFields.
     *      - All userIdFields should be defined in yaml schema file.
     *      - Timestamp column cannot be null and should exist in schema.
     * @param operationalParams OperationalParams object.
     * @param schemaFilePath The path to the schemas file.
     * @return True if valid schema, false otherwise.
     */
    private static boolean validate(OperationalParams operationalParams, String schemaFilePath) throws Exception {

        // Read in schema file and prepare for validation
        Set<String> schemaNames = new HashSet<>();
        Set<String> expandedFields = new HashSet<>(); // For map type, expanded field format is FIELD['KEY']
        Set<String> mapTypeFields = new HashSet<>();
        Set<String> tables = new HashSet<>();
        Set<String> userIdFields = new HashSet<>();
        Set<String> defaultFilterIds = new HashSet<>();

        Schemas schemas = SchemaReader.readSchema(schemaFilePath);
        
        for (YamlSchema schema : schemas.getSchemas()) {
            // Prepare schemaNames set
            schemaNames.add(schema.getName());

            // Prepare tables set
            for (Map.Entry<String, String> entry: schema.getTables().entrySet()) {
                tables.add(entry.getValue());
            }

            // Prepare expanded fields set
            for (YamlField yamlField : schema.getFields()) {
                String fieldName = yamlField.getName();
                if (yamlField.getKeys() == null) {
                    expandedFields.add(fieldName);
                } else {
                    mapTypeFields.add(fieldName);
                    for (YamlKey yamlKey : yamlField.getKeys()) {
                        String keyName = yamlKey.getName();
                        expandedFields.add(fieldName + "['" + keyName + "']");
                    }
                }
            }
        }

        // Verify there's no duplicate items in defaultFilters and all of thems exist in schema
        List<Map<String, String>> defaultFilters = operationalParams.getDefaultFilters();
        if (defaultFilters != null) { // default filters are optional
            for (Map<String, String> filter : defaultFilters) {
                String filterId = filter.get("id");
                if (filterId == null) {
                    log.error("There exists default filter without field id.");
                    return false;
                }
                if (defaultFilterIds.contains(filterId)) {
                    log.error("Duplicate default filter field id: {}.", filterId);
                    return false;
                }
                if (!expandedFields.contains(filterId) && !mapTypeFields.contains(filterId)) {
                    log.error("Filter field id {} not in schema definition.", filterId);
                    return false;
                }
                defaultFilterIds.add(filterId);
            }
        }

        // Validations under disableFunnel == false
        if (!operationalParams.isDisableFunnel()) {
            // Verify the target table parameter exists
            if (operationalParams.getFunnelTargetTable() == null) {
                log.error("Target table is not defined while funnel is enabled.");
                return false;
            }
            // Verify target table be one of the available tables in yaml schema file
            if (!tables.contains(operationalParams.getFunnelTargetTable())) {
                log.error("Target table not one of the available tables in yaml schema file.");
                return false;
            }

            // Verify there's no duplicate userIdFields and all of them exist in schema
            for (String field : operationalParams.getUserIdFields()) {
                if (userIdFields.contains(field)) {
                    log.error("Duplicate user id field: {}.", field);
                    return false;
                }

                if (!expandedFields.contains(field)) {
                    log.error("User id field {} not in schema definition.", field);
                    return false;
                }
                userIdFields.add(field);
            }

            // Timestamp column should be specified
            if (operationalParams.getTimestampColumnParam() == null) {
                log.error("Operation parameters file miss timestamp column.");
                return false;
            }

            // Timestamp column should exist in schema
            if (!expandedFields.contains(operationalParams.getTimestampColumnParam())) {
                log.error("Timestamp column parameter {} not in schema definition.", operationalParams.getTimestampColumnParam());
                return false;
            }
        }

        return true;
    }
}
