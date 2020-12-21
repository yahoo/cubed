/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed;

import com.beust.jcommander.JCommander;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.settings.YamlSettings;
import com.yahoo.cubed.source.ConfigurationLoader;
import com.yahoo.cubed.source.DatabaseConnectionManager;
import com.yahoo.cubed.yaml.YamlField;
import com.yahoo.cubed.yaml.YamlKey;
import com.yahoo.cubed.yaml.YamlSchema;
import com.yahoo.cubed.yaml.SchemaReader;
import com.yahoo.cubed.yaml.Schemas;
import com.yahoo.cubed.yaml.OperationalParamsReader;
import com.yahoo.cubed.yaml.OperationalParams;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.HashSet;

import liquibase.Contexts;
import liquibase.LabelExpression;
import liquibase.Liquibase;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.exception.LiquibaseException;
import liquibase.resource.ClassLoaderResourceAccessor;
import lombok.extern.slf4j.Slf4j;

import spark.TemplateEngine;
import spark.template.thymeleaf.ThymeleafTemplateEngine;
import static spark.Spark.delete;
import static spark.Spark.get;
import static spark.Spark.port;
import static spark.Spark.post;
import static spark.Spark.put;
import static spark.Spark.redirect;
import static spark.Spark.staticFiles;
import static spark.Spark.threadPool;

/**
 * Application entry point.
 */
@Slf4j
public class App {
    /** Name of Liquibase changelog file. */
    private static final String LIQUIBASE_FILE = "database-changelog-master.xml";

    /**
     * Launch the web service.
     */
    public static void main(String[] args) {
        try {
            // Parse CLI args and store
            CLISettings settings = new CLISettings();
            JCommander jCommander = new JCommander(settings, args);
            // Print the settings
            settings.print();
            // Check if we want to print help
            jCommander.setProgramName("Cubed");
            if (CLISettings.HELP) {
                jCommander.usage();
                return;
            }
            // Launch the server
            App app = new App();
            app.run();
        } catch (Exception e) {
            log.error("Error in main: {}", e.getMessage());
        }
    }

    /**
     * Run the server.
     */
    public void run() {
        // Launch the web server
        try {
            prepareDatabase();
            loadSchemas(CLISettings.SCHEMA_FILES_DIR);
            startWebServer();
        } catch (Exception e) {
            log.error("Error: {}", e);
        }
    }

    /**
     * Prepare the database.
     */
    public static void prepareDatabase() throws Exception {
        // (1) load configuration
        log.info("Loading configuration");
        loadConfiguration();
        // (2) run liquibase, sync database if it's dev
        log.info("Running Liquibase...");
        runLiquibase();
    }

    /**
     * Prepare the web service by declaring routes, load static files, and log settings.
     */
    private void prepareWebServer() throws Exception {
        // Set the version string
        log.info("Version: {}", CLISettings.VERSION);

        // Set the version string
        log.info("Template output folder: {}", CLISettings.TEMPLATE_OUTPUT_FOLDER);

        // Initalize the route variables
        Routes.init();

        // Set static file location (css, js, etc)
        staticFiles.location("/static");

        // Create the new template engine
        TemplateEngine templateEngine = new ThymeleafTemplateEngine();

        // Datamart list
        get("/cubed", Routes::listDatamart, templateEngine);

        // Deleted datamart list
        get("/deleted", Routes::listDeletedDatamart, templateEngine);

        // Create new datamart
        get("/datamart/new", Routes::newDatamart, templateEngine);

        // Create new funnel group query
        get("/funnelgroup/new", Routes::newFunnelGroupGet, templateEngine);
        post("/funnelgroup/new", Routes::newFunnelGroupPost, templateEngine);

        // Preview funnel query
        post("/funnel/preview", Routes::previewFunnelQuery);

        // Run funnel query
        post("/funnel/run", Routes::runFunnelQuery);

        // Create new funnel group
        post("/funnelgroup/new/save", Routes::createNewFunnelGroup);

        // List all active Oozie bundle IDs
        get("/datamart/all-bundle-ids", Routes::allBundleIds, templateEngine);

        // Clone a datamart
        get("/datamart/clone/:id", Routes::cloneDatamart, templateEngine);

        // Create new pipeline, post with input
        post("/datamart/new", Routes::createNewDatamart);

        // View specific datamart
        get("/datamart/:id", Routes::viewDatamart, templateEngine);

        // View specific funnel group
        get("/funnelgroup/:id", Routes::viewFunnelGroup, templateEngine);

        // Clone a funnel group
        get("/funnelgroup/clone/:id", Routes::cloneFunnelGroup, templateEngine);

        // Submit pipeline to bullet and return results
        put("/datamart/new/bullet", Routes::queryBullet);
        put("/datamart/:id/bullet", Routes::queryBullet);
        put("/datamart/clone/:id/bullet", Routes::queryBullet);

        // Delete a specific data mart
        delete("/datamart/:id", Routes::deleteDatamart);

        // Delete a specific funnel group
        delete("/funnelgroup/:id", Routes::deleteFunnelGroup);

        // Launch specific datamart
        post("/datamart/:id/launch", Routes::launchDatamart);

        // Stop specific datamart
        put("/datamart/:id/stop", Routes::stopPipeline);

        // Stop specific funnel group
        put("/funnelgroup/:id/stop", Routes::stopFunnelGroup);

        // Update datamart filters
        put("/datamart/:id/update", Routes::updateDatamart);

        // Update funnel group
        put("/funnelgroup/:id/update", Routes::updateFunnelGroup);

        // Launch specific funnel group
        post("funnelgroup/:id/launch", Routes::launchFunnelGroup);

        // Restore datamart
        put("/datamart/:id/restore", Routes::restoreDatamart);

        // Restore funnel group
        put("/funnelgroup/:id/restore", Routes::restoreFunnelGroup);

        // Reset datamart
        put("/datamart/:id/reset", Routes::resetDatamart);

        // Download specific datamart
        get("/datamart/:id/download", Routes::downloadDatamart);

        // Download specific funnel group
        get("/funnelgroup/:id/download", Routes::downloadFunnelGroup);

        // Preview datamart ETL script
        post("/datamart/preview", Routes::previewDatamartEtl);

        // View a specific datamart's ETL
        get("/datamart/:id/view-etl", Routes::viewDatamartEtl);

        // Relaunch all active datamarts
        post("/datamart/all-bundle-ids/relaunch", Routes::relaunchDatamart);

        // Health check
        get("/status", Routes::getStatus);

        // Redirect health check
        redirect.get("/status.html", "/status");

        // Default redirect
        redirect.get("/", "/cubed");

        // Routes configured
        log.info("Routes ready");
    }

    /**
     * Configure the routing for all pages and start the web server.
     */
    private void startWebServer() throws Exception {
        // Set the port
        log.info("Running on port: {}", CLISettings.PORT);
        port(CLISettings.PORT);

        // Set the thread count
        log.info("Running with threads: {}", CLISettings.THREADS);
        threadPool(CLISettings.THREADS);

        // Declaring routes, load static files, and log settings
        prepareWebServer();
        log.info("Site online: http://localhost:{}", CLISettings.PORT);
    }

    private static void loadConfiguration() throws IOException {
        ConfigurationLoader.load();
    }

    private static void runLiquibase() throws ClassNotFoundException, SQLException, LiquibaseException {
        Connection connection = null;
        try {
            connection = DatabaseConnectionManager.createConnection();
            Database database = DatabaseFactory.getInstance().findCorrectDatabaseImplementation(new JdbcConnection(connection));
            Liquibase liquibase = new liquibase.Liquibase(LIQUIBASE_FILE, new ClassLoaderResourceAccessor(), database);
            liquibase.update(new Contexts(), new LabelExpression());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }

    /**
     * Drop all fields. Used before adding in new schema.
     */
    public static void dropAllFields() throws Exception {
        // Get all current fields
        List<Field> fields = ServiceFactory.fieldService().fetchAll();
        // Delete each existing field
        // This also deletes the field keys

        for (Field field : fields) {
            ServiceFactory.fieldService().delete(field.getSchemaName(), field.getFieldId());
        }
    }

    /**
     * Load all schemas and add new schema.
     */
    public static void loadSchemas(String schemasDirPath) throws Exception {
        File dir = new File(schemasDirPath);
        if (!dir.isDirectory()) {
            throw new IllegalArgumentException("The " + schemasDirPath + " is not directory!");
        }
        Set<String> schemaNamesInConfig = new HashSet<>();
        File[] schemasDir = dir.listFiles();
        for (File schemaDir : schemasDir) {
            log.info("Load schema with path: {}", schemaDir.getAbsolutePath());
            if (schemaDir.isDirectory()) {
                File[] files = schemaDir.listFiles();
                String schemaFilePath = null, operationalParamsFilePath = null;
                for (File file : files) {
                    String fileName = file.getName();
                    if (fileName.indexOf(YamlSettings.OPERATIONAL_PARAMS_YAML_FILE_NAME_FORMAT) != -1) {
                        operationalParamsFilePath = file.getAbsolutePath();
                    } else if (fileName.indexOf(YamlSettings.SCHEMA_YAML_FILE_NAME_FORMAT) != -1) {
                        schemaFilePath = file.getAbsolutePath();
                    }
                }
                if (schemaFilePath == null || operationalParamsFilePath == null) {
                    throw new IllegalArgumentException("The directory " + schemaDir.getAbsolutePath() + " miss yaml files for schema");
                } else {
                    schemaNamesInConfig.addAll(addSchema(schemaFilePath, operationalParamsFilePath));
                }
            }
        }
        // Mark schemas that exist in database but not show up in configs as deleted
        List<String> schemaNamesInDb = ServiceFactory.schemaService().fetchAllName(false);
        for (String schemaName : schemaNamesInDb) {
            if (!schemaNamesInConfig.contains(schemaName)) {
                Schema schema = ServiceFactory.schemaService().fetchByName(schemaName);
                schema.setIsSchemaDeleted(true);
                ServiceFactory.schemaService().update(schema);
            }
        }

    }


    /**
     * Add schema from a YAML file.
     * This runs on every launch of Cubed.
     */
    private static Set<String> addSchema(String schemaFilePath, String operationalParamsFilePath) throws Exception {
        // The schema name list to return
        Set<String> schemaNames = new HashSet<>();
        // Load the schema file
        Schemas schemas = SchemaReader.readSchema(schemaFilePath);
        OperationalParams operationalParams = OperationalParamsReader.readOperationalParams(operationalParamsFilePath, schemaFilePath);
        ObjectMapper mapper = new ObjectMapper();

        // Add the schemas to the database
        for (YamlSchema yamlSchema : schemas.getSchemas()) {
            Schema schema = null;
            try {
                schema = ServiceFactory.schemaService().fetchByName(yamlSchema.getName());
            } catch (DataValidatorException e) {
                log.info("Adding schema: {}", yamlSchema.getName());
            }

            boolean isUpdate = schema != null;

            schema = new Schema();
            schema.setSchemaName(yamlSchema.getName());
            schema.setSchemaDatabase(yamlSchema.getDatabase());
            schema.setSchemaTables(mapper.writeValueAsString(yamlSchema.getTables()));
            schema.setSchemaOozieJobType(operationalParams.getOozieJobType());
            schema.setSchemaOozieBackfillJobType(operationalParams.getOozieBackfillJobType());

            schema.setSchemaUserIdFields(mapper.writeValueAsString(operationalParams.getUserIdFields()));
            schema.setSchemaDefaultFilters(mapper.writeValueAsString(operationalParams.getDefaultFilters()));
            schema.setSchemaDisableBullet(operationalParams.isDisableBullet());
            schema.setSchemaDisableFunnel(operationalParams.isDisableFunnel());
            if (!operationalParams.isDisableFunnel()) {
                schema.setSchemaTargetTable(operationalParams.getFunnelTargetTable());
            }
            schema.setSchemaBulletUrl(operationalParams.getBulletUrl());
            schema.setSchemaTimestampColumnParam(operationalParams.getTimestampColumnParam());
            schema.setIsSchemaDeleted(false);

            schemaNames.add(yamlSchema.getName());

            if (isUpdate) { // If the schema exists, update it
                ServiceFactory.schemaService().update(schema);
            } else { // If this schema does not exist, save as a new schema
                ServiceFactory.schemaService().save(schema);
            }

            for (YamlField yamlField : yamlSchema.getFields()) {
                log.info("  Adding field (name, type, id): ({}, {}, {})", yamlField.getName(), yamlField.getType(), yamlField.getId());


                Field field = null;
                try {
                    field = ServiceFactory.fieldService().fetchByName(schema.getPrimaryName() + Field.FIELD_NAME_SEPARATOR + yamlField.getName());
                } catch (DataValidatorException e) {
                    log.info("Adding field: {}", yamlSchema.getName());
                }
                // If this field does not exist
                if (field == null) {
                    // Construct the new field
                    field = new Field();
                    field.setSchemaName(schema.getSchemaName());
                    log.info("Set field with schema name: {}", field.getSchemaName());

                    field.setFieldName(yamlField.getName());
                    field.setFieldType(yamlField.getType());
                    if (yamlField.getId() == null) {
                        throw new Exception("Field " + field.getFieldName() + " missing id");
                    }
                    field.setFieldId(yamlField.getId());
                    // Store the new field in the database
                    ServiceFactory.fieldService().save(field);
                }


                // Add any keys the field might have
                if (yamlField.getKeys() != null) {
                    for (YamlKey yamlKey : yamlField.getKeys()) {
                        log.info("Adding key (name, id): ({}, {})", yamlKey.getName(), yamlKey.getId());

                        // Create the map key, pointing to the field it belongs to
                        FieldKey key = null;
                        try {
                            key = ServiceFactory.fieldKeyService().fetchByName(schema.getPrimaryName() + FieldKey.FIELD_KEY_NAME_SEPARATOR + field.getFieldId() + FieldKey.FIELD_KEY_NAME_SEPARATOR + yamlKey.getName());
                        } catch (DataValidatorException e) {
                            log.info("Adding Field Key: {}", yamlKey.getName());
                        }
                        // If this field does not exist
                        if (key == null) {
                            // Construct the new fieldKey
                            key = new FieldKey();
                            key.setKeyName(yamlKey.getName());
                            if (yamlKey.getId() == null) {
                                throw new Exception("FieldKey " + key.getKeyName() + " missing key id for subfield " + yamlKey.getName());
                            }
                            key.setKeyId(yamlKey.getId());
                            key.setSchemaName(schema.getSchemaName());
                            key.setFieldId(field.getFieldId());
                            // Store the new key in the database
                            ServiceFactory.fieldKeyService().save(key);

                        }

                    }
                }
            }

        }
        return schemaNames;
    }
}
