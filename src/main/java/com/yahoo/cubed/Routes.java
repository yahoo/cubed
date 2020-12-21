/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed;

import java.nio.charset.StandardCharsets;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.yahoo.cubed.json.FunnelQueryResult;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.FieldKey;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.pipeline.launch.PipelineLauncher;
import com.yahoo.cubed.pipeline.launch.PipelineLauncherManager;
import com.yahoo.cubed.pipeline.stop.PipelineStopperManager;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.querybullet.QueryBulletService;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.templating.DatamartTemplateGenerator;
import com.yahoo.cubed.templating.FunnelGroupTemplateGenerator;
import com.yahoo.cubed.templating.ScriptsTransformHql;

import com.yahoo.cubed.util.Status;
import com.yahoo.cubed.util.Utils;
import com.yahoo.cubed.source.HiveConnectionManager;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.concurrent.atomic.AtomicBoolean;

import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import spark.ModelAndView;
import spark.Request;
import spark.Response;

/**
 * Route the web requests.
 */
@Slf4j
public class Routes {
    /** Fields key. */
    public static final String FIELDS_KEY = "fields";

    /** Error constant. */
    public static final String ERROR_KEY = "error";

    /** Version key. */
    public static final String VERSION_KEY = "version";

    /** Bundle key. */
    public static final String BUNDLE_KEY = "bundle";

    /** Disable bullet key. */
    public static final String DISABLE_BULLET_KEY = "disable_bullet";

    /** Disable funnel key. */
    public static final String DISABLE_FUNNEL_KEY = "disable_funnel";

    /** Default filters key. */
    public static final String DEFAULT_FILTERS_KEY = "default_filters";

    /** User id fields. */
    public static final String USER_ID_FIELDS = "userIdFields";

    /** List data mart template file name. */
    public static final String LIST_DATA_MART_TEMPLATE = "list_datamart";

    /** New/View data mart template file name. */
    public static final String NEW_VIEW_DATA_MART_TEMPLATE = "new_view_datamart";

    /** New/View funnel group template file name. */
    public static final String NEW_VIEW_FUNNEL_GROUP_TEMPLATE = "new_view_funnel_group";

    /** AllBundleId template file name. */
    public static final String ALL_BUNDLE_ID_TEMPLATE = "all_bundle_id";

    /** OK status. */
    public static final String OK_STATUS = "OK";

    /** Empty response for OK status. */
    public static final String EMPTY_RESPONSE = "";

    /** Hive Connector. */
    public static HiveConnectionManager hiveConnector;

    /** Hive Pipeline Launcher Manager.*/
    public static PipelineLauncherManager pipelineLauncherManager;

    /** Datamart ETL Script Generator. */
    public static ScriptsTransformHql scriptsTransformHql; 

    /** Datamart Template Generator. */
    public static DatamartTemplateGenerator datamartTemplateGenerator;

    /** Funnel group Template Generator. */
    public static FunnelGroupTemplateGenerator funnelGroupTemplateGenerator;

    /** Default parameters. */
    public static Map<String, Object> defaultParams;

    /** Default backfill start date. */
    private static final String DEFAULT_BACKFILL_START_DATE_VALUE = "0";

    /** Template for generating oozie URLs. */
    private static final String OOZIE_LINK_TEMPLATE = CLISettings.OOZIE_URL + "/?job=%s";

    /** Used for testing purposes. */
    public static boolean SHOULD_RESET_HIVE_CONNECTOR = true;

    /** Separator for toString() result of List. */
    private static final String SEPARATOR_LIST_TO_STRING = ", ";

    /**
     * Flatten field list that may contain field with subfields.
     * @param fields list of fields
     * @return list of flattened fields
     */
    private static List<Field> flattenFields(List<Field> fields) {
        List<Field> result = new ArrayList<>();
        for (Field field : fields) {
            result.addAll(flattenField(field));
        }
        return result;
    }

    /**
     * Flatten field that may have subfields.
     * @param field that may have subfields
     * @return list of flattened fields
     */
    private static List<Field> flattenField(Field field) {
        List<Field> result = new ArrayList<>();
        final String genericKeyAnnotation = "*";
        // add field itself
        field.setKey(null);
        result.add(field);
        if (field.isNotSimpleType()) {
            if (field.getFieldKeys() == null || field.getFieldKeys().isEmpty()) {
                //add generic subfield: field[*]
                Field genericSubField = new Field();
                genericSubField.setFieldId(field.getFieldId());
                genericSubField.setFieldName(field.getFieldName());
                genericSubField.setFieldType(field.getFieldType());
                genericSubField.setKey(genericKeyAnnotation);
                result.add(genericSubField);
            } else {
                for (FieldKey key : field.getFieldKeys()) {
                    // add specific subfield: field[key]
                    Field specificSubfield = new Field();
                    specificSubfield.setFieldId(field.getFieldId());
                    specificSubfield.setFieldName(field.getFieldName());
                    specificSubfield.setFieldType(field.getFieldType());
                    specificSubfield.setKey(key.getKeyName());
                    result.add(specificSubfield);
                }
            }
        }
        return result;
    }

    /**
     * Initalize objects used during routing.
     */
    public static void init() throws Exception {
        if (pipelineLauncherManager == null) {
            pipelineLauncherManager = new PipelineLauncherManager();
        }
        if (datamartTemplateGenerator == null) {
            datamartTemplateGenerator = new DatamartTemplateGenerator();
        }
        if (funnelGroupTemplateGenerator == null) {
            funnelGroupTemplateGenerator = new FunnelGroupTemplateGenerator();
        }
        if (scriptsTransformHql == null) {
            scriptsTransformHql = new ScriptsTransformHql();
        }
        // Setup default params
        defaultParams = new HashMap<>();
        defaultParams.put(VERSION_KEY, CLISettings.VERSION);
    }

    /**
     * Update params for new_view_datamart, new_view_funnel, or new_view_funnel_group page.
     */
    private static void updateParamsWithSchemaName(Map<String, Object> params, String schemaName) {
        try {
            // Set params even if there are errors
            params.put("schemas", ServiceFactory.schemaService().fetchAllName(true));
            params.put("bulletSchemas", ServiceFactory.schemaService().fetchAllBulletSchemaName());
            params.put("funnelSchemas", ServiceFactory.schemaService().fetchAllFunnelSchemaName());
            params.put("defaultSchema", schemaName);

            Schema schema = ServiceFactory.schemaService().fetchByName(schemaName);
            ObjectMapper mapper = new ObjectMapper();
            // Set fields for the schema
            List<Field> schemaFields = flattenFields(schema.getFields());
            for (Field field : schemaFields) {
                field.setFieldNameId();
            }
            // Sort fields
            Collections.sort(schemaFields, new Comparator<Field>() {
                @Override
                public int compare(Field f1, Field f2) {
                    return f1.getFieldNameId().compareTo(f2.getFieldNameId());
                }
            });
            params.put(FIELDS_KEY, schemaFields);
            params.put(USER_ID_FIELDS, mapper.readValue(schema.getSchemaUserIdFields(), List.class));
            params.put(DEFAULT_FILTERS_KEY, mapper.readValue(schema.getSchemaDefaultFilters(), List.class));
            params.put(DISABLE_BULLET_KEY, schema.getSchemaDisableBullet());
            params.put(DISABLE_FUNNEL_KEY, schema.getSchemaDisableFunnel());
        } catch (Exception e) {
            // Add the error to the params
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }
    }

    /**
     * List the data marts page.
     */
    public static ModelAndView listDatamart(Request req, Response res) {
        // Log the connection
        log.info("List data marts");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        try {
            // Get the list of pipelines
            List<Pipeline> pipelines = ServiceFactory.pipelineService().fetchAll();
            pipelines = pipelines.stream().filter(p -> !p.getPipelineIsDeleted()).collect(Collectors.toList());

            for (Pipeline p : pipelines) {
                log.info("Update time at list_datamart for pipeline " + p.getPipelineEditTime());
                if (p.getPipelineEditTime() == null) {
                    // Use create time as update time if update time is empty.
                    // Need this for better FE datatable column sorting.
                    p.setPipelineEditTime(p.getPipelineCreateTime());
                }
            }

            // Pass the list of data marts to the UI
            params.put("listOfDataMarts", getDisplayPipelines(pipelines));

            // Get the list of funnel groups
            List<FunnelGroup> funnelGroups = ServiceFactory.funnelGroupService().fetchAll();
            funnelGroups = funnelGroups.stream().filter(p -> !p.getFunnelGroupIsDeleted()).collect(Collectors.toList());

            for (FunnelGroup funnelGroup : funnelGroups) {
                log.info("Update time at list_datamart for funnel group " + funnelGroup.getFunnelGroupEditTime());
                if (funnelGroup.getFunnelGroupEditTime() == null) {
                    // Use create time as update time if update time is empty.
                    // Need this for better FE datatable column sorting.
                    funnelGroup.setFunnelGroupEditTime(funnelGroup.getFunnelGroupCreateTime());
                }
            }

            // Pass the list of funnel groups to the UI
            params.put("listOfFunnelGroups", funnelGroups);

            // Set the title of the page
            params.put("title", "List of Marts");

            // Set the default status filter value
            params.put("statusFilter", "Deployed");

            // Set all schema options
            params.put("schemas", ServiceFactory.schemaService().fetchAllName(true));
            params.put("bulletSchemas", ServiceFactory.schemaService().fetchAllBulletSchemaName());
            params.put("funnelSchemas", ServiceFactory.schemaService().fetchAllFunnelSchemaName());
        } catch (Exception e) {
            // Add the error to the params
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the list of data marts
        return new ModelAndView(params, LIST_DATA_MART_TEMPLATE);
    }

    /**
     * List the deleted data marts page.
     */
    public static ModelAndView listDeletedDatamart(Request req, Response res) {
        // Log the connection
        log.info("List deleted data marts");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        try {
            // Get the list of deleted pipelines
            List<Pipeline> pipelines = ServiceFactory.pipelineService().fetchAll();
            pipelines = pipelines.stream().filter(p -> p.getPipelineIsDeleted()).collect(Collectors.toList());

            // Pass the list of deleted data marts to the UI
            params.put("listOfDataMarts", getDisplayPipelines(pipelines));

            // Pass the list of deleted funnel group to the UI
            List<FunnelGroup> funnelGroups = ServiceFactory.funnelGroupService().fetchAll();
            params.put("listOfFunnelGroups", funnelGroups.stream().filter(p -> p.getFunnelGroupIsDeleted()).collect(Collectors.toList()));

            // Set the title of the page
            params.put("title", "List of Deleted Marts");

            // Set the default status filter value
            params.put("statusFilter", "");

            // Set all schema options
            params.put("schemas", ServiceFactory.schemaService().fetchAllName(true));
            params.put("bulletSchemas", ServiceFactory.schemaService().fetchAllBulletSchemaName());
            params.put("funnelSchemas", ServiceFactory.schemaService().fetchAllFunnelSchemaName());
        } catch (Exception e) {
            // Add the error to the params
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the list of data marts
        return new ModelAndView(params, LIST_DATA_MART_TEMPLATE);
    }

    /**
     * Launch a data mart from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String queryBullet(Request req, Response res) {
        try {
            // create queryBullet instance to send pipeline and receive results
            QueryBulletService queryBulletService = ServiceFactory.queryBulletService();
            QueryBulletService.ResponseJson queryResult = queryBulletService.sendBulletQueryJson(req.body());

            if (queryResult.statusCode != HttpStatus.SC_OK) {
                res.status(500);
                String errMsg = String.format("Failed to send the bullet query with response code [%d]", queryResult.statusCode);
                log.error(errMsg);
                return errMsg;
            }
            // received successfully
            res.status(200);
            // Plain text output
            res.type("text/plain");
            return queryResult.jsonResponse;

        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Launch a data mart from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String launchDatamart(Request req, Response res) {
        // Log launch of pipeline
        log.info("Launch data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            String result = Utils.launchPipeline(dataMartId);
            res.status(200);
            // Plain text output
            res.type("text/plain");
            return result;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Generate, zip, and download a specific data mart.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code.
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String downloadDatamart(Request req, Response res) {
        // Log launch of pipeline
        log.info("Download data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            // Generate zip file for downloading
            String zipOutFile = Utils.zipPipeline(dataMartId);

            // Download the zipped file
            File file = new File(zipOutFile);
            res.raw().setContentType("application/force-download");
            String fileName = file.getName();
            String fileNameValidationErrorMessage = Utils.validateFileName(fileName);
            if (fileNameValidationErrorMessage != null) {
                log.error("Error: ", fileNameValidationErrorMessage);
                return fileNameValidationErrorMessage;
            }
            res.raw().setHeader("Content-Disposition", "attachment; filename=" + fileName);

            // Read the zip file to memory
            log.info("Read zip file {} into memory", zipOutFile);
            byte[] data = Files.readAllBytes(Paths.get(zipOutFile));

            // Write the data
            log.info("Write the zip file to the output stream", zipOutFile);
            res.raw().getOutputStream().write(data);
            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            // Successful launch
            res.status(200);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Preview HQL ETL script.
     *
     * @param req Request information, current data mart.
     * @param res Response information, data mart etl script.
     * @return Data mart etl script (200 status), or error message (500 status)
     */
    public static String previewDatamartEtl(Request req, Response res) {
        try {
            Pipeline model = Utils.constructFunnelmartPipeline(req.body(), -1, false, false);
            String datamartEtl = scriptsTransformHql.generateFile(model, -1);
            res.type("text/plain");
            res.status(200);
            log.info("Preview data mart ETL {}", datamartEtl);
            return datamartEtl;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Generate and send HQL ETL script.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code.
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String viewDatamartEtl(Request req, Response res) {
        // Log launch of pipeline
        log.info("View data mart ETL {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Get date string
            String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
            log.info("Version {} for pipeline {}", date, pipeline.getPipelineName());

            // Convert to long
            long version = Long.parseLong(date);
            String outputDir = datamartTemplateGenerator.generateTemplateFiles(pipeline, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);
            log.info("Generated template for {}.v{} at location {}", pipeline.getPipelineName(), date, outputDir);

            // Location of HQL file
            String hqlEtlFile = outputDir + "/scripts/transform.hql";

            // Send contents of HQL file
            File file = new File(hqlEtlFile);

            // Read the zip file to memory
            log.info("Read HQL ETL file {} into memory", hqlEtlFile);
            byte[] data = Files.readAllBytes(Paths.get(hqlEtlFile));
            String hqlEtlContents = new String(data, StandardCharsets.UTF_8);

            // Plain text output
            res.type("text/plain");

            // Successful launch
            res.status(200);
            log.info("Returning the contents of the HQL ETL file.");
            return hqlEtlContents;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Stop a pipeline with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String stopPipeline(Request req, Response res) {
        // Log launch of pipeline
        log.info("Stop data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long pipelineId = Long.parseLong(req.params(":id"), 10);

            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);

            // Stop pipeline
            PipelineStopperManager pipelineStopperManager = new PipelineStopperManager();
            pipelineStopperManager.stopPipeline(pipeline.getPipelineName(), CLISettings.PIPELINE_OWNER);

            // Successful stop
            res.status(200);
            // Mark pipeline as stopped
            pipeline.setPipelineStatus(Status.INACTIVE);
            ServiceFactory.pipelineService().update(pipeline);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Stop a funnel group and its funnels with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String stopFunnelGroup(Request req, Response res) {
        log.info("Stop funnel group {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Create the stopper manager that used for stopping the funnel group and pipeline
            PipelineStopperManager pipelineStopperManager = new PipelineStopperManager();

            // Stop the funnel group
            pipelineStopperManager.stopPipeline(funnelGroup.getFunnelGroupName(), CLISettings.PIPELINE_OWNER);
            funnelGroup.setFunnelGroupStatus(Status.INACTIVE);
            ServiceFactory.funnelGroupService().update(funnelGroup);
            log.info("Stopped funnel group {}", funnelGroupId);

            // Stop, and mark each funnel as inactive in the db
            for (Pipeline shallowPipeline : funnelGroup.getPipelines()) {
                long pipelineId = shallowPipeline.getPipelineId();
                String pipelineName = shallowPipeline.getPipelineName();
                // In practice, the pipelineStopperManager cannot be reused, unlike the pipelineLauncherManager
                pipelineStopperManager = new PipelineStopperManager();
                pipelineStopperManager.stopPipeline(pipelineName, CLISettings.PIPELINE_OWNER);
                Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);
                pipeline.setPipelineStatus(Status.INACTIVE);
                ServiceFactory.pipelineService().update(pipeline);
                log.info("Stopped funnel id: {}, name: {}", pipelineId, pipelineName);
            }

            // Successful stop
            res.status(200);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Delete a data mart with a DELETE request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteDatamart(Request req, Response res) {
        // Log launch of pipeline
        log.info("Delete data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Mark the datamart as deleted in the db
            pipeline.setPipelineIsDeleted(true);
            ServiceFactory.pipelineService().update(pipeline);
            log.info("Deleted datamart {}", dataMartId);

            // Successful delete
            res.status(200);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Relaunch all active datamarts.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String relaunchDatamart(Request req, Response res) {
        // Log launch of pipeline
        log.info("Relaunch all active data marts");

        try {
            // Get all active pipelines
            List<Pipeline> pipelines = ServiceFactory.pipelineService().fetchAll();
            pipelines = pipelines.stream().filter(p -> (!p.getPipelineIsDeleted() && p.getPipelineStatus().substring(0, Math.min(p.getPipelineStatus().length(), 8)).equals(Status.ACTIVE.substring(0, 8)))).collect(Collectors.toList());
            // Flags denoting the relaunch status;
            AtomicBoolean errorHappened = new AtomicBoolean(false);
            StringBuffer errorMsg = new StringBuffer ("");;

            class RelaunchInstance implements Runnable {
                // Pipeline to relaunch
                private Pipeline pipeline;
                String id;
                Thread t;

                RelaunchInstance(Pipeline p, String jobId) {
                    pipeline = p;
                    id = jobId;
                    t = new Thread(this);
                    t.start();
                }

                public void run() {
                    if (!errorHappened.get()) {
                        try {
                            long dataMartId = pipeline.getPipelineId();
                            // Stop pipeline
                            PipelineStopperManager pipelineStopperManager = new PipelineStopperManager();
                            pipelineStopperManager.stopPipeline(pipeline.getPipelineName(), CLISettings.PIPELINE_OWNER);
                            log.info("Stop Oozie job {} for pipeline {}", pipeline.getPipelineOozieJobId(), pipeline.getPipelineName());

                            // Fetch the pipeline again since the stopper erases the variable to null
                            pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

                            // Get date string
                            String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
                            // Convert to long
                            long version = Long.parseLong(date);
                            String outputDir = datamartTemplateGenerator.generateTemplateFiles(pipeline, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);
                            log.info("Generated template for {}.v{} at location {}", pipeline.getPipelineName(), date, outputDir);

                            // Setup launch scripts
                            log.info("Starting launch for {}.v{}...", pipeline.getPipelineName(), date);

                            // Backfill job config
                            String backfillStartDate = DEFAULT_BACKFILL_START_DATE_VALUE; // the initial is no backfill
                            if (pipeline.getPipelineBackfillStartTime() != null && !pipeline.getPipelineBackfillStartTime().isEmpty()) {
                                backfillStartDate = pipeline.getPipelineBackfillStartTime();
                            }

                            // Fetch schema info
                            Schema schema = ServiceFactory.schemaService().fetchByName(pipeline.getPipelineSchemaName());
                            final String oozieJobType = schema.getSchemaOozieJobType();
                            final String oozieBackfillJobType = schema.getSchemaOozieBackfillJobType();

                            // Initialize pipeline launcher
                            PipelineLauncherManager pipelineLauncherManager = new PipelineLauncherManager();
                            Future<PipelineLauncher.LaunchStatus> status = pipelineLauncherManager.launchPipeline(pipeline.getPipelineName(), date, CLISettings.PIPELINE_OWNER , outputDir, backfillStartDate, false, oozieJobType, oozieBackfillJobType);

                            // If error, modify flags
                            if (status.get().hasError) {
                                errorHappened.set(true);
                                errorMsg.append(status.get().errorMsg);
                            } else {
                                // Successful launch
                                // Mark pipeline as launched
                                log.info("Setting pipeline {} as launched", pipeline.getPipelineId());
                                pipeline.setPipelineStatus(String.format(Status.ACTIVE, Long.toString(pipeline.getPipelineVersion())));
                                log.info("Setting the Oozie job id {} for pipeline {}", status.get().oozieJobId, pipeline.getPipelineId());
                                pipeline.setPipelineOozieJobId(status.get().oozieJobId);
                                ServiceFactory.pipelineService().update(pipeline);
                            }

                        } catch (Exception e) {
                            errorHappened.set(true);
                            errorMsg.append(e.getMessage());
                        }
                    }
                }
            }

            // For a batch of 5 pipelines, stop/relaunch them concurrently.
            int batchSize = 5;
            for (int i = 0; i < pipelines.size(); i += batchSize) {
                log.info("Batch Start");
                List<RelaunchInstance> relaunchInstances = new ArrayList<>();
                for (int c = i; c < pipelines.size() && (c - i) < batchSize; c++) {
                    log.info("Create instance for {}/{}", c, pipelines.size());
                    relaunchInstances.add(new RelaunchInstance(pipelines.get(c), pipelines.get(c).getPipelineOozieJobId()));
                }
                log.info("Waiting Start");
                for (RelaunchInstance rl: relaunchInstances) {
                    try {
                        rl.t.join();
                        log.info("Thread {} finished", rl.id);
                    } catch (Exception e) {
                        errorHappened.set(true);
                        errorMsg.append(e.getMessage());
                    }
                }
                log.info("Waiting Finish");
                if (errorHappened.get()) {
                    log.info("Error: {}", errorMsg.toString());
                    log.info("Relaunch Aborted");
                    res.status(500);
                    return errorMsg.toString();
                }
                log.info("Batch Finish");
            }
            log.info("Relaunch Success");
            res.status(200);
            return EMPTY_RESPONSE;

        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Restore a data mart with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String restoreDatamart(Request req, Response res) {
        // Log update of pipeline
        log.info("Restoring data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);
            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Update pipeline
            pipeline.setPipelineIsDeleted(false);
            log.info("Restoring datamart {}", dataMartId);

            // Update the data mart
            ServiceFactory.pipelineService().update(pipeline);
            log.info("Restored datamart {}", dataMartId);

            // Successful update
            res.status(200);
            return Long.toString(dataMartId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Reset a data mart with a PUT request.
     * This allows projections to be edited again.
     *
     * @param req Request information, including JSON body.
     * @param res Response information.
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String resetDatamart(Request req, Response res) {
        // Log update of pipeline
        log.info("Reseting data mart {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);
            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Set pipeline to 'new' status
            pipeline.setPipelineStatus(Status.INACTIVE);
            pipeline.setPipelineOozieJobId(null);
            pipeline.setPipelineOozieBackfillJobId(null);
            log.info("Reseting datamart {}", dataMartId);

            // Update the data mart
            ServiceFactory.pipelineService().update(pipeline);
            log.info("Reset datamart {}", dataMartId);

            // Successful update
            res.status(200);
            return Long.toString(dataMartId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Update a data mart with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String updateDatamart(Request req, Response res) {
        // Log update of pipeline
        log.info("Updating data mart {}", req.params(":id"));

        try {
            long dataMartId = Long.parseLong(req.params(":id"), 10);
            Pipeline pipeline = Utils.constructFunnelmartPipeline(req.body(), dataMartId, false, false);
            pipeline.setProductName(CLISettings.INSTANCE_NAME);

            // Update the data mart
            ServiceFactory.pipelineService().update(pipeline);
            log.info("Updated datamart {}", dataMartId);

            // Successful update
            res.status(200);
            return Long.toString(dataMartId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Create a new data mart from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Pipeline id (201 status), or error message (500 status)
     */
    public static String createNewDatamart(Request req, Response res) {
        // Data mart POST submission
        log.info("Create new data mart");

        try {
            Pipeline pipeline = Utils.constructFunnelmartPipeline(req.body(), -1, false, false);

            pipeline.setProductName(CLISettings.INSTANCE_NAME);
            // Save the pipeline to the database
            ServiceFactory.pipelineService().save(pipeline);
            long pipelineId = pipeline.getPrimaryIdx();
            // Log the success
            log.info("Created data mart #{}", pipelineId);

            // Return the id of the new pipeline
            res.status(201);
            return Long.toString(pipelineId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Create a new data mart page.
     */
    public static ModelAndView newDatamart(Request req, Response res) {
        // Log the connection
        log.info("New data mart");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Update params related with specific schema
        updateParamsWithSchemaName(params, req.queryParams("schemaName"));
        // Render the template
        return new ModelAndView(params, NEW_VIEW_DATA_MART_TEMPLATE);
    }

    /**
     * Launch a funnel group from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String launchFunnelGroup(Request req, Response res) {
        // Log launch of funnel group
        log.info("Launch funnel group {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);
            String errorMsg = Utils.launchFunnelGroup(funnelGroupId);

            // Error happened when launching funnels in the funnel group
            if (!errorMsg.equals("")) {
                log.error("Error: ", errorMsg);
                res.status(500);
                return errorMsg;
            }

            res.status(200);
            // Plain text output
            res.type("text/plain");
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            // Error happened when launching the funnel group
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Generate, zip, and download a specific funnel group.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code.
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String downloadFunnelGroup(Request req, Response res) {
        log.info("Download funnel group {}", req.params(":id"));

        try {
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Generate zip file for downloading
            String zipOutFile = Utils.zipFunnelGroup(funnelGroupId);

            // Download the zipped file
            File file = new File(zipOutFile);
            res.raw().setContentType("application/force-download");
            String fileName = file.getName();
            String fileNameValidationErrorMessage = Utils.validateFileName(fileName);
            if (fileNameValidationErrorMessage != null) {
                log.error("Error: ", fileNameValidationErrorMessage);
                return fileNameValidationErrorMessage;
            }
            res.raw().setHeader("Content-Disposition", "attachment; filename=" + fileName);

            // Read the zip file to memory
            log.info("Read zip file {} into memory", zipOutFile);
            byte[] data = Files.readAllBytes(Paths.get(zipOutFile));

            // Write the data
            log.info("Write the zip file to the output stream", zipOutFile);
            res.raw().getOutputStream().write(data);
            res.raw().getOutputStream().flush();
            res.raw().getOutputStream().close();

            // Successful launch
            res.status(200);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Create a new funnel from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Pipeline id (201 status), or error message (500 status)
     */
    static String createNewFunnel(Request req, Response res) {
        // Data mart POST submission
        log.info("Create new funnel");
        try {
            Pipeline pipeline = Utils.constructFunnelmartPipeline(req.body(), -1, true, false);
            pipeline.setProductName(CLISettings.INSTANCE_NAME);
            // Save the pipeline to the database
            ServiceFactory.pipelineService().save(pipeline);
            long pipelineId = pipeline.getPrimaryIdx();

            // Log the success
            log.info("Created funnel #{}", pipelineId);

            // Return the id of the new pipeline
            res.status(201);
            return Long.toString(pipelineId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Create a new funnel group from a JSON POST.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return FunnelGroup id (201 status), or error message (500 status)
     */
    public static String createNewFunnelGroup(Request req, Response res) {
        log.info("Create new funnel group");
        try {
            FunnelGroup funnelGroup = Utils.constructFunnelGroup(req.body(), -1);
            funnelGroup.setProductName(CLISettings.INSTANCE_NAME);
            // Save the pipeline to the database
            ServiceFactory.funnelGroupService().save(funnelGroup);
            long funnelGroupId = funnelGroup.getPrimaryIdx();

            // Log the success
            log.info("Created funnel group {}", funnelGroupId);

            // Return the id of the new funnel group
            res.status(201);
            return Long.toString(funnelGroupId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Update a funnel group with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String updateFunnelGroup(Request req, Response res) {
        log.info("Updating funnel group {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Fetch the funnel group by id
            FunnelGroup oldFunnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Stop and delete old funnels in the funnel group
            PipelineStopperManager pipelineStopperManager = new PipelineStopperManager();
            for (Pipeline pipeline : oldFunnelGroup.getPipelines()) {
                // Stop the pipeline's oozie job
                pipelineStopperManager.stopPipeline(pipeline.getPipelineName(), CLISettings.PIPELINE_OWNER);
                // Delete the pipeline from database
                ServiceFactory.pipelineService().delete(pipeline.getPipelineId());
            }

            // Construct funnel group
            FunnelGroup funnelGroup = Utils.constructFunnelGroup(req.body(), funnelGroupId);
            funnelGroup.setProductName(CLISettings.INSTANCE_NAME);
            // Update the funnel group
            ServiceFactory.funnelGroupService().update(funnelGroup);
            log.info("Updated funnel group {}", funnelGroupId);
            // Save each funnel
            for (Pipeline pipeline : funnelGroup.getPipelines()) {
                ServiceFactory.pipelineService().save(pipeline);
                log.info("Saved funnel name: {}, id: {}", pipeline.getPipelineName(), pipeline.getPipelineId());
            }

            // Successful update
            res.status(200);
            return Long.toString(funnelGroupId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Delete a funnel group with a DELETE request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String deleteFunnelGroup(Request req, Response res) {
        // Log deletion of funnel group
        log.info("Delete funnel group {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Mark the funnel group as deleted in the db
            funnelGroup.setFunnelGroupIsDeleted(true);
            ServiceFactory.funnelGroupService().update(funnelGroup);
            log.info("Deleted funnel group {}", funnelGroupId);

            // Mark each funnel as deleted in the db
            for (Pipeline shallowPipeline : funnelGroup.getPipelines()) {
                long pipelineId = shallowPipeline.getPipelineId();
                String pipelineName = shallowPipeline.getPipelineName();
                Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);
                pipeline.setPipelineIsDeleted(true);
                ServiceFactory.pipelineService().update(pipeline);
                log.info("Deleted funnel id: {}, name: {}", pipelineId, pipelineName);
            }

            // Successful delete
            res.status(200);
            return EMPTY_RESPONSE;
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Restore a funnel group with a PUT request.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code..
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static String restoreFunnelGroup(Request req, Response res) {
        // Log restore of funnel group
        log.info("Restoring funnel group {}", req.params(":id"));

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);
            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Mark the funnel group as not deleted in the db
            funnelGroup.setFunnelGroupIsDeleted(false);
            ServiceFactory.funnelGroupService().update(funnelGroup);
            log.info("Restored funnel group {}", funnelGroupId);

            // Mark each funnel as not deleted in the db
            for (Pipeline shallowPipeline : funnelGroup.getPipelines()) {
                long pipelineId = shallowPipeline.getPipelineId();
                String pipelineName = shallowPipeline.getPipelineName();
                Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);
                pipeline.setPipelineIsDeleted(false);
                ServiceFactory.pipelineService().update(pipeline);
                log.info("Restored funnel id: {}, name: {}", pipelineId, pipelineName);
            }

            // Successful update
            res.status(200);
            return Long.toString(funnelGroupId);
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Create a new funnel group page with get method.
     */
    public static ModelAndView newFunnelGroupGet(Request req, Response res) {
        // Log the connection
        log.info("User viewed: New funnel");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Update params related with specific schema
        updateParamsWithSchemaName(params, req.queryParams("schemaName"));

        // Render the template
        return new ModelAndView(params, NEW_VIEW_FUNNEL_GROUP_TEMPLATE);
    }

    /**
     * Create a new funnel group page with post method.
     */
    public static ModelAndView newFunnelGroupPost(Request req, Response res) {
        // Log the connection
        log.info("User viewed: New funnel");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Update params related with specific schema
        updateParamsWithSchemaName(params, req.queryParams("schemaName"));

        log.info("Transform data:{}", req.queryParams("transformData"));

        try {

            Pipeline pipeline = Utils.constructFunnelmartPipeline(req.queryParams("transformData"), -1, true, false);
            // Set projections
            List<PipelineProjection> newProjections = pipeline.getProjections();
            // Set key into the field for each projection,
            // set name identification of a field, and also set it in projection
            for (PipelineProjection projection : newProjections) {
                projection.setFieldNameId(projection.getField().setFieldNameId());
            }

            params.put("userIdField", pipeline.getFunnelUserIdField());
            params.put("steps", pipeline.getFunnelStepsJson());
            params.put("stepNames", pipeline.getFunnelStepNames());
            params.put("queryRange", pipeline.getFunnelQueryRange());
            params.put("repeatInterval", pipeline.getFunnelRepeatInterval());
            params.put("pipelineName", pipeline.getPipelineName());
            params.put("pipelineDescription", pipeline.getPipelineDescription());
            params.put("pipelineOwner", pipeline.getPipelineOwner());
            params.put("projections", newProjections);
            params.put("filters", pipeline.getPipelineFilterJson());
        } catch (Exception e) {
            // Log the error
            log.error(e.getMessage());

            // Add the error
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }
        // Render the template
        return new ModelAndView(params, NEW_VIEW_FUNNEL_GROUP_TEMPLATE);
    }

    private static void setupHiveConnector() {
        // Start Hive server
        hiveConnector = new HiveConnectionManager();
        String[] args = new String[] {"--hive-jdbc", CLISettings.HIVE_JDBC_URL};
        hiveConnector.setup(args);
    }

    /**
     * Preview a funnel query.
     * @param req
     * @param res
     * @return
     */
    public static String previewFunnelQuery(Request req, Response res) {
        // Data mart POST submission
        log.info("User action: Previewed funnel query");

        try {
            String funnelQueryHiveString = Utils.createAndValidateFunnelQuery(req.body(), false, false);
            log.info("Preview query: {}", funnelQueryHiveString);
            res.status(200);
            return funnelQueryHiveString;
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Sum up funnel results for the same category.
     *
     * @param originalUDFResults Funnel udf results without summing up the for each step.
     * @return the combined funnel query result
     */
    private static ArrayList<FunnelQueryResult> zipFunnelQueryUDF(ArrayList<FunnelQueryResult> originalUDFResults) {
        Map<String, List<Long>> aggregationKeyMap = new HashMap<>();
        ArrayList<FunnelQueryResult> zippedUDFResults = new ArrayList<>();
        for (FunnelQueryResult result : originalUDFResults) {
            String key = result.getKeys().toString();
            key = key.substring(1, key.length() - 1);
            if (!aggregationKeyMap.containsKey(key)) {
                aggregationKeyMap.put(key, new ArrayList<Long>());
            }
            List<String> resultValues = result.getValues();
            List<Long> value = aggregationKeyMap.get(key);
            for (int i = 0; i < resultValues.size(); i++) {
                String s = resultValues.get(i);
                if (s.length() > 0) {
                    // if s is "", just ignore it
                    long eValue = Long.parseLong(s);
                    if (i >= value.size()) {
                        value.add(eValue);
                    } else {
                        value.set(i, value.get(i) + eValue);
                    }
                }
            }
        }

        for (String key : aggregationKeyMap.keySet()) {
            // if string is "", we need return the empty list
            List<String> keys = new ArrayList<>();
            if (key.length() > 0) {
                keys = Arrays.asList(key.split(SEPARATOR_LIST_TO_STRING));
            }
            List<String> strVals = aggregationKeyMap.get(key).stream().map(x -> x.toString()).collect(Collectors.toList());
            FunnelQueryResult item = new FunnelQueryResult(keys, strVals);
            zippedUDFResults.add(item);
        }
        return zippedUDFResults;
    }

    /**
     * Run a funnel query.
     * @param req
     * @param res
     * @return
     */
    public static String runFunnelQuery(Request req, Response res) {
        // Data mart POST submission
        log.info("User action: Issued funnel query");

        try {
            String funnelQueryHiveString = Utils.createAndValidateFunnelQuery(req.body(), false, false);
            log.info("Query: {}", funnelQueryHiveString);

            if (SHOULD_RESET_HIVE_CONNECTOR) {
                setupHiveConnector();
            }

            ArrayList<FunnelQueryResult> results = hiveConnector.execute(funnelQueryHiveString);
            if (results == null) {
                throw new Exception("Hive connector could not fetch results");
            }
            results = zipFunnelQueryUDF(results);
            log.info("Finished running, received {} results", results.size());
            ObjectMapper mapper = new ObjectMapper();
            String resultStr = mapper.writeValueAsString(results);
            res.status(200);
            return resultStr;
        } catch (Exception e) {
            res.status(500);
            return e.getMessage();
        }
    }

    /**
     * Clone a funnel group.
     */
    public static ModelAndView cloneFunnelGroup(Request req, Response res) {
        // Log the connection
        log.info("Clone funnel group");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Let the UI know that this is a clone of an existing pipeline
        params.put("clonePipeline", "true");

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Log the datamart we wish to clone
            log.info("Clone of {}", funnelGroupId);

            // Fetch the data mart by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Set key into the field for each projection,
            // set name identification of a field, and also set it in projection
            for (PipelineProjection projection : funnelGroup.getProjections()) {
                projection.setFieldNameId(projection.getField().setFieldNameId());
            }

            // Add user id fields
            params.put("userIdField", funnelGroup.getFunnelGroupUserIdField());

            // Pass the pipeline to the UI
            params.put("steps", funnelGroup.getFunnelGroupStepsJson());
            params.put("stepNames", funnelGroup.getFunnelGroupStepNames());
            params.put("queryRange", funnelGroup.getFunnelGroupQueryRange());
            params.put("repeatInterval", funnelGroup.getFunnelGroupRepeatInterval());
            params.put("pipelineName", funnelGroup.getFunnelGroupName());
            params.put("pipelineDescription", funnelGroup.getFunnelGroupDescription());
            params.put("pipelineOwner", funnelGroup.getFunnelGroupOwner());
            params.put("projections", funnelGroup.getProjections());
            params.put("filters", funnelGroup.getFunnelGroupFilterJson());


            params.put("backfillStartDate", funnelGroup.getFunnelGroupBackfillStartTime());
            params.put("endTimeEnabled", funnelGroup.isFunnelGroupEndTimeEnabled());

            params.put("pipelineDeleted", funnelGroup.getFunnelGroupIsDeleted());
            params.put("pipelineStatus", funnelGroup.getFunnelGroupStatus());
            params.put("funnelGroupTopology", funnelGroup.getFunnelGroupTopology());
            params.put("funnelGroupFunnelNames", funnelGroup.getFunnelNames());

            updateParamsWithSchemaName(params, funnelGroup.getFunnelGroupSchemaName());
        } catch (NumberFormatException e) {
            // The error message
            String message = "Error parsing funnel group id.";

            // Log the error
            log.error(message);

            // Add the error
            params.put(ERROR_KEY, message);
            log.error("Error: ", e);
        } catch (Exception e) {
            // Log the error
            log.error(e.getMessage());

            // Add the error
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the datamart view
        return new ModelAndView(params, NEW_VIEW_FUNNEL_GROUP_TEMPLATE);
    }


    /**
     * Clone a datamart.
     */
    public static ModelAndView cloneDatamart(Request req, Response res) {
        // Log the connection
        log.info("Clone datamart");

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Let the UI know that this is a clone of an existing pipeline
        params.put("clonePipeline", "true");

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            // Log the datamart we wish to clone
            log.info("Clone of {}", dataMartId);

            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Set key into the field for each projection,
            // set name identification of a field, and also set it in projection
            for (PipelineProjection projection : pipeline.getProjections()) {
                projection.setFieldNameId(projection.getField().setFieldNameId());
            }

            // Pass the pipeline to the UI
            params.put("pipelineName", pipeline.getPipelineName());
            params.put("pipelineDescription", pipeline.getPipelineDescription());
            params.put("pipelineOwner", pipeline.getPipelineOwner());
            params.put("projections", pipeline.getProjections());
            params.put("filters", pipeline.getPipelineFilterJson());

            updateParamsWithSchemaName(params, pipeline.getPipelineSchemaName());;
        } catch (NumberFormatException e) {
            // The error message
            String message = "Error parsing pipeline id.";

            // Log the error
            log.error(message);

            // Add the error
            params.put(ERROR_KEY, message);
            log.error("Error: ", e);
        } catch (Exception e) {
            // Log the error
            log.error(e.getMessage());

            // Add the error
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the datamart view
        return new ModelAndView(params, NEW_VIEW_DATA_MART_TEMPLATE);
    }

    /**
     * Generate an oozie job link.
     */
    private static String generateOozieJobLink(String jobId) {
        if (jobId == null || jobId.isEmpty()) {
            return null;
        }
        return String.format(OOZIE_LINK_TEMPLATE, jobId);
    }

    /**
     * View a specific data mart page.
     */
    public static ModelAndView viewDatamart(Request req, Response res) {
        // Log the connection
        log.info("View data mart {}", req.params(":id"));

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Let the UI know that this is a view of an existing pipeline
        params.put("viewPipeline", "true");

        try {
            // Try to convert id string to long
            long dataMartId = Long.parseLong(req.params(":id"), 10);

            // Fetch the data mart by id
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(dataMartId);

            // Set key into the field for each projection,
            // set name identification of a field, and also set it in projection
            for (PipelineProjection projection : pipeline.getProjections()) {
                projection.setFieldNameId(projection.getField().setFieldNameId());
            }

            // generate oozie job link
            String oozieJobLink = generateOozieJobLink(pipeline.getPipelineOozieJobId());

            // generate oozie backfill job link
            String oozieBackfillJobLink = generateOozieJobLink(pipeline.getPipelineOozieBackfillJobId());

            // Pass the pipeline to the UI
            params.put("pipelineName", pipeline.getPipelineName());
            params.put("pipelineDescription", pipeline.getPipelineDescription());
            params.put("pipelineOwner", pipeline.getPipelineOwner());
            params.put("projections", pipeline.getProjections());
            params.put("filters", pipeline.getPipelineFilterJson());
            params.put("oozieJobId", pipeline.getPipelineOozieJobId());
            params.put("oozieJobLink", oozieJobLink);
            params.put("oozieJobStatus", pipeline.getPipelineOozieJobStatus());
            params.put("oozieBackfillJobId", pipeline.getPipelineOozieBackfillJobId());
            params.put("oozieBackfillJobLink", oozieBackfillJobLink);
            params.put("oozieBackfillJobStatus", pipeline.getPipelineOozieBackfillJobStatus());
            params.put("backfillEnabled", pipeline.isPipelineBackfillEnabled());
            params.put("backfillStartDate", pipeline.getPipelineBackfillStartTime());
            params.put("endTimeEnabled", pipeline.isPipelineEndTimeEnabled());
            params.put("endTimeDate", pipeline.getPipelineEndTime());
            params.put("pivotUILink", getPivotUiLink(pipeline));
            params.put("supersetUILink", CLISettings.SUPERSET_URL);
            params.put("pipelineDeleted", pipeline.getPipelineIsDeleted());
            params.put("pipelineStatus", pipeline.getPipelineStatus());

            updateParamsWithSchemaName(params, pipeline.getPipelineSchemaName());
        } catch (NumberFormatException e) {
            // The error message
            String message = "Error parsing pipeline id.";

            // Log the error
            log.error(message);

            // Add the error
            params.put(ERROR_KEY, message);
            log.error("Error: ", e);
        } catch (Exception e) {
            // Log the error
            log.error(e.getMessage());

            // Add the error
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the datamart view
        return new ModelAndView(params, NEW_VIEW_DATA_MART_TEMPLATE);
    }

    private static String getPivotUiLink(Pipeline pipeline) {
        return String.format(CLISettings.TURNILO_URL, pipeline.getProductName(), pipeline.getPipelineName());
    }

    private static String getPivotUiLinkFunnelGroup(FunnelGroup funnelGroup) {
        return String.format(CLISettings.TURNILO_URL, funnelGroup.getProductName(), funnelGroup.getFunnelGroupName());
    }

    /**
     * View a specific funnel group page.
     */
    public static ModelAndView viewFunnelGroup(Request req, Response res) {
        // Log the connection
        log.info("View funnel {}", req.params(":id"));

        // Store the values to pass to the UI
        Map<String, Object> params = new HashMap<>(defaultParams);

        // Let the UI know that this is a view of an existing pipeline
        params.put("viewPipeline", "true");

        try {
            // Try to convert id string to long
            long funnelGroupId = Long.parseLong(req.params(":id"), 10);

            // Fetch the funnel group by id
            FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

            // Set key into the field for each projection,
            // set name identification of a field, and also set it in projection
            for (PipelineProjection projection : funnelGroup.getProjections()) {
                projection.setFieldNameId(projection.getField().setFieldNameId());
            }

            // generate oozie job link
            String oozieJobLink = generateOozieJobLink(funnelGroup.getFunnelGroupOozieJobId());

            // Add user id fields
            params.put("userIdField", funnelGroup.getFunnelGroupUserIdField());

            // Pass the funnel group to the UI
            params.put("steps", funnelGroup.getFunnelGroupStepsJson());
            params.put("stepNames", funnelGroup.getFunnelGroupStepNames());
            params.put("queryRange", funnelGroup.getFunnelGroupQueryRange());
            params.put("repeatInterval", funnelGroup.getFunnelGroupRepeatInterval());
            params.put("pipelineName", funnelGroup.getFunnelGroupName());
            params.put("pipelineDescription", funnelGroup.getFunnelGroupDescription());
            params.put("pipelineOwner", funnelGroup.getFunnelGroupOwner());
            params.put("projections", funnelGroup.getProjections());
            params.put("filters", funnelGroup.getFunnelGroupFilterJson());
            params.put("oozieJobId", funnelGroup.getFunnelGroupOozieJobId());
            params.put("oozieJobLink", oozieJobLink);
            params.put("oozieJobStatus", funnelGroup.getFunnelGroupOozieJobStatus());
            params.put("backfillStartDate", funnelGroup.getFunnelGroupBackfillStartTime());
            params.put("endTimeEnabled", funnelGroup.isFunnelGroupEndTimeEnabled());
            params.put("endTimeDate", funnelGroup.getFunnelGroupEndTime());
            params.put("pivotUILink", getPivotUiLinkFunnelGroup(funnelGroup));
            params.put("supersetUILink", CLISettings.SUPERSET_URL);
            params.put("pipelineDeleted", funnelGroup.getFunnelGroupIsDeleted());
            params.put("pipelineStatus", funnelGroup.getFunnelGroupStatus());
            params.put("funnelGroupTopology", funnelGroup.getFunnelGroupTopology());
            params.put("funnelGroupFunnelNames", funnelGroup.getFunnelNames());

            updateParamsWithSchemaName(params, funnelGroup.getFunnelGroupSchemaName());
        } catch (NumberFormatException e) {
            // The error message
            String message = "Error parsing funnel group id.";

            // Log the error
            log.error(message);

            // Add the error
            params.put(ERROR_KEY, message);
            log.error("Error: ", e);
        } catch (Exception e) {
            // Log the error
            log.error(e.getMessage());

            // Add the error
            params.put(ERROR_KEY, e.getMessage());
            log.error("Error: ", e);
        }

        // Render the datamart view
        return new ModelAndView(params, NEW_VIEW_FUNNEL_GROUP_TEMPLATE);
    }

    /**
     * Generate and send all active bundle ids.
     *
     * @param req Request information, including JSON body.
     * @param res Response information, including code.
     * @return Nothing on success (200 status), or error message (500 status)
     */
    public static ModelAndView allBundleIds(Request req, Response res) {
        // Log launch of pipeline
        log.info("List all bundle ids.");
        Map<String, Object> params = new HashMap<>(defaultParams);
        params.put(VERSION_KEY, CLISettings.VERSION);
        try {
            // Get all active pipelines
            List<Pipeline> pipelines = ServiceFactory.pipelineService().fetchAll();
            pipelines = pipelines.stream().filter(p -> !p.getPipelineIsDeleted()).collect(Collectors.toList());

            // Store the hourly bundle ids
            StringBuilder hourlyBundleIds = new StringBuilder();
            hourlyBundleIds.append("=======================\n");
            hourlyBundleIds.append("Hourly Oozie Bundle Ids\n");
            hourlyBundleIds.append("=======================\n");

            // Store the daily bundle ids
            StringBuilder backfillBundleIds = new StringBuilder();
            backfillBundleIds.append("===============================\n");
            backfillBundleIds.append("Daily Backfill Oozie Bundle Ids\n");
            backfillBundleIds.append("===============================\n");

            // For each pipeline, add the bundle ids
            for (Pipeline p : pipelines) {
                if (p.getPipelineOozieJobId() != null) {
                    hourlyBundleIds.append(p.getPipelineOozieJobId() + "\n");
                }
                if (p.getPipelineOozieBackfillJobId() != null) {
                    backfillBundleIds.append(p.getPipelineOozieBackfillJobId() + "\n");
                }
            }

            // Add footer at the end
            hourlyBundleIds.append("#######################\n");
            backfillBundleIds.append("###############################\n");

            // Successful launch
            res.status(200);
            log.info("Returning all active Oozie bundle ids.");

            params.put(BUNDLE_KEY, hourlyBundleIds.toString() + backfillBundleIds.toString());
            return new ModelAndView(params, ALL_BUNDLE_ID_TEMPLATE);
        } catch (Exception e) {
            log.error("Error: ", e);
            res.status(500);
            return new ModelAndView(params, ALL_BUNDLE_ID_TEMPLATE);
        }
    }

    /**
     * Return OK status for health check.
     */
    public static String getStatus(Request req, Response res) {
        return OK_STATUS;
    }

    private static List<Pipeline> getDisplayPipelines(List<Pipeline> pipelines) {
        return pipelines.stream().filter(p -> !p.getPipelineType().equals(Utils.FUNNEL)).collect(Collectors.toList());
    }
}
