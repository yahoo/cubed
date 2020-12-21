/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.core.type.TypeReference;
import com.yahoo.cubed.Routes;
import com.yahoo.cubed.json.Funnelmart;
import com.yahoo.cubed.json.NewDatamart;
import com.yahoo.cubed.json.UpdateDatamart;
import com.yahoo.cubed.json.FunnelQuery;
import com.yahoo.cubed.json.NewFunnelQuery;
import com.yahoo.cubed.json.FunnelGroupQuery;
import com.yahoo.cubed.json.NewFunnelGroupQuery;
import com.yahoo.cubed.json.UpdateFunnelGroupQuery;
import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.json.filter.LogicalRule;

import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.pipeline.launch.PipelineLauncher;
import com.yahoo.cubed.service.ServiceFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Collections;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.io.IOException;

import com.yahoo.cubed.service.bullet.query.BulletQuery;
import com.yahoo.cubed.service.cardinality.CardinalityEstimationService;
import com.yahoo.cubed.settings.CLISettings;
import com.yahoo.cubed.templating.FunnelHql;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.http.HttpStatus;

/**
 * General utilities.
 */
@Slf4j
public class Utils {
    /** Protected unix ts column. */
    private static final String PROTECTED_COLUMN_UNIX_TS = "hour_unix_timestamp";
    /** Protected granularity column. */
    private static final String PROTECTED_COLUMN_GRANULARITY = "granularity";
    /** Protected dt column. */
    private static final String PROTECTED_COLUMN_DT = "dt";
    /** Separator for map column aliases. */
    private static final String MAP_COLUMN_KEY_ALIAS_SEPARATOR = "_";
    /** Date Format. */
    public static final String QUERY_DATE_FORMAT = "yyyyMMdd";
    /** Maximum number of days that a query can run. */
    public static final int QUERY_MAX_NUM_DAYS = 30;
    /** Date template for displaying pipeline creation time. */
    private static final String CREATE_DATE_TEMPLATE = "yyyy/MM/dd";
    /** Default backfill start date. */
    private static final String DEFAULT_BACKFILL_START_DATE_VALUE = "0";
    /** Dev environment identifier. */
    private static final String DEV_ENV = "dev";
    /** Step name delimiter when storing funnel steps json. */
    private static final String STEP_NAME_DELIMITER = "\n\n";
    /** Underscore delimiter. */
    public static final String UNDERSCORE_DELIMITER = "_";
    /** Dash delimiter. */
    private static final String DASH_DELIMITER = "-";
    /** Flag used for identifying the start of funnel in the funnel group context. */
    private static final String FUNNEL_START_FLAG = "START";
    /** Json key: stepNames. */
    private static final String STEP_NAMES_JSON_KEY = "stepNames";
    /** Json key: steps. */
    private static final String STEPS_JSON_KEY = "steps";
    /** Json key: name. */
    private static final String NAME_JSON_KEY = "name";
    /** Json key: description. */
    private static final String DESCRIPTION_JSON_KEY = "description";
    /** Json key: topology. */
    private static final String TOPOLOGY_JSON_KEY = "topology";
    /** Json key: funnelNames. */
    private static final String FUNNEL_NAMES_JSON_KEY = "funnelNames";
    /** Funnel pipeline type. */
    public static final String FUNNEL = "funnel";
    /** The name json field. */
    public static final String NAME_JSON_FIELD = "name";
    /** The description json field. */
    public static final String DESCRIPTION_JSON_FIELD = "description";
    /** The owner json field. */
    public static final String OWNER_JSON_FIELD = "owner";
    /** The alias json field. */
    private static final String ALIAS_JSON_FIELD = "alias";
    /** The field key json field. */
    private static final String FIELD_KEY_JSON_FIELD = "key";
    /** The value mapping value json field. */
    public static final String VM_VALUE_JSON_FIELD = "value mapping value";
    /** The value mapping alias json field. */
    public static final String VM_ALIAS_JSON_FIELD = "value mapping alias";
    /** The funnel step name json field. */
    public static final String FUNNEL_STEP_NAME_JSON_FIELD = "funnel step name";
    /** Regular text field pattern in json requests. */
    private static final Pattern REGULAR_TEXT_FIELD_PATTERN = Pattern.compile("^[A-Za-z0-9 _-]*$");
    /** Funnel names serialized json string pattern in json requests. */
    private static final Pattern FUNNEL_NAMES_PATTERN = Pattern.compile("^[A-Za-z0-9\"{},: _-]*$");
    /** File name pattern used in file downloads. */
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("^[A-Za-z0-9 ._-]*$");


    /**
     * Validate projections object.
     * @param projections
     * @return
     */
    public static String validateProjections(List<Map<String, String>> projections) {
        
        // Need at least 1 projection
        if (projections.size() == 0) {
            return "Need at least one dimension or metric";
        }
        // Set of all aliases
        Set<String> columnNameSet = new HashSet<>();
        // Add reserved column names
        columnNameSet.add(PROTECTED_COLUMN_UNIX_TS);
        columnNameSet.add(PROTECTED_COLUMN_GRANULARITY);
        columnNameSet.add(PROTECTED_COLUMN_DT);
        // Check projections
        for (Map<String, String> projection : projections) {

            // Get the column id
            try {
                // Try to convert column id string to long
                long columnId = Long.parseLong(projection.get(NewDatamart.COLUMN_ID), 10);
                String schemaName = projection.get(NewDatamart.SCHEMA_NAME);

                // Fetch the field
                Field field = ServiceFactory.fieldService().fetchByCompositeKey(schemaName, columnId);

                // Store projected column name (either alias or column name)
                String projectColumnName;

                String alias = projection.get(NewDatamart.ALIAS);
                String key = projection.get(NewDatamart.KEY);

                // The alias and the key are user inputs, so validate them if malicious strings
                String regularTextFieldErrorMessage = ObjectUtils.firstNonNull(
                        alias == null ? null : Utils.validateRegularTextJsonField(alias, Utils.ALIAS_JSON_FIELD),
                        key == null ? null : Utils.validateRegularTextJsonField(key, Utils.FIELD_KEY_JSON_FIELD));
                if (regularTextFieldErrorMessage != null) {
                    return regularTextFieldErrorMessage;
                }

                if (alias != null && alias.trim().length() > 0) {
                    // Provided alias by user
                    projectColumnName = alias;
                } else if (key != null && key.trim().length() > 0) {
                    // No alias provided, map column projection with key
                    projectColumnName = key;
                } else {
                    // No alias provided, raw column projection
                    projectColumnName = field.getFieldName();
                }

                // Check if projected column name already exists
                if (columnNameSet.contains(projectColumnName)) {
                    return "Duplicate column alias:" + projectColumnName;
                }
                // Add alias to set
                columnNameSet.add(projectColumnName);
            } catch (Exception e) {
                log.error("Exception parsing new data mart: ", e);
                return "Error while parsing new data mart";
            }
        }
        return null;
    }

    /**
     * Check if projections contains at least 1 aggregate.
     * @param projections
     * @return
     */
    public static String projectionsHaveAtLeastOneAggregate(List<Map<String, String>> projections) {
        boolean containsAggregate = false;
        // Check projections
        for (Map<String, String> projection : projections) {
            String aggregate = projection.get(NewDatamart.AGGREGATE);
            if (aggregate != null && aggregate.length() > 0 && !Constants.AGGREGATE_NONE.equals(aggregate)) {
                containsAggregate = true;
            }
        }
        if (!containsAggregate && projections.size() > 0) {
            return "Projection must have at least one aggregate";
        }
        return null;
    }

    /**
     * validate Filter object.
     * @param filter
     * @return
     */
    public static String validateFilters(Filter filter) {
        // Check filters
        if (!(filter instanceof LogicalRule)) {
            return "filter must have junction operation at the top level.";
        }
        return null;
    }

    /**
     * Build projections from parsed json.
     * @param projectionListMap
     * @return
     * @throws Exception
     */
    public static List<PipelineProjection> buildProjections(List<Map<String, String>> projectionListMap, boolean isInOriginalSchema) throws Exception {
        // Build the list of projections
        List<PipelineProjection> projections = new ArrayList<>();
        for (Map<String, String> projectionMap : projectionListMap) {
            PipelineProjection projection = new PipelineProjection();
            // Get the field id
            Field field = ServiceFactory.fieldService().fetchByCompositeKey(projectionMap.get(NewDatamart.SCHEMA_NAME), Long.parseLong(projectionMap.get(NewDatamart.COLUMN_ID)));
            projection.setField(field);

            // By default, alias is the column name
            projection.setAlias(field.getFieldName());

            // Add key if not empty
            String key = projectionMap.get(NewDatamart.KEY);
            if (key != null && key.trim().length() > 0) {
                projection.setKey(key);
                // By default, alias for a map column is `column_key`
                projection.setAlias(field.getFieldName() + MAP_COLUMN_KEY_ALIAS_SEPARATOR + key);
            }

            // If user provided an alias, use that instead
            String alias = projectionMap.get(NewDatamart.ALIAS);
            if (alias != null && alias.trim().length() > 0) {
                projection.setAlias(alias);
            }

            // Add aggregate
            projection.setAggregation(Aggregation.byName(projectionMap.get(NewDatamart.AGGREGATE)));
            // Set projection as not part of original schema
            projection.setInOriginalSchema(isInOriginalSchema);
            // Add projection to list
            projections.add(projection);
            // Log the projection we are adding
            log.info("Adding projection: {}", projection.toString());
        }
        return projections;
    }

    /**
     * Check for duplicates in list.
     */
    public static <T> T checkListForDuplicates (List<T> list) throws Exception {
        Set<T> set = new HashSet<>();
        for (T element : list) {
            if (set.contains(element)) {
                return element;
            }
            set.add(element);
        }
        return null;
    }


    /**
     * Given a list of projections, add aliases if not provided by the user.
     * @param projections
     * @return
     * @throws Exception
     */
    public static List<Map<String, String>> setAliases(List<Map<String, String>> projections) throws Exception {
        // Build the list of projections
        for (Map<String, String> projectionMap : projections) {
            // Get the field id
            Field field =  ServiceFactory.fieldService().fetchByCompositeKey(projectionMap.get(FunnelQuery.SCHEMA_NAME), Long.parseLong(projectionMap.get(FunnelQuery.COLUMN_ID), 10));

            if (!projectionMap.containsKey(FunnelQuery.ALIAS) || projectionMap.get(FunnelQuery.ALIAS).length() == 0) {
                // By default, alias is the column name
                projectionMap.put(FunnelQuery.ALIAS, field.getFieldName());

                // Add key if not empty
                String key = projectionMap.get(FunnelQuery.KEY);
                if (key != null && key.trim().length() > 0) {
                    // By default, alias for a map column is `column_key`
                    projectionMap.put(FunnelQuery.ALIAS, field.getFieldName() + MAP_COLUMN_KEY_ALIAS_SEPARATOR + key);
                }
            }
        }
        return projections;
    }

    /**
     * Generate Funnel Query.
     * @param query
     * @param isPipeline
     * @return
     * @throws Exception
     */
    public static String createAndValidateFunnelQuery(String query, boolean isPipeline, boolean isFunnelGroup) throws Exception {
        // Parse the JSON
        ObjectMapper mapper = new ObjectMapper();
        NewFunnelQuery fQuery = mapper.readValue(query, NewFunnelQuery.class);

        // Log the request JSON
        log.info("JSON: {}", fQuery.toString());

        // Validate the JSON object
        if (fQuery.isValid() != null) {
            log.info("Funnel query was not valid");
            throw new Exception(fQuery.isValid());
        }

        fQuery.setProjections(Utils.setAliases(fQuery.getProjections()));

        return FunnelHql.generateFile(fQuery, isPipeline, isFunnelGroup).trim();
    }

    /**
     * Construct proper pipeline for creating/updating.
     */
    public static Pipeline constructFunnelmartPipeline(String jsonReq, long funnelmartId, boolean isFunnel, boolean isPartOfFunnelGroup) throws Exception {
        boolean isUpdate = funnelmartId != -1;
        ObjectMapper mapper = new ObjectMapper();
        Funnelmart funnelmart;
        if (!isFunnel) {
            funnelmart = isUpdate ? mapper.readValue(jsonReq, UpdateDatamart.class) : mapper.readValue(jsonReq, NewDatamart.class);
        } else {
            funnelmart = mapper.readValue(jsonReq, NewFunnelQuery.class);
        }

        // Validate the JSON object
        if (funnelmart.isValid() != null) {
            throw new Exception(funnelmart.isValid());
        }

        // Build the pipeline
        Pipeline pipeline = isUpdate ? ServiceFactory.pipelineService().fetch(funnelmartId) : new Pipeline();

        if (!isUpdate) {
            // Log the request JSON
            log.info("JSON: {}", funnelmart.toString());
            // Force lowercase pipeline name
            if (isFunnel) {
                pipeline.setPipelineName(((NewFunnelQuery) funnelmart).getName().toLowerCase());
            } else {
                pipeline.setPipelineName(((NewDatamart) funnelmart).getName().toLowerCase());
            }
            // Start as INACTIVE when create new pipeline.
            pipeline.setPipelineStatus(Status.INACTIVE);
            // Version 1 by default
            pipeline.setPipelineVersion(1L);
            // Set schemaId for the pipeline
            pipeline.setPipelineSchemaName(funnelmart.getSchemaName());
        } else {
            // Log the request JSON
            log.info("Funnelmart {} update JSON: {}", funnelmartId, funnelmart.toString());
            // Set pipeline version
            pipeline.setPipelineVersion(pipeline.getPipelineVersion() + 1L);
        }

        pipeline.setPipelineDescription(funnelmart.getDescription());
        pipeline.setPipelineOwner(funnelmart.getOwner());

        if (isFunnel) {
            // Turn on pipeline flag
            pipeline.setPipelineType(FUNNEL);
            String userIdColumn = ((FunnelQuery) funnelmart).getUserIdColumn();
            List<Filter> steps = ((FunnelQuery) funnelmart).getSteps();
            List<String> stepNames = ((FunnelQuery) funnelmart).getStepNames();
            String startDate = ((FunnelQuery) funnelmart).getStartDate();
            String queryRange = ((FunnelQuery) funnelmart).getQueryRange();
            String repeatInterval = ((FunnelQuery) funnelmart).getRepeatInterval();

            // Set user id colum
            pipeline.setFunnelUserIdField(userIdColumn);
            // Set steps Json to the pipeline
            if (steps != null && steps.size() > 0) {
                StringBuilder json = new StringBuilder("");
                for (Filter f: steps) {
                    json.append(Filter.toJson(f));
                    json.append(STEP_NAME_DELIMITER);
                }
                pipeline.setFunnelStepsJson(json.toString());
            }

            // Set step names
            if (stepNames != null && stepNames.size() > 0) {
                StringBuilder name = new StringBuilder("");
                for (String s: stepNames) {
                    name.append(s);
                    name.append(STEP_NAME_DELIMITER);
                }
                pipeline.setFunnelStepNames(name.toString());
            }

            // Get the query hql
            String funnelHql = Utils.createAndValidateFunnelQuery(jsonReq, true, isPartOfFunnelGroup);
            pipeline.setFunnelHql(funnelHql);

            // Set backfill
            Date date = new SimpleDateFormat("yyyyMMdd").parse(startDate);;
            String datestr = new SimpleDateFormat("yyyy-MM-dd").format(date);
            pipeline.setPipelineBackfillStartTime(datestr);

            // Set end time
            pipeline.setPipelineEndTime(null);

            // Set funnel query range
            pipeline.setFunnelQueryRange(queryRange);

            // Set repeat interval
            pipeline.setFunnelRepeatInterval(repeatInterval);

            // Add new projections to pipeline
            List<PipelineProjection> newProjections = new ArrayList<>();
            // Add the projections
            newProjections.addAll(Utils.buildProjections(funnelmart.getProjections(), true));
            if (isUpdate) {
                log.info("Current projections are: {}", pipeline.getProjections());
                log.info("New projections are: {}", funnelmart.getProjections());
            }
            // Validate the new projections
            String duplicateColumnName = Utils.checkListForDuplicates(newProjections.stream().map(p -> p.getAlias()).collect(Collectors.toList()));
            if (duplicateColumnName != null) {
                throw new Exception("Duplicate column name " + duplicateColumnName);
            }
            // Set projections
            pipeline.setProjections(newProjections);

        } else {
            // Add new projections to pipeline
            List<PipelineProjection> newProjections = new ArrayList<>();
            // Add the projections
            newProjections.addAll(Utils.buildProjections(funnelmart.getProjections(), true));
            // Add value alias for each projection
            if (funnelmart.getProjectionVMs() != null) {
                for (int i = 0; i < funnelmart.getProjectionVMs().size(); i++) {
                    if (newProjections.get(i) != null) {
                        newProjections.get(i).setProjectionVMs(funnelmart.getProjectionVMs().get(i));
                    }
                }
            }
            if (isUpdate) {
                log.info("Current projections are: {}", pipeline.getProjections());
                log.info("New projections are: {}", funnelmart.getProjections());
            }
            // Validate the new projections
            String duplicateColumnName = Utils.checkListForDuplicates(newProjections.stream().map(p -> p.getAlias()).collect(Collectors.toList()));
            if (duplicateColumnName != null) {
                throw new Exception("Duplicate column name " + duplicateColumnName);
            }
            // Set projections
            pipeline.setProjections(newProjections);

            // Set backfill
            if (funnelmart.isBackfillEnabled()) {
                pipeline.setPipelineBackfillStartTime(funnelmart.getBackfillStartDate());
            } else {
                pipeline.setPipelineBackfillStartTime(null);
            }

            // Set end time
            if (funnelmart.isEndTimeEnabled()) {
                pipeline.setPipelineEndTime(funnelmart.getEndTimeDate());
            } else {
                // Use default if not provided
                pipeline.setPipelineEndTime(null);
            }
        }

        // Set filter Json to the pipeline
        if (funnelmart.getFilter() != null) {
            pipeline.setPipelineFilterJson(Filter.toJson(funnelmart.getFilter()));
            pipeline.setPipelineFilterObject(funnelmart.getFilter().toModel(funnelmart.getSchemaName()));
        } else {
            pipeline.setPipelineFilterJson(null);
            pipeline.setPipelineFilterObject(null);
        }


        // Set update time
        pipeline.setPipelineEditTime(new SimpleDateFormat(CREATE_DATE_TEMPLATE).format(new Date()));
        log.info("Set update time to " + pipeline.getPipelineEditTime());

        if (!isUpdate) {
            // Set create time
            pipeline.setPipelineCreateTime(new SimpleDateFormat(CREATE_DATE_TEMPLATE).format(new Date()));
        }

        return pipeline;
    }

    /**
     * Construct funnel group for creating / updating.
     */
    public static FunnelGroup constructFunnelGroup(String jsonReq, long funnelGroupId) throws Exception {
        boolean isUpdate = funnelGroupId != -1;
        ObjectMapper mapper = new ObjectMapper();
        Funnelmart funnelmart = isUpdate ? mapper.readValue(jsonReq, UpdateFunnelGroupQuery.class) : mapper.readValue(jsonReq, NewFunnelGroupQuery.class);

        // Validate the JSON object
        String jsonValidateErrorMsg = funnelmart.isValid();
        if (jsonValidateErrorMsg != null) {
            throw new Exception(jsonValidateErrorMsg);
        }

        // Build the funnel group
        FunnelGroup funnelGroup = isUpdate ? ServiceFactory.funnelGroupService().fetch(funnelGroupId) : new FunnelGroup();

        String funnelGroupName;

        if (!isUpdate) {
            // Log the request JSON
            log.info("JSON: {}", funnelmart.toString());
            // funnelGroupName: force lowercase funnel group name
            funnelGroupName = ((NewFunnelGroupQuery) funnelmart).getName().toLowerCase();
            funnelGroup.setFunnelGroupName(funnelGroupName);
            // status: start as INACTIVE when create new pipeline.
            funnelGroup.setFunnelGroupStatus(Status.INACTIVE);
            // version: version 1 by default
            funnelGroup.setFunnelGroupVersion(1L);
            // funnelGroupSchemaName: set schema name for the pipeline
            funnelGroup.setFunnelGroupSchemaName(funnelmart.getSchemaName());
        } else {
            // Log the request JSON
            log.info("Funnel group {} update JSON: {}", funnelGroupId, funnelmart.toString());
            // version
            funnelGroup.setFunnelGroupVersion(funnelGroup.getFunnelGroupVersion() + 1L);
            // funnelGroupName
            funnelGroupName = funnelGroup.getFunnelGroupName();
        }

        // funnelGroupDescription
        String funnelGroupDescription = funnelmart.getDescription();
        funnelGroup.setFunnelGroupDescription(funnelGroupDescription);

        // funnelGroupOwner
        funnelGroup.setFunnelGroupOwner(funnelmart.getOwner());

        // funnelGroupTopology
        funnelGroup.setFunnelGroupTopology(((FunnelGroupQuery) funnelmart).getTopology());

        // userIdColumn, steps, stepNames, startDate, queryRange, repeatInterval
        String userIdColumn = ((FunnelGroupQuery) funnelmart).getUserIdColumn();
        List<LogicalRule> steps = ((FunnelGroupQuery) funnelmart).getSteps();
        List<String[]> stepNames = ((FunnelGroupQuery) funnelmart).getStepNames();
        String startDate = ((FunnelGroupQuery) funnelmart).getStartDate();
        String queryRange = ((FunnelGroupQuery) funnelmart).getQueryRange();
        String repeatInterval = ((FunnelGroupQuery) funnelmart).getRepeatInterval();

        // Set user id column
        funnelGroup.setFunnelGroupUserIdField(userIdColumn);

        // Set steps Json to funnel group
        // Create map for step json, with step name as key, json as value
        Map<String, String> stepsMap = new HashMap<>();

        if (steps != null && steps.size() > 0) {
            StringBuilder json = new StringBuilder("");
            for (LogicalRule rule: steps) {
                String stepName = rule.getName();
                String filterJson = Filter.toJson(rule);
                stepsMap.put(stepName, filterJson);
                json.append(filterJson);
                json.append(STEP_NAME_DELIMITER);
            }
            funnelGroup.setFunnelGroupStepsJson(json.toString());
        }

        // Set step names, serialized in the format [[from1,to1],[from2,to2]]
        if (stepNames != null && stepNames.size() > 0) {
            List<String> name = new ArrayList<>();
            for (String[] s: stepNames) {
                name.add("[" + s[0] + "," + s[1] + "]");
            }
            funnelGroup.setFunnelGroupStepNames("[" + String.join(",", name) + "]");
        }

        // Calculate step names for each funnel based on graph traversal
        List<String> stepNamesForEachFunnel = generateStepNamesForEachFunnel(stepNames);

        // Set backfill
        Date date = new SimpleDateFormat("yyyyMMdd").parse(startDate);;
        String datestr = new SimpleDateFormat("yyyy-MM-dd").format(date);
        funnelGroup.setFunnelGroupBackfillStartTime(datestr);

        // Set end time
        funnelGroup.setFunnelGroupEndTime(null);

        // Set funnel group query range
        funnelGroup.setFunnelGroupQueryRange(queryRange);

        // Set repeat interval
        funnelGroup.setFunnelGroupRepeatInterval(repeatInterval);

        // Add new projections to funnel group
        List<Map<String, String>> projections = funnelmart.getProjections();
        List<PipelineProjection> newProjections = new ArrayList<>(Utils.buildProjections(projections, true));
        if (isUpdate) {
            log.info("Current projections are: {}", funnelGroup.getProjections());
            log.info("New projections are: {}", funnelmart.getProjections());
        }
        // Validate the new projections
        String duplicateColumnName = Utils.checkListForDuplicates(newProjections.stream().map(p -> p.getAlias()).collect(Collectors.toList()));
        if (duplicateColumnName != null) {
            throw new Exception("Duplicate column name " + duplicateColumnName);
        }
        // Set projections
        funnelGroup.setProjections(newProjections);

        // Set filter Json to funnel group
        if (funnelmart.getFilter() != null) {
            funnelGroup.setFunnelGroupFilterJson(Filter.toJson(funnelmart.getFilter()));
            funnelGroup.setFunnelGroupFilterObject(funnelmart.getFilter().toModel(funnelmart.getSchemaName()));
        } else {
            funnelGroup.setFunnelGroupFilterJson(null);
            funnelGroup.setFunnelGroupFilterObject(null);
        }

        // Set update time
        funnelGroup.setFunnelGroupEditTime(new SimpleDateFormat(CREATE_DATE_TEMPLATE).format(new Date()));
        log.info("Set update time to " + funnelGroup.getFunnelGroupEditTime());

        if (!isUpdate) {
            // Set create time
            funnelGroup.setFunnelGroupCreateTime(new SimpleDateFormat(CREATE_DATE_TEMPLATE).format(new Date()));
        }

        // Funnel names
        String userSpecifiedFunnelNames = ((FunnelGroupQuery) funnelmart).getFunnelNames();
        TypeReference<HashMap<String, String>> typeRef = new TypeReference<HashMap<String, String>>() { };
        Map<String, String> funnelNames = mapper.readValue(userSpecifiedFunnelNames, typeRef);

        // Propagate attributes to each pipeline and add pipelines to funnel group
        List<Pipeline> pipelines = new ArrayList<>();
        for (String funnelStepNames: stepNamesForEachFunnel) {

            String[] funnelStepNamesSplit = funnelStepNames.split(STEP_NAME_DELIMITER);

            // Set funnel name
            String pipelineName;
            String funnelId = String.join(DASH_DELIMITER, funnelStepNamesSplit);
            String userDefinedFunnelName = funnelNames.get(funnelId);

            if (userDefinedFunnelName == null) {
                throw new Exception("Unable to find user defined funnel name for funnel: " + funnelId);
            }

            pipelineName = funnelGroupName + UNDERSCORE_DELIMITER + userDefinedFunnelName;

            // Set funnel steps
            List<String> funnelStepsJson = new ArrayList<>();
            for (String step: funnelStepNamesSplit) {
                String stepJson = stepsMap.get(step);
                if (stepJson != null) {
                    funnelStepsJson.add(stepJson);
                }
            }

            String funnelJsonReq;
            if (!isUpdate) {
                funnelJsonReq = getFunnelJsonReq(jsonReq, pipelineName, funnelStepNames, funnelStepsJson, funnelGroupDescription);
            } else {
                String nameHeader = "{\"name\": \"" + funnelGroupName + "\",";
                funnelJsonReq = getFunnelJsonReq(nameHeader + jsonReq.replaceFirst("\\{", ""), pipelineName, funnelStepNames, funnelStepsJson, funnelGroupDescription);
            }

            Pipeline pipeline = Utils.constructFunnelmartPipeline(funnelJsonReq, -1, true, true);
            pipeline.setProductName(CLISettings.INSTANCE_NAME);

            pipelines.add(pipeline);
        }

        funnelGroup.setFunnelNames(mapper.writeValueAsString(funnelNames));
        funnelGroup.setPipelines(pipelines);

        return funnelGroup;
    }

    /**
     * Launch pipeline.
     */
    public static String launchPipeline(long pipelineId) throws Exception {
        // Fetch the pipeline by id
        Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);

        // Fetch schema info
        Schema schema = ServiceFactory.schemaService().fetchByName(pipeline.getPipelineSchemaName());

        // Get date string
        String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        log.info("Version {} for pipeline {}", date, pipeline.getPipelineName());

        // Convert to long
        long version = Long.parseLong(date);
        String outputDir = Routes.datamartTemplateGenerator.generateTemplateFiles(pipeline, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);

        log.info("Generated template for {}.v{} at location {}", pipeline.getPipelineName(), date, outputDir);

        // Setup launch scripts
        log.info("Starting launch for {}.v{}...", pipeline.getPipelineName(), date);

        // Backfill job config
        String backfillStartDate = DEFAULT_BACKFILL_START_DATE_VALUE; // the initial is no backfill
        if (pipeline.getPipelineBackfillStartTime() != null && !pipeline.getPipelineBackfillStartTime().isEmpty()) {
            backfillStartDate = pipeline.getPipelineBackfillStartTime();
        }
        log.info("BACKFILL: {}", backfillStartDate);

        // Used for checking if Bullet saw records during its run
        boolean bulletHasNoResults = false;
        // Count the number of dimension columns
        long numberOfDimensions = pipeline.getProjections().
                stream().
                map(projection -> projection.isAggregation()).
                filter(p -> p == false).
                count();
        log.info("Number of dimension columns: {}", numberOfDimensions);

        // Only check cardinality with Bullet if we have 1 or more dimensions.
        // If set in CLI option, skip Bullet validation.
        if (numberOfDimensions > 0 && !schema.getSchemaDisableBullet()) {
            CardinalityEstimationService cardinalityEstimationService = ServiceFactory.cardinalityEstimationService();
            CardinalityEstimationService.Response queryResult = cardinalityEstimationService.sendBulletQuery(pipeline);
            if (queryResult.statusCode != HttpStatus.SC_OK) {
                String errMsg = String.format("Failed to send the bullet query with response code [%d]", queryResult.statusCode);
                log.error(errMsg);
                throw new Exception(errMsg);
            }
            if (!queryResult.aggregationValues.containsKey(BulletQuery.AGGREGATION_TYPE)) {
                String errMsg = String.format("Failed to fetch the value of aggregation '%s' via Bullet query.", BulletQuery.AGGREGATION_TYPE);
                log.error(errMsg);
                throw new Exception(errMsg);
            }

            // Check Cardinality Cap
            double cardinalityNum = queryResult.aggregationValues.get(BulletQuery.AGGREGATION_TYPE);
            // Cardinality upper bounds
            if (cardinalityNum > CLISettings.CARDINALITY_CAP) {
                String errMsg = String.format("Pipeline %s CANNOT be launched because it has cardinality %s, which exceeds the cap %s. Please modify the filter condition or remove projections.", pipeline.getPipelineName(), cardinalityNum, CLISettings.CARDINALITY_CAP);
                log.error(errMsg);
                throw new Exception(errMsg);
            }
            // Cardinality lower bounds
            if (cardinalityNum < 1.0) {
                bulletHasNoResults = true;
                String errMsg = String.format("Pipeline %s has no results from Bullet.", pipeline.getPipelineName());
                log.error(errMsg);
            }
        }

        final String oozieJobType = schema.getSchemaOozieJobType();
        final String oozieBackfillJobType = schema.getSchemaOozieBackfillJobType();

        // Initialize pipeline launcher
        Future<PipelineLauncher.LaunchStatus> status = Routes.pipelineLauncherManager.launchPipeline(pipeline.getPipelineName(), date, CLISettings.PIPELINE_OWNER , outputDir, backfillStartDate, false, oozieJobType, oozieBackfillJobType);

        // Launch the pipeline and get the status
        log.info("Launch had error: {}", status.get().hasError);
        log.info("Launch error message: {}", status.get().errorMsg);
        log.info("Launch info message: {}", status.get().infoMsg);
        log.info("Oozie job id: {}", status.get().oozieJobId);

        // If error, inform user
        if (status.get().hasError) {
            throw new Exception(status.get().errorMsg);
        }

        // Successful launch
        // Mark pipeline as launched
        log.info("Setting pipeline {} as launched", pipelineId);
        pipeline.setPipelineStatus(String.format(Status.ACTIVE, Long.toString(pipeline.getPipelineVersion())));
        log.info("Setting the Oozie job id {} for pipeline {}", status.get().oozieJobId, pipelineId);
        pipeline.setPipelineOozieJobId(status.get().oozieJobId);
        log.info("Oozie backfill job id: {}", status.get().oozieJobIdOfBackfill == null ? "N/A" : status.get().oozieJobIdOfBackfill);
        // backfill job id
        if (status.get().oozieJobIdOfBackfill != null) {
            log.info("Setting the Oozie backfill job id {} for pipeline {}", status.get().oozieJobIdOfBackfill, pipelineId);
            pipeline.setPipelineOozieBackfillJobId(status.get().oozieJobIdOfBackfill);
        } else {
            log.info("No backfill job. Setting the Oozie backfill job id is skipped.");
        }

        ServiceFactory.pipelineService().update(pipeline);

        if (bulletHasNoResults) {
            return "No results found in Bullet for your pipeline, try adjusting your filters and projections. The pipeline has still been launched.";
        }

        return "";
    }

    /**
     * Launch funnel group.
     */
    public static String launchFunnelGroup(long funnelGroupId) throws Exception {
        // fetch the funnel group by id
        FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);
        String funnelGroupName = funnelGroup.getFunnelGroupName();

        // fetch schema info
        Schema schema = ServiceFactory.schemaService().fetchByName(funnelGroup.getFunnelGroupSchemaName());

        // get date string
        String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        log.info("Version {} for funnel group {}", date, funnelGroupName);

        // use date as version
        long version = Long.parseLong(date);

        // backfill job config
        String backfillStartDate = DEFAULT_BACKFILL_START_DATE_VALUE; // the initial is no backfill
        if (funnelGroup.getFunnelGroupBackfillStartTime() != null && !funnelGroup.getFunnelGroupBackfillStartTime().isEmpty()) {
            backfillStartDate = funnelGroup.getFunnelGroupBackfillStartTime();
        }
        log.info("BACKFILL: {}", backfillStartDate);

        final String oozieJobType = schema.getSchemaOozieJobType();
        final String oozieBackfillJobType = schema.getSchemaOozieBackfillJobType();

        // declare the launching of funnel group
        log.info("Starting launch for funnel group {}.v{}...", funnelGroupName, date);

        Routes.funnelGroupTemplateGenerator.setDownload(false);
        String outputDir = Routes.funnelGroupTemplateGenerator.generateTemplateFiles(funnelGroup, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);
        log.info("Generated template for {}.v{} at location {}", funnelGroupName, date, outputDir);

        Future<PipelineLauncher.LaunchStatus> status = Routes.pipelineLauncherManager.launchPipeline(
                funnelGroupName,
                date,
                CLISettings.PIPELINE_OWNER,
                outputDir,
                backfillStartDate,
                true,
                oozieJobType,
                oozieBackfillJobType);

        log.info("Launch had error: {}", status.get().hasError);
        log.info("Launch error message: {}", status.get().errorMsg);
        log.info("Launch info message: {}", status.get().infoMsg);
        log.info("Oozie job id: {}", status.get().oozieJobId);

        // inform the user if the funnel group launch has error
        if (status.get().hasError) {
            throw new Exception(status.get().errorMsg);
        }

        // mark each pipeline within the funnel group as launched
        for (Pipeline shallowPipeline : funnelGroup.getPipelines()) {
            long pipelineId = shallowPipeline.getPipelineId();
            Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);
            pipeline.setPipelineStatus(String.format(Status.ACTIVE, Long.toString(pipeline.getPipelineVersion())));
            ServiceFactory.pipelineService().update(pipeline);
        }

        // funnel group successfully launched, mark it as launched
        log.info("Setting funnel group name: {}, id: {} as launched", funnelGroupName, funnelGroupId);
        funnelGroup.setFunnelGroupStatus(String.format(Status.ACTIVE, Long.toString(funnelGroup.getFunnelGroupVersion())));
        log.info("Setting the Oozie job id {} for funnel group {}", status.get().oozieJobId, funnelGroupId);
        funnelGroup.setFunnelGroupOozieJobId(status.get().oozieJobId);

        ServiceFactory.funnelGroupService().update(funnelGroup);

        return "";
    }

    /**
     * Generate pipeline zip file.
     */
    public static String zipPipeline(long pipelineId) throws Exception {
        // Fetch the data mart by id
        Pipeline pipeline = ServiceFactory.pipelineService().fetch(pipelineId);

        // Get date string
        String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        log.info("Version {} for pipeline {}", date, pipeline.getPipelineName());

        // Convert to long
        long version = Long.parseLong(date);
        String outputDir = Routes.datamartTemplateGenerator.generateTemplateFiles(pipeline, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);

        log.info("Generated template for {}.v{} at location {}", pipeline.getPipelineName(), date, outputDir);

        // Zip pipeline
        String zipOutFile = outputDir.substring(0, outputDir.length() - 1) + ".zip";
        log.info("Zipping {} to {}.zip", outputDir, zipOutFile);
        ZipUtil.zipDir(zipOutFile, outputDir);

        return zipOutFile;
    }

    /**
     * Generate funnel group zip file.
     */
    public static String zipFunnelGroup(long funnelGroupId) throws Exception {
        FunnelGroup funnelGroup = ServiceFactory.funnelGroupService().fetch(funnelGroupId);

        Routes.funnelGroupTemplateGenerator.setDownload(true);

        // Get date string
        String date = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        log.info("Version {} for funnel group {}", date, funnelGroup.getFunnelGroupName());

        // Convert to long
        long version = Long.parseLong(date);
        String outputDir = Routes.funnelGroupTemplateGenerator.generateTemplateFiles(funnelGroup, CLISettings.TEMPLATE_OUTPUT_FOLDER, version);

        log.info("Generated template for {}.v{} at location {}", funnelGroup.getFunnelGroupName(), date, outputDir);

        // Zip funnel group
        String zipOutFile = outputDir.substring(0, outputDir.length() - 1) + ".zip";
        log.info("Zipping {} to {}.zip", outputDir, zipOutFile);
        ZipUtil.zipDir(zipOutFile, outputDir);

        return zipOutFile;

    }

    /**
     * Generate step names json for each funnel from funnel group's edge list.
     * Outputs are unique paths from all 0-in-degree nodes to all 0-out-degree nodes.
     * They are steps for each funnel contained in the funnel group.
     * @param stepNames format: [[from1, to1], [from2, to2], ...]
     * @return format: [[step1, step2, step3, ...], [stepa, stepb, stepc, ...], ...]
     */
    public static List<String> generateStepNamesForEachFunnel(List<String[]> stepNames) {

        // Traverse the edge list, to build adjacency list, and to calculate in and out degrees for each node
        Map<String, List<String>> adjList = new HashMap<>();
        Map<String, int[]> inNOut = new HashMap<>(); // key: [in-degree, out-degree]

        for (String[] edge : stepNames) {
            String fromNode = edge[0];
            String toNode = edge[1];

            // Add to adjacency list
            if (!adjList.containsKey(fromNode)) {
                adjList.put(fromNode, new ArrayList<>(Collections.singletonList(toNode)));
            } else {
                adjList.get(fromNode).add(toNode);
            }

            // Increment out-degree
            if (!inNOut.containsKey(fromNode)) {
                inNOut.put(fromNode, new int[]{0, 1});
            } else {
                inNOut.get(fromNode)[1] += 1;
            }

            // Increment in-degree
            if (!inNOut.containsKey(toNode)) {
                inNOut.put(toNode, new int[]{1, 0});
            } else {
                inNOut.get(toNode)[0] += 1;
            }
        }

        // Find all nodes with zero in-degree and zero out-degree
        LinkedList<String> zeroInDegreeNodes = new LinkedList<>();
        Set<String> zeroOutDegreeNodes = new HashSet<>();
        for (Map.Entry<String, int[]> entry : inNOut.entrySet()) {
            String node = entry.getKey();
            int[] degrees = entry.getValue();
            if (degrees[0] == 0) { // zero in-degree
                zeroInDegreeNodes.add(node);
            }
            if (degrees[1] == 0) { // zero out-degree
                zeroOutDegreeNodes.add(node);
            }
        }

        // Traverse from all zero-in-degree nodes to all zero-out-degree nodes
        LinkedList<LinkedList<String>> deque = new LinkedList<>();

        // Adding all zero in-degree nodes into the deque
        while (!zeroInDegreeNodes.isEmpty()) {
            deque.offerLast(new LinkedList(Collections.singletonList(zeroInDegreeNodes.pollFirst())));
        }

        // Construct all the funnels
        List<LinkedList<String>> funnels = new ArrayList<>();
        while (!deque.isEmpty()) {
            LinkedList<String> list = deque.pollFirst();
            String node = list.peekLast();
            for (String neighbor : adjList.get(node)) {
                LinkedList<String> copiedList = copyLinkedList(list);
                copiedList.offerLast(neighbor);
                if (zeroOutDegreeNodes.contains(neighbor)) {
                    // If the last node has zero out-degree, meaning it's end of a funnel, adding to result
                    funnels.add(copiedList);
                } else {
                    // If the last node has out-degree, putting it back to deque
                    deque.offerLast(copiedList);
                }
            }
        }

        // Represent linked list funnels as string
        List<String> strFunnels = new ArrayList<>();
        for (LinkedList<String> funnel : funnels) {
            if (funnel.getFirst().equals(FUNNEL_START_FLAG)) {
                funnel.removeFirst();
            }
            strFunnels.add(String.join(STEP_NAME_DELIMITER, funnel));
        }

        return strFunnels;
    }

    /**
     * Get individual funnel json request string.
     * @param funnelGroupJsonReq json request string of funnel group
     * @param name name of the funnel
     * @param stepNames step names of the funnel, String in the format "STEP1\n\nSTEP2\n\nSTEP3"
     * @param steps steps of the funnel
     * @return the json request string of the funnel
     */
    public static String getFunnelJsonReq(String funnelGroupJsonReq, String name, String stepNames, List<String> steps, String description) throws IOException {

        ObjectMapper mapper = new ObjectMapper();
        ObjectNode rootNode = (ObjectNode) mapper.readTree(funnelGroupJsonReq);

        // Replace stepNames
        String[] stepNamesSplit = stepNames.split(STEP_NAME_DELIMITER);
        ArrayNode stepNamesNode = rootNode.putArray(STEP_NAMES_JSON_KEY);
        for (String stepName : stepNamesSplit) {
            stepNamesNode = stepNamesNode.add(stepName); // add() returns this ArrayNode (chaining)
        }

        // Replace steps
        ArrayNode stepsNode = rootNode.putArray(STEPS_JSON_KEY);
        for (String stepJson : steps) {
            stepsNode = stepsNode.addPOJO(mapper.readValue(stepJson, LogicalRule.class));
        }

        // Replace name, description
        rootNode.put(NAME_JSON_KEY, name);
        rootNode.put(DESCRIPTION_JSON_KEY, description);

        // Remove topology and funnel names
        rootNode.remove(TOPOLOGY_JSON_KEY);
        rootNode.remove(FUNNEL_NAMES_JSON_KEY);

        return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(rootNode);
    }

    private static LinkedList<String> copyLinkedList(LinkedList<String> list) {
        LinkedList<String> copiedList = new LinkedList<>();
        for (String node : list) {
            copiedList.offerLast(node);
        }
        return copiedList;
    }

    /**
     * Check whether a regular text json field (e.g. name, owner) is valid.
     * @return Error message or null.
     */
    public static String validateRegularTextJsonField(String fieldContent, String fieldName) {
        if (fieldContent == null) {
            return "Missing " + fieldName;
        }
        if (!REGULAR_TEXT_FIELD_PATTERN.matcher(fieldContent).find()) {
            return "Invalid " + fieldName;
        }
        return null;
    }

    /**
     * Check whether the funnel names json field is valid.
     * @return Error message or null.
     */
    public static String validateFunnelNamesJsonField(String fieldContent) {
        if (fieldContent == null) {
            return "Missing funnel names";
        }
        if (!FUNNEL_NAMES_PATTERN.matcher(fieldContent).find()) {
            return "Invalid funnel names";
        }
        return null;
    }

    /**
     * Check whether a file name is valid.
     * @return Error message or null.
     */
    public static String validateFileName(String fieldContent) {
        if (fieldContent == null) {
            return "Missing the file name.";
        }
        if (!FILE_NAME_PATTERN.matcher(fieldContent).find()) {
            return "Invalid file name";
        }
        return null;
    }
}
