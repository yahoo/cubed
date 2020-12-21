/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.json.FunnelQuery;
import com.yahoo.cubed.json.NewFunnelQuery;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.util.Constants;
import lombok.extern.slf4j.Slf4j;
import org.antlr.stringtemplate.StringTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Hive ETL template generator.
 */
@Slf4j
public class FunnelHql {
    // Template files
    private static final String FUNNEL_QUERY_TEMPLATE_FILE = "funnel_query_hql.st";
    private static final String FUNNEL_QUERY_PIPELINE_TEMPLATE_FILE = "funnel_query_hql_pipeline.st";
    private static final String FUNNEL_QUERY_PIPELINE_TEMPLATE_FILE_FUNNEL_GROUP = "funnel_query_hql_pipeline_funnel_group.st";
    // Attributes
    private static final String NUM_STEPS_ATTRIBUTE = "NUM_STEPS";
    private static final String STEPS_ATTRIBUTE = "STEPS";
    private static final String TIMESTAMP_COLUMN_ATTRIBUTE = "TIMESTAMP_COLUMN";
    private static final String START_DATE_ATTRIBUTE = "START_DATE";
    private static final String END_DATE_ATTRIBUTE = "END_DATE";
    private static final String USER_ID_COLUMN_ATTRIBUTE = "USER_ID_COLUMN";
    private static final String FILTER_ATTRIBUTE = "FILTERS";
    private static final String STEP_FILTERS_ATTRIBUTE = "STEP_FILTERS";
    private static final String OUTER_SELECT_PROJECTIONS_ATTRIBUTE = "OUTER_SELECT_PROJECTIONS";
    private static final String INNER_SELECT_PROJECTIONS_ATTRIBUTE = "INNER_SELECT_PROJECTIONS";
    private static final String FINAL_PROJECTIONS_ATTRIBUTE = "FINAL_PROJECTIONS";
    private static final String FINAL_PROJECTIONS_WITH_TYPE_ATTRIBUTE = "FINAL_PROJECTIONS_WITH_TYPE";
    private static final String FINAL_PROJECTIONS_GROUP_BY_ATTRIBUTE = "FINAL_PROJECTIONS_GROUP_BY";
    private static final String FINAL_PROJECTIONS_COALESCE_ATTRIBUTE = "FINAL_PROJECTIONS_COALESCE";
    private static final String GENERATE_SKETCHES_ATTRIBUTE = "GENERATE_SKETCHES";
    private static final String IN_DATABASE_ATTRIBUTE = "IN_DATABASE";
    private static final String TARGET_TABLE_ATTRIBUTE = "TARGET_TABLE";
    private static final String WINDOW_FUNCTIONS_ATTRIBUTE = "WINDOW_FUNCTIONS";

    // Constants
    private static final String FILTER_DELIMITER = " \n ";
    private static final String COMMA_DELIMITER = ",";
    private static final String WINDOW_FUNCTION_DELIMITER = ",\n";
    private static final String THEN_DELIMITER = " THEN ";
    private static final String WHEN_DELIMITER = " WHEN ";
    private static final String AND_DELIMITER = " AND ";
    private static final String OR_DELIMITER = " OR ";
    private static final String FUNNEL_START = "FUNNEL_START";

    // Templates
    private static final String OUTER_PROJECTION_TEMPLATE = "funnel_first(%s) AS %s";
    private static final String INNER_PROJECTION_TEMPLATE = "%s AS %s";
    private static final String FINAL_PROJECTION_GROUP_BY_TEMPLATE = "GROUP BY %s";
    private static final String FINAL_PROJECTION_COALESCE_TEMPLATE = "COALESCE(regexp_replace(%s, '[\\\\t\\\\r\\\\n]', ''), 'null')";
    private static final String CASTING_TEMPLATE = "CAST(%s AS STRING)";
    private static final String GENERATE_SKETCH_TEMPLATE = "insert into sketch_intermediate select %s '%s', unionSketches(sketch, 262144) from theta_input where step >= %d %s;\n";
    private static final String GENERATE_SKETCH_TEMPLATE_FUNNEL_GROUP = "insert into sketch_intermediate select %s '%s', '%s', '%s', unionSketches(sketch, 262144) from theta_input where step >= %d %s;\n";
    private static final String WINDOW_FUNCTION_TEMPLATE = "FIRST_VALUE((IF(step == 0, %s, null)), TRUE) OVER (PARTITION BY user_id ORDER BY ts ASC) AS %s";
    /**
     * Generate Hive query string from template.
     * @param model
     * @return
     * @throws Exception
     */
    public static String generateFile(NewFunnelQuery model, boolean isPipeline, boolean isFunnelGroup) throws Exception {
        if (model.isValid() != null) {
            return null;
        }

        StringTemplate template;
        if (!isPipeline) {
            template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.FUNNEL_TEMPLATE_DIR + FUNNEL_QUERY_TEMPLATE_FILE));
        } else {
            if (!isFunnelGroup) {
                template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.FUNNEL_TEMPLATE_DIR + FUNNEL_QUERY_PIPELINE_TEMPLATE_FILE));
            } else {
                template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.FUNNEL_TEMPLATE_DIR + FUNNEL_QUERY_PIPELINE_TEMPLATE_FILE_FUNNEL_GROUP));
            }
        }

        Schema schema = ServiceFactory.schemaService().fetchByName(model.getSchemaName());

        template.setAttribute(NUM_STEPS_ATTRIBUTE, IntStream.range(0, model.getSteps().size())
                                                            .mapToObj(Integer::toString)
                                                            .collect(Collectors.joining(COMMA_DELIMITER)));
        template.setAttribute(TIMESTAMP_COLUMN_ATTRIBUTE, schema.getSchemaTimestampColumnParam());
        template.setAttribute(USER_ID_COLUMN_ATTRIBUTE, model.getUserIdColumn());
        template.setAttribute(START_DATE_ATTRIBUTE, model.getStartDate());
        template.setAttribute(END_DATE_ATTRIBUTE, model.getEndDate());
        List<String> hiveQueryList = new ArrayList<>();
        List<String> stepsFilter = new ArrayList<>();
        for (int i = 0; i < model.getSteps().size(); i++) {
            String step =  "(" + model.getSteps().get(i).toModel(model.getSchemaName()).prettyPrint() + ")";
            hiveQueryList.add(WHEN_DELIMITER + step + THEN_DELIMITER + i);
            stepsFilter.add(step);
        }
        String steps = hiveQueryList.stream().collect(Collectors.joining(FILTER_DELIMITER));
        template.setAttribute(STEPS_ATTRIBUTE, steps);
        String stepFilterStr = "(" + stepsFilter.stream().collect(Collectors.joining(OR_DELIMITER + FILTER_DELIMITER)) + ")";
        template.setAttribute(STEP_FILTERS_ATTRIBUTE, stepFilterStr + AND_DELIMITER);
        if (model.getFilter().toModel(model.getSchemaName()).prettyPrint().length() > 0) {
            template.setAttribute(FILTER_ATTRIBUTE, model.getFilter().toModel(model.getSchemaName()).prettyPrint() + AND_DELIMITER);
        }

        String finalProjectionsStr = "";
        String finalProjectionsStrWithType = "";
        String finalProjectionsGroupByStr = "";
        String finalProjectionsCoalesceStr = "";
        String outerSelectProjectionsStr = "";
        String innerSelectProjectionsStr = "";
        String windowFunctionsProjectionStr = "";

        if (model.getProjections().size() > 0) {
            // Attach projections
            finalProjectionsStr = model.getProjections().stream().map(m -> m.get(FunnelQuery.ALIAS)).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);
            finalProjectionsStrWithType = model.getProjections().stream().map(m -> (m.get(FunnelQuery.ALIAS) + " string")).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);
            finalProjectionsCoalesceStr = model.getProjections().stream().map(m -> (String.format(FINAL_PROJECTION_COALESCE_TEMPLATE, m.get(FunnelQuery.ALIAS)))).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);;
            finalProjectionsGroupByStr = String.format(FINAL_PROJECTION_GROUP_BY_TEMPLATE, model.getProjections().stream().map(m -> m.get(FunnelQuery.ALIAS)).collect(Collectors.joining(COMMA_DELIMITER)));
            finalProjectionsGroupByStr += ", funnels";

            outerSelectProjectionsStr = COMMA_DELIMITER.concat(model.getProjections().stream().map(m -> {
                    try {
                        return String.format(OUTER_PROJECTION_TEMPLATE, getFieldAndCast(m.get(FunnelQuery.SCHEMA_NAME), m.get(FunnelQuery.COLUMN_ID), m.get(FunnelQuery.ALIAS)), m.get(FunnelQuery.ALIAS));
                    } catch (Exception e) {
                        log.error("Error: ", e.getMessage());
                    }
                    return "";
                }).collect(Collectors.joining(COMMA_DELIMITER)));
            innerSelectProjectionsStr = model.getProjections().stream().map(m -> {
                    try {
                        String fieldName = ServiceFactory.fieldService().fetchByCompositeKey(m.get(FunnelQuery.SCHEMA_NAME), Long.parseLong(m.get(FunnelQuery.COLUMN_ID), 10)).getFieldName();
                        if (m.containsKey(FunnelQuery.KEY) && m.get(FunnelQuery.KEY) != null && m.get(FunnelQuery.KEY).length() > 0) {
                            fieldName = fieldName.concat("['" + m.get(FunnelQuery.KEY) + "']");
                        }
                        return String.format(INNER_PROJECTION_TEMPLATE, fieldName, m.get(FunnelQuery.ALIAS));
                    } catch (DataValidatorException e) {
                        log.error("Caught DataValidatorException while getting field information");
                    } catch (DatabaseException e) {
                        log.error("Caught DatabaseException while getting field information");
                    }
                    return "";
                }).collect(Collectors.joining(COMMA_DELIMITER)).concat(COMMA_DELIMITER);
            windowFunctionsProjectionStr = model.getProjections().stream().map(m ->
                    String.format(WINDOW_FUNCTION_TEMPLATE, m.get(FunnelQuery.ALIAS), m.get(FunnelQuery.ALIAS))
                ).collect(Collectors.joining(WINDOW_FUNCTION_DELIMITER)).concat(COMMA_DELIMITER);
        } else if (isPipeline) {
            finalProjectionsGroupByStr = String.format(FINAL_PROJECTION_GROUP_BY_TEMPLATE, "funnels");
        }

        template.setAttribute(FINAL_PROJECTIONS_ATTRIBUTE, finalProjectionsStr);
        template.setAttribute(FINAL_PROJECTIONS_GROUP_BY_ATTRIBUTE, finalProjectionsGroupByStr);
        template.setAttribute(INNER_SELECT_PROJECTIONS_ATTRIBUTE, innerSelectProjectionsStr);
        template.setAttribute(OUTER_SELECT_PROJECTIONS_ATTRIBUTE, outerSelectProjectionsStr);
        template.setAttribute(FINAL_PROJECTIONS_COALESCE_ATTRIBUTE, finalProjectionsCoalesceStr);
        template.setAttribute(WINDOW_FUNCTIONS_ATTRIBUTE, windowFunctionsProjectionStr);

        StringBuilder generateSketchesStr = new StringBuilder("");
        for (int i = 1; i <= model.getSteps().size(); i++) {
            String unionSketchGroupByStr = finalProjectionsStr == "" ? "" : "group by " + finalProjectionsStr.replaceAll(",$", "");;
            if (model.getStepNames().get(i - 1) != null) {
                if (isFunnelGroup) {
                    String prevStepName = i > 1 ? model.getStepNames().get(i - 2) : FUNNEL_START;
                    generateSketchesStr.append(String.format(GENERATE_SKETCH_TEMPLATE_FUNNEL_GROUP, finalProjectionsStr,  model.getStepNames().get(i - 1), prevStepName, model.getName(), i, unionSketchGroupByStr));
                } else {
                    generateSketchesStr.append(String.format(GENERATE_SKETCH_TEMPLATE, finalProjectionsStr,  model.getStepNames().get(i - 1), i, unionSketchGroupByStr));
                }
            } else {
                if (isFunnelGroup) {
                    String prevStepName = i > 1 ? "step" + (i - 1) : FUNNEL_START;
                    generateSketchesStr.append(String.format(GENERATE_SKETCH_TEMPLATE_FUNNEL_GROUP, finalProjectionsStr, "step" + i, prevStepName, model.getName(), i, unionSketchGroupByStr));
                } else {
                    generateSketchesStr.append(String.format(GENERATE_SKETCH_TEMPLATE, finalProjectionsStr, "step" + i, i, unionSketchGroupByStr));
                }
            }

        }
        template.setAttribute(GENERATE_SKETCHES_ATTRIBUTE, generateSketchesStr.toString());
        template.setAttribute(FINAL_PROJECTIONS_WITH_TYPE_ATTRIBUTE, finalProjectionsStrWithType);
        if (!isPipeline) {
            // Fetch schema info
            final String inDatabase = schema.getSchemaName();
            final String targetTable = schema.getSchemaTargetTable();
            template.setAttribute(IN_DATABASE_ATTRIBUTE, inDatabase);
            template.setAttribute(TARGET_TABLE_ATTRIBUTE, targetTable);
        }
        return template.toString();
    }

    private static String getFieldAndCast(String schemaName, String fieldId, String alias) throws Exception {
        Field field = ServiceFactory.fieldService().fetchByCompositeKey(schemaName, Long.parseLong(fieldId, 10));
        String fieldType = field.getFieldType();
        // if a map, check inner type to see if casting is necessary
        if (fieldType.startsWith("map")) {
            fieldType = fieldType.substring(fieldType.indexOf(",") + 1, fieldType.indexOf(">"));
        }
        if (!Constants.STRING.equals(fieldType)) {
            return String.format(CASTING_TEMPLATE, alias);
        }
        return alias;
    }
}
