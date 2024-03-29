/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;
import com.yahoo.cubed.service.ServiceFactory;
import com.yahoo.cubed.util.Constants;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.antlr.stringtemplate.StringTemplate;

/**
 * Hive ETL template generator.
 */
public class ScriptsTransformHql implements TemplateFile<Pipeline> {
    // Template files
    private static final String HIVE_TRANSFORM_AND_FILTER_TEMPLATE_FILE = "transform_hql.st";

    // Attributes
    private static final String TRANSFORMATIONS_ATTRIBUTE = "transformations";
    private static final String FILTERS_ATTRIBUTE = "filters";
    private static final String GROUP_BY_ATTRIBUTE = "group_by_option";
    private static final String DATETIME_PARTITION_COLUMN_ATTRIBUTE = "datetime_partition_column";

    // Other settings
    private static final String GROUP_BY_KEYWORD = "GROUP BY";
    private static final String FILTER_DELIMITER = " AND ";

    // Null column filter
    private static final String NULL_COLUMN_FILTER = "%s IS NOT NULL AND\n";

    // Projection format for theta sketches. Coalesce to prevent nulls.
    private static final String THETA_SKETCH_PROJECTION = "COALESCE(base64(data_to_sketch(%s, 2048, 1.0)), '')";

    // Projection format for strings. Remove characters that cause problems in Druid.
    private static final String STRING_CLEANUP_PROJECTION = "COALESCE(regexp_replace(%s, '[\\\\t\\\\r\\\\n]', ''), '')";

    // Projection format for sum aggregates.
    private static final String SUM_PROJECTION = "COALESCE(SUM(%s), 0)";

    // Projection format for standard columns
    private static final String STANDARD_PROJECTION = "%s";

    // Value mapping operators
    private static final String EQUAL_VM_OPERATOR = "equal";
    private static final String LIKE_VM_OPERATOR = "like";
    private static final String LIKE = "LIKE";
    private static final String EQUAL = "=";
    private static final String RLIKE = "RLIKE";

    @Override
    public String generateFile(Pipeline model, long version) throws Exception {
        StringTemplate template = new StringTemplate(TemplateUtils.getParametrizedTemplateAsString(TemplateUtils.OOZIE_TEMPLATES_DIR + HIVE_TRANSFORM_AND_FILTER_TEMPLATE_FILE));
        if (model.getProjections() != null) {
            String projections = getHqlString(model.getProjections(), Constants.COMMA_DELIMITER);

            template.setAttribute(TRANSFORMATIONS_ATTRIBUTE, projections);
        }

        if (model.getPipelineFilterObject() != null) {
            template.setAttribute(FILTERS_ATTRIBUTE, model.getPipelineFilterObject().prettyPrint() + FILTER_DELIMITER);
        }

        template.setAttribute(GROUP_BY_ATTRIBUTE, strOfGroupBy(model));
        template.setAttribute(DATETIME_PARTITION_COLUMN_ATTRIBUTE, ServiceFactory.schemaService().fetchByName(model.getPipelineSchemaName()).getSchemaDatetimePartitionColumn());

        return template.toString();
    }

    private String getHqlString(List<PipelineProjection> list, String delimiter) {
        return list.stream()
                .map(p -> this.convertProjection(p))
                .collect(Collectors.joining(delimiter + Constants.NEWLINE));
    }

    private String strOfGroupBy(Pipeline model) {
        List<PipelineProjection> projections = PipelineProjection.filterAggregation(model.getProjections());

        if (projections == null || projections.isEmpty()) {
            return "";
        }
        StringBuilder ret = new StringBuilder(GROUP_BY_KEYWORD);
        ret.append(" ");
        Iterator<PipelineProjection> iter = projections.iterator();
        while (iter.hasNext()) {
            PipelineProjection projection = iter.next();
            if (projection.getProjectionVMs() != null && projection.getProjectionVMs().size() > 0) {
                ret.append(getValueMappingProjectionQuery(projection));
            } else {
                ret.append(projection.getField().strOfSimpleName());
            }
            if (iter.hasNext()) {
                ret.append(Constants.COMMA_DELIMITER);
            }
        }
        return ret.toString();
    }

    /**
     * Convert a projection. Perform special logic for theta sketches and strings for Druid.
     * @param projection Column projection
     * @return String representation for Hive of the projection
     */
    private String convertProjection(PipelineProjection projection) {
        String fieldName = projection.getField().toString();

        if (projection.isThetaSketch()) {
            // Handle theta sketch format
            return String.format(THETA_SKETCH_PROJECTION, fieldName);
        } else if (projection.isSum()) {
            // Handle sum aggregate
            return String.format(SUM_PROJECTION, fieldName);
        }

        // Check for existing value mapping
        if (projection.getProjectionVMs() != null && projection.getProjectionVMs().size() > 0) {
            return getValueMappingProjectionQuery(projection);
        } else {
            // No value mapping exists
            if (Constants.STRING.equals(projection.getField().getFieldType())) {
                // String type projection, perform cleanup regex
                return String.format(STRING_CLEANUP_PROJECTION, fieldName);
            } else {
                // Else, do the standard projection
                return String.format(STANDARD_PROJECTION, fieldName);
            }
        }
    }

    /**
     * Get matching value mapping SQL operator.
     * @param operatorStr User specified operator in the front end and database
     * @return String corresponding SQL operator
     */
    private String getValueMappingOperator(String operatorStr) {
        return operatorStr.equals(EQUAL_VM_OPERATOR) ? EQUAL : operatorStr.equals(LIKE_VM_OPERATOR) ? LIKE : RLIKE;
    }

    /**
     * Get projection query that has value mapping.
     * @param projection A PipelineProjection object that contains value mapping
     * @return The formatted projection query
     */
    private String getValueMappingProjectionQuery(PipelineProjection projection) {
        List<PipelineProjectionVM> vms = projection.getProjectionVMs();
        String fieldName = projection.getField().toString();
        String defaultVMAlias = projection.getDefaultVMAlias();
        // Add CASE WHEN statement for value mapping pairs
        StringBuilder caseStatement = new StringBuilder("\n\tCASE");
        for (PipelineProjectionVM vm : vms) {
            caseStatement.append("\n\t\tWHEN ");
            caseStatement.append(fieldName);
            caseStatement.append(" ");
            caseStatement.append(getValueMappingOperator(vm.getOperator()));
            caseStatement.append(" '");
            caseStatement.append(vm.getFieldValue());
            caseStatement.append("' THEN '");
            caseStatement.append(vm.getFieldValueMapping());
            caseStatement.append("'");
        }
        if (defaultVMAlias != null) {
            caseStatement.append("\n\t\tELSE '");
            caseStatement.append(defaultVMAlias);
            caseStatement.append("'\n\tEND");
        } else {
            caseStatement.append("\n\t\tELSE ");
            caseStatement.append(fieldName);
            caseStatement.append("\n\tEND");
        }

        if (Constants.STRING.equals(projection.getField().getFieldType())) {
            // String type projection, perform cleanup regex
            return String.format(STRING_CLEANUP_PROJECTION, caseStatement.toString());
        } else {
            // Else, do the standard projection
            return String.format(STANDARD_PROJECTION, caseStatement.toString());
        }
    }
}
