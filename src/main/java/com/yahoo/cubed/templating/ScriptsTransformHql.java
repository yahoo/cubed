/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.PipelineProjectionVM;
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
            ret.append(iter.next().getField().strOfSimpleName());
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
        List<PipelineProjectionVM> vas = projection.getProjectionVMs();
        String defaultVMAlias = projection.getDefaultVMAlias();
        if (vas != null && vas.size() > 0) {
            // Add CASE WHEN statement for value mapping pairs
            StringBuilder caseStatement = new StringBuilder("\n\tCASE");
            for (PipelineProjectionVM va : vas) {
                caseStatement.append("\n\t\tWHEN ");
                caseStatement.append(fieldName);
                caseStatement.append(" = '");
                caseStatement.append(va.getFieldValue());
                caseStatement.append("' THEN '");
                caseStatement.append(va.getFieldValueMapping());
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
}
