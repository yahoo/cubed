/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json.filter;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.yahoo.cubed.model.filter.PipelineLogicalRule;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilter;
import com.yahoo.cubed.service.bullet.model.filter.BulletQueryFilterLogicalRule;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import java.util.List;
import lombok.Data;

/**
 * Logical rule in filter.
 */
@Data
@JsonSerialize
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonDeserialize
public class LogicalRule extends Filter {

    private String name;
    private String condition;
    private List<Filter> rules;

    /**
     * Convert logical filter rule to rule model.
     */
    @Override
    public PipelineLogicalRule toModel(String schemaName) throws NumberFormatException, DataValidatorException, DatabaseException {
        PipelineLogicalRule ruleModel = new PipelineLogicalRule();
        ruleModel.setName(this.name);
        ruleModel.setCondition(this.condition);
        if (this.rules != null) {
            for (Filter rule : this.rules) {
                ruleModel.addRule(rule.toModel(schemaName));
            }
        }
        return ruleModel;
    }

    @Override
    protected BulletQueryFilter toBulletQueryFilterKernel()
            throws NumberFormatException, DataValidatorException, DatabaseException {
        BulletQueryFilterLogicalRule ruleModel = new BulletQueryFilterLogicalRule();
        ruleModel.setOperation(this.condition);
        if (this.rules != null) {
            for (Filter rule : this.rules) {
                BulletQueryFilter bulletRule = rule.toBulletQueryFilter();
                if (bulletRule != null) {
                    ruleModel.addClause(bulletRule);
                }
            }
        }
        if (ruleModel.clauses == null || ruleModel.clauses.isEmpty()) {
            return null; // no sub queries, just return null
        }
        return ruleModel;
    }
}
