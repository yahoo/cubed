/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.service;

import com.yahoo.cubed.dao.AbstractEntityDAO;
import com.yahoo.cubed.dao.DAOFactory;
import com.yahoo.cubed.model.Field;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.PipelineProjection;
import com.yahoo.cubed.model.filter.PipelineFilter;
import com.yahoo.cubed.model.filter.PipelineLogicalRule;
import com.yahoo.cubed.model.filter.PipelineRelationalRule;
import com.yahoo.cubed.service.exception.DataValidatorException;
import com.yahoo.cubed.service.exception.DatabaseException;
import com.yahoo.cubed.util.Constants;
import org.hibernate.Session;

/**
 * Funnel Group service implementation.
 */
public class FunnelGroupServiceImpl extends AbstractServiceImpl<FunnelGroup> implements FunnelGroupService {

    @Override
    protected AbstractEntityDAO<FunnelGroup> getDAO() {
        return DAOFactory.funnelGroupDAO();
    }

    /**
     * Validate projection.
     */
    protected void checkProjection(Session session, PipelineProjection projection) throws DataValidatorException {
        validateField(session, projection.getField());
    }

    private void validateField(Session session, Field projectedField) throws DataValidatorException {
        if (projectedField == null) {
            throw new DataValidatorException("A funnel group field projection should contain field information.");
        }
        if (projectedField.getFieldName() == null || projectedField.getFieldName().isEmpty()) {
            throw new DataValidatorException("A funnel group field projection should provide a field name.");
        }
        if (projectedField.getFieldId() <= 0) {
            throw new DataValidatorException("A funnel group field projection should provide a valid field id.");
        }
        Field field = DAOFactory.fieldDAO().fetchByCompositeKey(session, projectedField.getSchemaName(), projectedField.getFieldId());
        if (field == null) {
            throw new DataValidatorException("Field with id [" + projectedField.getFieldId() + "] does not exist.");
        }
        if (!field.getFieldName().equals(projectedField.getFieldName())) {
            throw new DataValidatorException("Field with id [" + projectedField.getFieldId() + "] does not match name [" + projectedField.getFieldName() + "].");
        }
    }

    /**
     * Check filter.
     */
    protected void checkFilter(Session session, PipelineFilter rule) throws DataValidatorException {
        if (rule instanceof PipelineLogicalRule) {
            checkLogicalRule(session, (PipelineLogicalRule) rule);
        } else if (rule instanceof PipelineRelationalRule) {
            checkRelationalRule(session, (PipelineRelationalRule) rule);
        }
    }

    /**
     * Check logical rules.
     */
    protected void checkLogicalRule(Session session, PipelineLogicalRule rule) throws DataValidatorException {
        if (!(Constants.DISJUNCTION.equals(rule.getCondition()) || Constants.CONJUNCTION.equals(rule.getCondition()))) {
            throw new DataValidatorException("A logical rule or rule group must have junction \"AND\" or \"OR\".");
        }
        if (rule.getRules() == null || rule.getRules().isEmpty()) {
            throw new DataValidatorException("A logical rule or rule group must contain a non-empty set of sub rules.");
        }
        for (PipelineFilter filter : rule.getRules()) {
            checkFilter(session, filter);
        }
    }

    /**
     * Check relational rules.
     */
    protected void checkRelationalRule(Session session, PipelineRelationalRule rule) throws DataValidatorException {
        this.validateField(session, rule.getField());

        if (rule.getOperator() == null) {
            throw new DataValidatorException("A pipeline relational rule should contain an operator.");
        }

        if (rule.getValue() == null) {
            throw new DataValidatorException("A pipeline relational rule should contain a value.");
        }
    }

    @Override
    protected void preSaveCheck(Session session, FunnelGroup model) throws DataValidatorException {

        super.preSaveCheck(session, model);

        if (model.getProjections() != null) {
            for (PipelineProjection projection : model.getProjections()) {
                this.checkProjection(session, projection);
            }
        }

        if (model.getFunnelGroupFilterObject() != null) {
            this.checkFilter(session, model.getFunnelGroupFilterObject());
        }
    }

    @Override
    protected FunnelGroup preUpdateCheck(Session session, FunnelGroup newModel) throws DataValidatorException {
        super.preUpdateCheck(session, newModel);

        String modelName = this.getDAO().getEntityClass().getSimpleName();
        // check if id valid
        if (newModel.getPrimaryIdx() <= 0) {
            throw new DataValidatorException("The id of the " + modelName + " is not provided.");
        }
        // check if id exists
        FunnelGroup oldModel = this.getDAO().fetch(session, newModel.getPrimaryIdx());
        if (oldModel == null) {
            throw new DataValidatorException("The " + modelName + " with id [" + newModel.getPrimaryIdx() + "] does not exist.");
        }
        // check name unique
        FunnelGroup modelWithSameName = this.getDAO().fetchByName(session, newModel.getPrimaryName());
        if (modelWithSameName != null && modelWithSameName.getPrimaryIdx() != newModel.getPrimaryIdx()) {
            throw new DataValidatorException("The name of the " + modelName + " is already used by another " + modelName + ".");
        }
        // check projectied fields (fields can be empty)
        if (newModel.getProjections() != null) {
            for (PipelineProjection projection : newModel.getProjections()) {
                this.checkProjection(session, projection);
            }
        }
        // check filter
        if (newModel.getFunnelGroupFilterObject() != null) {
            this.checkFilter(session, newModel.getFunnelGroupFilterObject());
        }
        return oldModel;
    }

    @Override
    public void save(FunnelGroup model) throws DataValidatorException, DatabaseException {
        super.save(model);
    }

    @Override
    public void update(FunnelGroup model) throws DataValidatorException, DatabaseException {
        super.update(model);
    }

    /**
     * Fetch entity by Id.
     */
    public FunnelGroup fetch(long id) throws  DataValidatorException, DatabaseException {
        Session session = this.createSession();

        FunnelGroup model = null;
        try {
            model = this.getDAO().fetch(session, id);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: cannot query " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + id + "].", e);
        } finally {
            this.reclaimSession(session);
        }

        if (model == null) {
            throw new DataValidatorException("Cannot find " + this.getDAO().getEntityClass().getSimpleName() + "  with id [" + id + "].");
        }

        return model;
    }

    /**
     * Delete entity.
     */
    public void delete(long id) throws DataValidatorException, DatabaseException {
        Session session = this.createSession();

        FunnelGroup model = this.preDeleteCheck(session, id);

        try {
            this.getDAO().delete(session, model);
        } catch (RuntimeException e) {
            throw new DatabaseException("Database exception: delete " + this.getDAO().getEntityClass().getSimpleName() + " failed.", e);
        } finally {
            this.reclaimSession(session);
        }
    }
}
