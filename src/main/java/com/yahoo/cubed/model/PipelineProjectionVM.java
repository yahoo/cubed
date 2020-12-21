/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import lombok.Getter;
import lombok.Setter;

/**
 * Pipeline projection value mapping (VM).
 */
@Entity
@Table(name = "pipeline_projection_vm")
public class PipelineProjectionVM implements AbstractModel {

    /**
     * Id of pipeline projection value mapping.
     */
    @Getter @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "pipeline_projection_vm_id", nullable = false)
    private long pipelineProjectionVMId;

    /**
     * Id of pipeline projection.
     */
    @Getter @Setter
    @Column(name = "pipeline_projection_id", nullable = false)
    private long pipelineProjectionId;

    /**
     * Field name.
     */
    @Getter @Setter
    @Column(name = "field_name")
    private String fieldName;

    /**
     * Value of field.
     */
    @Getter @Setter
    @Column(name = "field_value")
    private String fieldValue;


    /**
     * Alias for the value of field.
     */
    @Getter @Setter
    @Column(name = "field_value_mapping")
    private String fieldValueMapping;


    /**
     * Get Primary Index.
     */
    public long getPrimaryIdx() {
        return this.getPipelineProjectionVMId();
    }


    /**
     * Not supported method.
     */
    @Override
    public String getPrimaryName() {
        return null;
    }
    
}
