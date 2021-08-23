/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import com.yahoo.cubed.util.Aggregation;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.JoinColumn;
import javax.persistence.OneToMany;
import javax.persistence.OneToOne;
import javax.persistence.Table;
import javax.persistence.Transient;
import javax.persistence.JoinColumns;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

/**
 * Pipeline projection.
 */
@Entity
@Table(name = "pipeline_projection")
public class PipelineProjection implements AbstractModel {
    /**
     * Id of pipeline projection.
     */
    @Getter @Setter
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "pipeline_projection_id", nullable = false)
    private long pipelineProjectionId;

    /**
     * Id of the pipeline that this projection belongs to.
     */
    @Getter @Setter
    @Column(name = "pipeline_id")
    private long pipelineId;

    /**
     * Id of the funnel group that this projection belongs to.
     */
    @Getter @Setter
    @Column(name = "funnel_group_id")
    private long funnelGroupId;

    /**
     * Entity of field used by this projection.
     */
    @Setter
    @OneToOne(cascade = {}, fetch = FetchType.EAGER)
    @JoinColumns({
            @JoinColumn(name = "schema_name", referencedColumnName = "schema_name"),
            @JoinColumn(name = "field_id", referencedColumnName = "field_id")
        })
    private Field field;

    /**
     * Alias of field.
     */
    @Getter @Setter
    @Column(name = "field_alias")
    private String alias;

    /** List of projections value mapping. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "pipelineProjectionId", cascade = {}, fetch = FetchType.EAGER)
    @Getter
    private List<PipelineProjectionVM> projectionVMs;

    /**
     * Default VM alias.
     */
    @Getter @Setter
    @Column(name = "default_vm_alias")
    private String defaultVMAlias;

    /**
     * Key (subfield) of field.
     */
    @Getter @Setter
    @Column(name = "field_key")
    private String key;

    /**
     * Aggregation name if this projection is aggregation.
     */
    @Getter @Setter
    @Column(name = "aggregation_name")
    private String aggregationName;

    /**
     * flag to show if projection is in original schema.
     */
    @Getter @Setter
    @Column(name = "is_in_original_schema")
    private boolean isInOriginalSchema;
    
    // for UI
    @Getter @Setter
    @Transient
    private String fieldNameId;

    /**
     * Get field.
     */
    public Field getField() {
        // set the key to the field
        field.setKey(this.getKey());
        return field;
    }

    /**
     * Get aggregation.
     */
    public Aggregation getAggregation() {
        return Aggregation.byName(this.aggregationName);
    }

    /**
     * Set aggregation.
     */
    public void setAggregation(Aggregation aggregation) {
        if (aggregation == null) {
            return;
        }
        this.aggregationName = aggregation.name;
    }

    /**
     * Set projection value mapping.
     */
    public void setProjectionVMs(List<List<String>> vas) {
        if (vas == null) {
            return;
        }

        if (this.projectionVMs == null) {
            this.projectionVMs = new ArrayList<>();
        }

        this.projectionVMs.clear();
        for (List<String> va: vas) {
            PipelineProjectionVM p = new PipelineProjectionVM();
            p.setFieldValue(va.get(0));
            p.setFieldValueMapping(va.get(1));
            p.setPipelineProjectionId(this.pipelineProjectionId);
            p.setFieldName(this.field.getFieldName());
            this.projectionVMs.add(p);
        }
    }

    /**
     * Check if pipeline is an aggregation.
     */
    public boolean isAggregation() {
        return this.getAggregation() != null;
    }

    /**
     * Check if projection is a theta sketch.
     * @return True if theta sketch, false otherwise
     */
    public boolean isThetaSketch() {
        return this.isAggregation() && this.getAggregation().name.equals(Aggregation.THETA_SKETCH.name);
    }

    /**
     * Check if projection is a sum.
     * @return True if sum, false otherwise
     */
    public boolean isSum() {
        return this.isAggregation() && this.getAggregation().name.equals(Aggregation.SUM.name);
    }

    /**
     * Get Primary Index.
     */
    public long getPrimaryIdx() {
        return this.getPipelineProjectionId();
    }

    /**
     * Set pipeline ID.
     */
    public static void setPipelineId(List<PipelineProjection> projections, long pipelineId) {
        if (projections == null) {
            return;
        }
        for (PipelineProjection projection : projections) {
            projection.setPipelineId(pipelineId);
        }
    }

    /**
     * Set funnel group ID for a list of pipeline projections.
     */
    public static void setFunnelGroupId(List<PipelineProjection> projections, long funnelGroupId) {
        if (projections == null) {
            return;
        }
        for (PipelineProjection projection : projections) {
            projection.setFunnelGroupId(funnelGroupId);
        }
    }
    
    /**
     * Get non-aggregation projections.
     */
    public static List<PipelineProjection> filterAggregation(List<PipelineProjection> projections) {
        List<PipelineProjection> ret = new ArrayList<>();
        if (projections == null) {
            return ret;
        }
        for (PipelineProjection projection : projections) {
            if (!projection.isAggregation()) {
                ret.add(projection);
            }
        }
        return ret;
    }
    
    /**
     * Check if pipeline has aggregation.
     */
    public static boolean hasAggregation(List<PipelineProjection> projections) {
        if (projections == null) {
            return false;
        }
        for (PipelineProjection projection : projections) {
            if (projection.isAggregation()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Not supported method.
     */
    @Override
    public String getPrimaryName() {
        return null;
    }


    /**
     * Serialize projection object to string.
     */
    @Override
    public String toString() {
        StringBuilder projBuilder = new StringBuilder(this.getField().strOfSimpleName());

        Aggregation aggregation = this.getAggregation();

        if (aggregation != null) {
            String nested = projBuilder.toString();
            String aggregationStr = aggregation.toString(nested);
            projBuilder = new StringBuilder(aggregationStr);
        }

        if (this.getAlias() != null) {
            projBuilder.append(" AS ");
            projBuilder.append(this.getAlias());
        }
        return projBuilder.toString();
    }
}
