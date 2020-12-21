/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.model.filter.PipelineFilter;
import java.util.ArrayList;
import java.util.List;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.FetchType;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.OneToMany;
import javax.persistence.Table;
import javax.persistence.Transient;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

/**
 * Stores pipeline logic.
 * Projections, filters, name, etc.
 */
@Entity
@Table(name = "pipeline")
public class Pipeline implements AbstractModel {
    /** Pipeline ID. */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "pipeline_id", nullable = false)
    @Getter @Setter
    private long pipelineId;

    /** Product Name. */
    @Column(name = "product_name")
    @Getter @Setter
    private String productName = "cubed";

    /** Pipeline type. */
    @Column(name = "pipeline_type")
    @Getter @Setter
    private String pipelineType = "datamart";

    /** Funnel query hql. */
    @Column(name = "funnel_hql")
    @Getter @Setter
    private String funnelHql;

    /** Funnel steps. */
    @Column(name = "funnel_steps")
    @Getter @Setter
    private String funnelStepsJson;

    /** Funnel steps names. */
    @Column(name = "funnel_step_names")
    @Getter @Setter
    private String funnelStepNames;

    /** Funnel query range. */
    @Column(name = "funnel_query_range")
    @Getter @Setter
    private String funnelQueryRange;

    /** Funnel repeat interval. */
    @Column(name = "funnel_repeat_interval")
    @Getter @Setter
    private String funnelRepeatInterval;

    /** Funnel user id field. */
    @Column(name = "funnel_user_id_field")
    @Getter @Setter
    private String funnelUserIdField;

    /** Pipeline name. */
    @Column(name = "pipeline_name", nullable = false)
    @Getter @Setter
    private String pipelineName;

    /** Pipeline description. */
    @Column(name = "pipeline_description", nullable = false)
    @Getter @Setter
    private String pipelineDescription;

    /** Pipeline owner. */
    @Column(name = "pipeline_owner")
    @Getter @Setter
    private String pipelineOwner;

    /** List of projections. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "pipelineId", cascade = {}, fetch = FetchType.LAZY)
    @Getter
    private List<PipelineProjection> projections;

    /** Pipeline filter JSON string. */
    @Column(name = "pipeline_filter")
    @Getter @Setter
    private String pipelineFilterJson;

    /** Pipeline status. */
    @Column(name = "pipeline_status")
    @Getter @Setter
    private String pipelineStatus;

    /** Pipeline oozie job id. */
    @Column(name = "pipeline_oozie_job_id")
    @Getter @Setter
    private String pipelineOozieJobId;

    /** Pipeline oozie job status. */
    @Column(name = "pipeline_oozie_job_status")
    @Getter @Setter
    private String pipelineOozieJobStatus;

    /** Pipeline oozie backfill job id. */
    @Column(name = "pipeline_oozie_backfill_job_id")
    @Getter @Setter
    private String pipelineOozieBackfillJobId;

    /** Pipeline oozie backfill status. */
    @Column(name = "pipeline_oozie_backfill_job_status")
    @Getter @Setter
    private String pipelineOozieBackfillJobStatus;

    /** Pipeline deleted status. */
    @Column(name = "pipeline_is_deleted")
    private boolean pipelineIsDeleted;

    /*
     * Pipeline backfill start time.
     * If this field value is null/empty, it means no backfill
     */
    @Column(name = "pipeline_backfill_start_time")
    @Getter @Setter
    private String pipelineBackfillStartTime;

    /*
     * Pipeline end time.
     * If this field value is null/empty, then there is no end time.
     */
    @Column(name = "pipeline_end_time")
    @Getter @Setter
    private String pipelineEndTime;

    /*
     * Pipeline create time.
     * If this field value is null/empty, then there is no end time.
     */
    @Column(name = "pipeline_create_time")
    @Getter @Setter
    private String pipelineCreateTime;

    /*
     * Pipeline edit time.
     * If this field value is null/empty, then there is no end time.
     */
    @Column(name = "pipeline_edit_time")
    @Getter @Setter
    private String pipelineEditTime;

    /** Get filters. */
    @Transient
    public PipelineFilter pipelineFilterObject;

    /** Pipeline version. */
    @Column(name = "pipeline_version")
    @Setter
    private Long pipelineVersion;

    /** Pipeline schema name. */
    @Column(name = "pipeline_schema_name")
    @Getter @Setter
    private String pipelineSchemaName;

    /**
     * Id of the funnel group that this pipeline belongs to.
     */
    @Getter @Setter
    @Column(name = "funnel_group_id")
    private long funnelGroupId;

    /**
     * Get pipeline version.
     */
    public long getPipelineVersion() {
        if (pipelineVersion == null) {
            return 1L;
        }
        return this.pipelineVersion;
    }

    /**
     * Set projections.
     */
    public void setProjections(List<PipelineProjection> projections) {
        for (PipelineProjection p : projections) {
            p.setPipelineId(this.pipelineId);
        }
        this.projections = projections;
    }

    /**
     * Get pipeline filter.
     */
    public PipelineFilter getPipelineFilterObject() {
        if (pipelineFilterObject == null && pipelineFilterJson != null) {
            try {
                pipelineFilterObject = Filter.fromJson(pipelineFilterJson).toModel(pipelineSchemaName);
            } catch (Exception e) {
                // TODO: do some logging here
                return null;
            }
        }
        return pipelineFilterObject;
    }

    /**
     * Set pipeline filter.
     */
    public void setPipelineFilterObject(PipelineFilter pipelineFilterObject) {
        this.pipelineFilterObject = pipelineFilterObject;
    }

    /**
     * Check if pipeline backfill is enabled.
     */
    public boolean isPipelineBackfillEnabled() {
        return this.pipelineBackfillStartTime != null && !this.pipelineBackfillStartTime.isEmpty();
    }

    /**
     * Check if pipeline backfill is enabled.
     */
    public boolean isPipelineEndTimeEnabled() {
        return this.pipelineEndTime != null && !this.pipelineEndTime.isEmpty();
    }

    /**
     * Set if pipeline should be marked as deleted.
     */
    public void setPipelineIsDeleted(boolean isPipelineDeleted) {
        this.pipelineIsDeleted = isPipelineDeleted;
    }

    /**
     * Check if pipeline is marked as deleted.
     */
    public boolean getPipelineIsDeleted() {
        return this.pipelineIsDeleted;
    }

    /**
     * Get pipeline database id.
     */
    public long getPrimaryIdx() {
        return this.getPipelineId();
    }

    /**
     * Simplify pipeline.
     */
    public Pipeline simplify() {
        Pipeline newPipeline = new Pipeline();
        newPipeline.pipelineId = this.pipelineId;
        newPipeline.pipelineType = this.pipelineType;
        newPipeline.funnelStepsJson = this.funnelStepsJson;
        newPipeline.funnelStepNames = this.funnelStepNames;
        newPipeline.funnelQueryRange = this.funnelQueryRange;
        newPipeline.funnelHql = this.funnelHql;
        newPipeline.funnelRepeatInterval = this.funnelRepeatInterval;
        newPipeline.funnelUserIdField = this.funnelUserIdField;
        newPipeline.pipelineName = this.pipelineName;
        newPipeline.pipelineDescription = this.pipelineDescription;
        newPipeline.pipelineFilterJson = this.pipelineFilterJson;
        newPipeline.pipelineOwner = this.pipelineOwner;
        newPipeline.pipelineStatus = this.pipelineStatus;
        newPipeline.pipelineOozieJobId = this.pipelineOozieJobId;
        newPipeline.pipelineOozieJobStatus = this.pipelineOozieJobStatus;
        newPipeline.pipelineOozieBackfillJobId = this.pipelineOozieBackfillJobId;
        newPipeline.pipelineOozieBackfillJobStatus = this.pipelineOozieBackfillJobStatus;
        newPipeline.pipelineBackfillStartTime = this.pipelineBackfillStartTime;
        newPipeline.pipelineIsDeleted = this.pipelineIsDeleted;
        newPipeline.pipelineCreateTime = this.pipelineCreateTime;
        newPipeline.pipelineEditTime = this.pipelineEditTime;
        newPipeline.pipelineVersion = this.pipelineVersion;
        newPipeline.pipelineSchemaName = this.pipelineSchemaName;
        return newPipeline;
    }

    /**
     * Simplify list of pipelines.
     */
    public static List<Pipeline> simplifyPipelineList(List<Pipeline> pipelines) {
        if (pipelines == null) {
            return null;
        }
        List<Pipeline> newPipelines = new ArrayList<>();
        for (Pipeline pipeline : pipelines) {
            newPipelines.add(pipeline.simplify());
        }
        return newPipelines;
    }

    /**
     * Set funnel group ID for a list of pipelines.
     */
    public static void setFunnelGroupId(List<Pipeline> pipelines, long funnelGroupId) {
        if (pipelines == null) {
            return;
        }
        for (Pipeline pipeline : pipelines) {
            pipeline.setFunnelGroupId(funnelGroupId);
        }
    }

    /**
     * Get pipeline name.
     */
    @Override
    public String getPrimaryName() {
        return this.getPipelineName();
    }
}
