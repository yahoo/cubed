/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import com.yahoo.cubed.json.filter.Filter;
import com.yahoo.cubed.model.filter.PipelineFilter;

import javax.persistence.Entity;
import javax.persistence.Table;
import javax.persistence.Id;
import javax.persistence.Column;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.OneToMany;
import javax.persistence.FetchType;
import javax.persistence.Transient;

import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;

import java.util.List;
import java.util.ArrayList;

import lombok.Getter;
import lombok.Setter;

/**
 * The Funnel Group object.
 */
@Entity
@Table(name = "funnel_group")
public class FunnelGroup implements AbstractModel {

    /** Funnel group ID. */
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    @Column(name = "funnel_group_id", nullable = false)
    @Getter @Setter
    private long funnelGroupId;

    /** Funnel group name. */
    @Column(name = "funnel_group_name", nullable = false)
    @Getter @Setter
    private String funnelGroupName;

    /** Product Name. */
    @Column(name = "product_name")
    @Getter @Setter
    private String productName = "cubed";

    /** Funnel group owner. */
    @Column(name = "funnel_group_owner")
    @Getter @Setter
    private String funnelGroupOwner;

    /** Funnel group description. */
    @Column(name = "funnel_group_description")
    @Getter @Setter
    private String funnelGroupDescription;

    /** A set of pipelines that the funnel group has. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "funnelGroupId", cascade = {}, fetch = FetchType.LAZY)
    @Getter
    private List<Pipeline> pipelines;

    /** List of projections. All funnels in a funnel group should have same projections. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "funnelGroupId", cascade = {}, fetch = FetchType.LAZY)
    @Getter
    private List<PipelineProjection> projections;

    /** Funnel group user id field. Same across all funnels in group. */
    @Column(name = "funnel_group_user_id_field")
    @Getter @Setter
    private String funnelGroupUserIdField;

    /** Funnel group repeat interval. Same across all funnels in group. */
    @Column(name = "funnel_group_repeat_interval")
    @Getter @Setter
    private String funnelGroupRepeatInterval;

    /** Funnel group query range. Same across all funnels in group. */
    @Column(name = "funnel_group_query_range")
    @Getter @Setter
    private String funnelGroupQueryRange;

    /*
     * Funnel group step names.
     * Step names are ordered by a graph traversal, from all in-degree = 0 nodes to all out-degree = 0 nodes.
     * The sequence is unique. For nodes in the same level, sorting alphabetically.
     */
    @Column(name = "funnel_group_step_names")
    @Getter @Setter
    private String funnelGroupStepNames;

    /** Funnel group step JSONs. The same order as step names. */
    @Column(name = "funnel_group_steps_json")
    @Getter @Setter
    private String funnelGroupStepsJson;

    /** Funnel group oozie job id. */
    @Column(name = "funnel_group_oozie_job_id")
    @Getter @Setter
    private String funnelGroupOozieJobId;

    /** Funnel group oozie job status. */
    @Column(name = "funnel_group_oozie_job_status")
    @Getter @Setter
    private String funnelGroupOozieJobStatus;

    /** Funnel group deleted status. */
    @Column(name = "funnel_group_is_deleted")
    private boolean funnelGroupIsDeleted;

    /*
     * Funnel group create time.
     * If this field value is null/empty, then there is no end time.
     */
    @Column(name = "funnel_group_create_time")
    @Getter @Setter
    private String funnelGroupCreateTime;

    /*
     * Funnel group edit time.
     * If this field value is null/empty, then there is no edit time.
     */
    @Column(name = "funnel_group_edit_time")
    @Getter @Setter
    private String funnelGroupEditTime;

    /*
     * Funnel group end time.
     * If this field value is null/empty, then there is no end time.
     */
    @Column(name = "funnel_group_end_time")
    @Getter @Setter
    private String funnelGroupEndTime;

    /*
     * Funnel group backfill start time.
     * If this field value is null/empty, it means no backfill.
     */
    @Column(name = "funnel_group_backfill_start_time")
    @Getter @Setter
    private String funnelGroupBackfillStartTime;

    /** Funnel group version. */
    @Column(name = "funnel_group_version")
    @Setter
    private Long funnelGroupVersion;

    /*
     * Funnel group schema name.
     * All funnels in the same group must have the same schema.
     * However this restriction could be relaxed in future versions as long as aggregations are the same.
     * But it introduces dev complexity.
     */
    @Column(name = "funnel_group_schema_name")
    @Getter @Setter
    private String funnelGroupSchemaName;

    /** Funnel group status. */
    @Column(name = "funnel_group_status")
    @Getter @Setter
    private String funnelGroupStatus;

    /** Funnel group filter JSON string. Same across all funnels in group. */
    @Column(name = "funnel_group_filter_json")
    @Getter @Setter
    private String funnelGroupFilterJson;

    /** Funnel group topology string. Used to reconstruct the graph. */
    @Column(name = "funnel_group_topology")
    @Getter @Setter
    private String funnelGroupTopology;

    /** User specified funnel names. */
    @Column(name = "funnel_names")
    @Getter @Setter
    private String funnelNames;

    /** Get filters. */
    @Transient
    public PipelineFilter funnelGroupFilterObject;

    /**
     * Get funnel group version.
     */
    public long getFunnelGroupVersion() {
        if (funnelGroupVersion == null) {
            return 1L;
        }
        return this.funnelGroupVersion;
    }

    /**
     * Set projections for the funnel group.
     */
    public void setProjections(List<PipelineProjection> projections) {
        for (PipelineProjection p : projections) {
            p.setFunnelGroupId(this.funnelGroupId);
        }
        this.projections = projections;
    }

    /**
     * Set pipelines for the funnel group.
     */
    public void setPipelines(List<Pipeline> pipelines) {
        for (Pipeline p : pipelines) {
            p.setFunnelGroupId(this.funnelGroupId);
        }
        this.pipelines = pipelines;
    }

    /**
     * Get funnel group filter object, transforming from json to filter object.
     */
    public PipelineFilter getFunnelGroupFilterObject() {
        if (funnelGroupFilterObject == null && funnelGroupFilterJson != null) {
            try {
                funnelGroupFilterObject = Filter.fromJson(funnelGroupFilterJson).toModel(funnelGroupSchemaName);
            } catch (Exception e) {
                return null;
            }
        }
        return funnelGroupFilterObject;
    }

    /**
     * Set funnel group filter.
     */
    public void setFunnelGroupFilterObject(PipelineFilter pipelineFilterObject) {
        this.funnelGroupFilterObject = pipelineFilterObject;
    }

    /**
     * Check if funnel group backfill is enabled.
     */
    public boolean isFunnelGroupBackfillEnabled() {
        return funnelGroupBackfillStartTime != null && !funnelGroupBackfillStartTime.isEmpty();
    }

    /**
     * Check if funnel group end time is enabled.
     */
    public boolean isFunnelGroupEndTimeEnabled() {
        return funnelGroupEndTime != null && !funnelGroupEndTime.isEmpty();
    }

    /**
     * Set if funnel group should be marked as deleted.
     */
    public void setFunnelGroupIsDeleted(boolean isFunnelGroupDeleted) {
        this.funnelGroupIsDeleted = isFunnelGroupDeleted;
    }

    /**
     * Check if funnel group is marked as deleted.
     */
    public boolean getFunnelGroupIsDeleted() {
        return funnelGroupIsDeleted;
    }

    /**
     * Get funnel group id.
     */
    public long getPrimaryIdx() {
        return this.getFunnelGroupId();
    }

    /**
     * Simplify pipeline.
     */
    public FunnelGroup simplify() {
        FunnelGroup newFunnelGroup = new FunnelGroup();
        newFunnelGroup.funnelGroupId = this.funnelGroupId;
        newFunnelGroup.funnelGroupName = this.funnelGroupName;
        newFunnelGroup.productName = this.productName;
        newFunnelGroup.funnelGroupOwner = this.funnelGroupOwner;
        newFunnelGroup.funnelGroupDescription = this.funnelGroupDescription;
        newFunnelGroup.funnelGroupFilterJson = this.funnelGroupFilterJson;
        newFunnelGroup.funnelGroupUserIdField = this.funnelGroupUserIdField;
        newFunnelGroup.funnelGroupRepeatInterval = this.funnelGroupRepeatInterval;
        newFunnelGroup.funnelGroupQueryRange = this.funnelGroupQueryRange;
        newFunnelGroup.funnelGroupStepNames = this.funnelGroupStepNames;
        newFunnelGroup.funnelGroupStepsJson = this.funnelGroupStepsJson;
        newFunnelGroup.funnelGroupOozieJobId = this.funnelGroupOozieJobId;
        newFunnelGroup.funnelGroupOozieJobStatus = this.funnelGroupOozieJobStatus;
        newFunnelGroup.funnelGroupIsDeleted = this.funnelGroupIsDeleted;
        newFunnelGroup.funnelGroupCreateTime = this.funnelGroupCreateTime;
        newFunnelGroup.funnelGroupEditTime = this.funnelGroupEditTime;
        newFunnelGroup.funnelGroupEndTime = this.funnelGroupEndTime;
        newFunnelGroup.funnelGroupBackfillStartTime = this.funnelGroupBackfillStartTime;
        newFunnelGroup.funnelGroupVersion = this.funnelGroupVersion;
        newFunnelGroup.funnelGroupSchemaName = this.funnelGroupSchemaName;
        newFunnelGroup.funnelGroupStatus = this.funnelGroupStatus;
        newFunnelGroup.funnelGroupTopology = this.funnelGroupTopology;

        return newFunnelGroup;
    }

    /**
     * Simplify list of funnel groups.
     */
    public static List<FunnelGroup> simplifyFunnelGroupList(List<FunnelGroup> funnelGroups) {
        if (funnelGroups == null) {
            return null;
        }
        List<FunnelGroup> newFunnelGroups = new ArrayList<>();
        for (FunnelGroup group : funnelGroups) {
            newFunnelGroups.add(group.simplify());
        }
        return newFunnelGroups;
    }

    /**
     * Get pipeline name.
     */
    @Override
    public String getPrimaryName() {
        return this.getFunnelGroupName();
    }
}
