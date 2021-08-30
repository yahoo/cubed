/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.model;

import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.hibernate.annotations.Fetch;
import org.hibernate.annotations.FetchMode;
import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.Id;
import javax.persistence.Table;
import javax.persistence.OneToMany;
import javax.persistence.FetchType;

/**
 * Schema, used in store schema yaml file.
 */
@Entity
@Table(name = "all_schemas")
public class Schema implements AbstractModel {

    /** Schema name. */
    @Id
    @Column(name = "schema_name", nullable = false)
    @Getter @Setter
    private String schemaName;

    /** Schema database name. */
    @Column(name = "schema_database", nullable = false)
    @Getter @Setter
    private String schemaDatabase;

    /** Schema tables. */
    @Column(name = "schema_tables", nullable = false)
    @Getter @Setter
    private String schemaTables;

    /** Schema oozie job type. */
    @Column(name = "schema_oozie_job_type", nullable = false)
    @Getter @Setter
    private String schemaOozieJobType;

    /** Schema oozie backfill job type. */
    @Column(name = "schema_oozie_backfill_job_type", nullable = false)
    @Getter @Setter
    private String schemaOozieBackfillJobType;


    /** Schema target table. */
    @Column(name = "schema_target_table")
    @Getter @Setter
    private String schemaTargetTable;

    /** Schema user id fields. */
    @Column(name = "schema_user_id_fields")
    @Getter @Setter
    private String schemaUserIdFields;

    /** Schema default filters. */
    @Column(name = "schema_default_filters")
    @Getter @Setter
    private String schemaDefaultFilters;

    /** Schema bullet url. */
    @Column(name = "schema_bullet_url")
    @Getter @Setter
    private String schemaBulletUrl;

    /** Schema disable bullet param, controlling bullet query module and explorer feature on/off. */
    @Column(name = "schema_disable_bullet")
    @Getter @Setter
    private Boolean schemaDisableBullet;

    /** Schema disable funnel param, controlling cubed feature on/off. */
    @Column(name = "schema_disable_funnel")
    @Getter @Setter
    private Boolean schemaDisableFunnel;

    /** Identify if a schema is deleted. If so, the schema is not available for selection. */
    @Column(name = "is_schema_deleted")
    @Getter @Setter
    private Boolean isSchemaDeleted;

    /** Schema parameter for the timestamp column filters. */
    @Column(name = "schema_timestamp_column_param")
    @Getter @Setter
    private String schemaTimestampColumnParam;

    /** Schema datetime partition column name, such as 'dt'. */
    @Column(name = "schema_datetime_partition_column")
    @Getter @Setter
    private String schemaDatetimePartitionColumn;

    /** Fields for maps. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "schemaName", cascade = {}, fetch = FetchType.EAGER)
    @Getter @Setter
    private List<Field> fields;

    /** Fields for maps. */
    @Fetch(value = FetchMode.SELECT)
    @OneToMany(mappedBy = "pipelineSchemaName", cascade = {}, fetch = FetchType.EAGER)
    @Getter @Setter
    private List<Pipeline> pipelines;
    /**
     * Get schema name.
     */
    @Override
    public String getPrimaryName() {
        return this.getSchemaName();
    }
}
