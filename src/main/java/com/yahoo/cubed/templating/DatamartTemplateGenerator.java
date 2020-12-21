/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.model.Schema;
import com.yahoo.cubed.service.ServiceFactory;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.AbstractMap.SimpleImmutableEntry;
import java.util.Map;

/**
 * Funnelmart template generator. Builds complete pipeline.
 */
public class DatamartTemplateGenerator extends TemplateGenerator<Pipeline> {
    private String outputDir = "";
    /**
     * Generates pipeline. Including Oozie, Hive, Druid and shell scripts.
     * @param model Pipeline to generate
     * @param outputPath Location for generated pipeline
     * @param version Pipeline version
     */
    public String generateTemplateFiles(Pipeline model, String outputPath, long version) throws Exception {
        // Initialize schema info
        Schema schema = ServiceFactory.schemaService().fetchByName(model.getPipelineSchemaName());
        final String oozieJobType = schema.getSchemaOozieJobType();
        final String oozieBackfillJobType = schema.getSchemaOozieBackfillJobType();

        // Create the output directories
        outputDir = String.format("%s/%s_v%s/", outputPath, model.getPipelineName(), version);
        String emailSubfolder = outputDir + "email/";
        String scriptSubfolder = outputDir + "scripts/";
        String regularFolder = null;
        String backfillFolder = null;

        // set oozie regular job folder
        if (oozieJobType.equals("hourly")) {
            regularFolder = outputDir + "regular_hour/";
        } else if (oozieJobType.equals("daily")) {
            regularFolder = outputDir + "regular_day/";
        } else {
            throw new Exception("Please specify oozie job type, daily or hourly.");
        }

        // set oozie backfill job folder
        if (oozieBackfillJobType.equals("hourly")) {
            backfillFolder = outputDir + "backfill_hour/";
        } else if (oozieBackfillJobType.equals("daily")) {
            backfillFolder = outputDir + "backfill_day/";
        } else {
            throw new Exception("Please specify oozie backfill job type, daily or hourly.");
        }
        
        // create directories
        List<String> folders = null;
        if (regularFolder != null && backfillFolder != null) {
            folders = Arrays.asList(outputDir, emailSubfolder, scriptSubfolder, regularFolder, backfillFolder);
        } else {
            throw new Exception("Please specify oozie job and oozie backfill job type, daily or hourly.");
        }

        createDirectoryStructure(folders);

        // Create all static files
        List<Map.Entry<String , String>> staticPipelineFiles = new ArrayList<>();

        staticPipelineFiles.add(new SimpleImmutableEntry<>("email_workflow.xml", emailSubfolder + "workflow.xml"));

        if (oozieJobType.equals("hourly")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("bundle_hourly.xml", outputDir + "bundle.xml"));
        } else if (oozieJobType.equals("daily")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("bundle_daily.xml", outputDir + "bundle.xml"));
        } else {
            throw new Exception("Please specify valid oozie job type, daily or hourly.");
        }

        if (oozieBackfillJobType.equals("hourly")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("bundle_backfill_hourly.xml", outputDir + "bundle_backfill.xml"));
        } else if (oozieBackfillJobType.equals("daily")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("bundle_backfill_daily.xml", outputDir + "bundle_backfill.xml"));
        } else {
            throw new Exception("Please specify valid oozie backfill job type, daily or hourly.");
        }

        if (regularFolder.equals(outputDir + "regular_hour/")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("hour_job.xml", regularFolder + "job.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("hour_workflow.xml", regularFolder + "workflow.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("regular_hour_coordinator.xml", regularFolder + "coordinator.xml"));
        } else if (regularFolder.equals(outputDir + "regular_day/")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("day_job.xml", regularFolder + "job.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("day_workflow.xml", regularFolder + "workflow.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("regular_day_coordinator.xml", regularFolder + "coordinator.xml"));
        } else {
            throw new Exception("Please specify valid oozie backfill job type, daily or hourly.");
        }

        if (backfillFolder.equals(outputDir + "backfill_hour/")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("hour_job.xml", backfillFolder + "job.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("hour_workflow.xml", backfillFolder + "workflow.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("backfill_hour_coordinator.xml", backfillFolder + "coordinator.xml"));
        } else if (backfillFolder.equals(outputDir + "backfill_day/")) {
            staticPipelineFiles.add(new SimpleImmutableEntry<>("day_job.xml", backfillFolder + "job.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("day_workflow.xml", backfillFolder + "workflow.xml"));
            staticPipelineFiles.add(new SimpleImmutableEntry<>("backfill_day_coordinator.xml", backfillFolder + "coordinator.xml"));
        } else {
            throw new Exception("Please specify valid oozie backfill job type, daily or hourly.");
        }
        
        for (Map.Entry<String, String> f : staticPipelineFiles) {
            Files.copy(TemplateUtils.getResouceFileAsStream(TemplateUtils.STATIC_RESOURCE_DIR + f.getKey()), Paths.get(f.getValue()));
        }

        // Create all dynamic files
        List<Map.Entry<String, TemplateFile>> dynamicPipelineFiles = new ArrayList<>();
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(outputDir + "properties.xml", new BundlePropertiesXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(outputDir + "properties_backfill.xml", new BundlePropertiesBackfillXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(scriptSubfolder + "transform.hql", new ScriptsTransformHql()));

        if (oozieJobType.equals("hourly")) {
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(regularFolder + "index.json", new DruidIndexJson(true)));
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(regularFolder + "druid_load.sh", new DruidLoadSh(true)));
        } else if (oozieJobType.equals("daily")) {
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(regularFolder + "index.json", new DruidIndexJson(false)));
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(regularFolder + "druid_load.sh", new DruidLoadSh(false)));
        } else {
            throw new Exception("Please specify valid oozie job type, daily or hourly.");
        }

        if (oozieBackfillJobType.equals("hourly")) {
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(backfillFolder + "index.json", new DruidIndexJson(true)));
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(backfillFolder + "druid_load.sh", new DruidLoadSh(true)));
        } else if (oozieBackfillJobType.equals("daily")) {
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(backfillFolder + "index.json", new DruidIndexJson(false)));
            dynamicPipelineFiles.add(new SimpleImmutableEntry<>(backfillFolder + "druid_load.sh", new DruidLoadSh(false)));
        } else {
            throw new Exception("Please specify valid oozie backfill job type, daily or hourly.");
        }

        for (Map.Entry<String, TemplateFile> file : dynamicPipelineFiles) {
            Files.write(Paths.get(file.getKey()), file.getValue().generateFile(model, version).getBytes());
        }

        return outputDir;
    }
}
