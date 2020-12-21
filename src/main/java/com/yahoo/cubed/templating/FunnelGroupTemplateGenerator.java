/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.model.Pipeline;
import com.yahoo.cubed.settings.CLISettings;

import java.nio.file.Files;
import java.nio.file.Paths;

import java.util.Map;
import java.util.List;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.AbstractMap.SimpleImmutableEntry;

import lombok.Getter;
import lombok.Setter;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

/**
 * Funnel group template generator. Builds complete pipeline.
 */
public class FunnelGroupTemplateGenerator extends TemplateGenerator<FunnelGroup> {

    /** Is for download. */
    @Getter @Setter
    private boolean download;

    /**
     * Generates funnel group files. Including Oozie, Hive, Druid and shell scripts.
     * @param model funnel group to generate
     * @param outputPath location for generated funnel group
     * @param version funnel group version
     */
    public String generateTemplateFiles(FunnelGroup model, String outputPath, long version) throws Exception {

        // Create the output directories
        String outputDir = String.format("%s/%s_v%s/", outputPath, model.getFunnelGroupName(), version);
        String emailSubfolder = outputDir + "email/";
        String scriptSubfolder = outputDir + "scripts/";
        String funnelFolder = outputDir + "funnel_group/";
        String libFolder = outputDir + "funnel_group/lib/";
        List<String> folders = Arrays.asList(outputDir, emailSubfolder, scriptSubfolder, funnelFolder, libFolder);
        createDirectoryStructure(folders);

        // Create all static files
        List<Map.Entry<String , String>> staticPipelineFiles = new ArrayList<>();
        staticPipelineFiles.add(new SimpleImmutableEntry<>("email_workflow.xml", emailSubfolder + "workflow.xml"));
        staticPipelineFiles.add(new SimpleImmutableEntry<>("day_job.xml", funnelFolder + "job.xml"));

        for (Map.Entry<String, String> f : staticPipelineFiles) {
            Files.copy(TemplateUtils.getResouceFileAsStream(TemplateUtils.STATIC_RESOURCE_DIR + f.getKey()), Paths.get(f.getValue()));
        }

        // Create all funnel group dynamic files
        List<Map.Entry<String, TemplateFile>> dynamicPipelineFiles = new ArrayList<>();

        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(outputDir + "properties.xml", new BundlePropertiesFunnelGroupXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(outputDir + "bundle.xml", new FunnelGroupBundleXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(funnelFolder + "workflow.xml", new FunnelGroupWorkflowXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(funnelFolder + "coordinator.xml", new FunnelGroupCoordinatorXml()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(funnelFolder + "index.json", new FunnelGroupDruidIndexJson()));
        dynamicPipelineFiles.add(new SimpleImmutableEntry<>(funnelFolder + "druid_load.sh", new FunnelGroupDruidLoadSh(false)));

        for (Map.Entry<String, TemplateFile> file : dynamicPipelineFiles) {
            Files.write(Paths.get(file.getKey()), file.getValue().generateFile(model, version).getBytes());
        }

        // Create all individual funnel dynamic files
        FunnelCoordinatorXml funnelCoordXmlGenerator = new FunnelCoordinatorXml();
        FunnelWorkflowXml funnelWorkflowXmlGenerator = new FunnelWorkflowXml();
        for (Pipeline pipeline : model.getPipelines()) {
            Files.write(Paths.get(funnelFolder + "coordinator_" + pipeline.getPipelineName() + ".xml"),
                    funnelCoordXmlGenerator.generateFile(pipeline, version).getBytes());
            Files.write(Paths.get(funnelFolder + "workflow_" + pipeline.getPipelineName() + ".xml"),
                    funnelWorkflowXmlGenerator.generateFile(pipeline, version).getBytes());
            Files.write(Paths.get(scriptSubfolder + "query_" + pipeline.getPipelineName() + ".hql"),
                    pipeline.getFunnelHql().getBytes());
        }

        if (!download) {
            Files.copy(Paths.get(CLISettings.SKETCHES_HIVE_JAR_PATH + "sketches-hive-0.13.0-with-shaded-core.jar"), Paths.get(libFolder + "sketches-hive-0.13.0-with-shaded-core.jar"), REPLACE_EXISTING);
        }

        return outputDir;
    }
}
