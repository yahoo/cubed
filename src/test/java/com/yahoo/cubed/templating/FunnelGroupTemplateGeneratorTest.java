/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.App;
import com.yahoo.cubed.TestUtils;
import com.yahoo.cubed.model.FunnelGroup;
import com.yahoo.cubed.settings.CLISettings;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Test template generator.
 */
public class FunnelGroupTemplateGeneratorTest {

    private FunnelGroupTemplateGenerator tg;

    /**
     * Init bundle properties generation.
     */
    @BeforeClass
    public void initialize() throws Exception {
        tg = new FunnelGroupTemplateGenerator();
        tg.setDownload(true);
        CLISettings.DB_CONFIG_FILE = "src/test/resources/database-configuration.properties";
        App.prepareDatabase();
        App.dropAllFields();
        App.loadSchemas("src/test/resources/schemas/");
    }

    /**
     * Test generation of funnel group package zip.
     */
    @Test
    public void testGenerateTemplateFiles() throws Exception {

        FunnelGroup funnelGroup = TestUtils.constructSampleFunnelGroup();
        long version = System.currentTimeMillis();

        tg.generateTemplateFiles(funnelGroup, "./target", version);

        String fileOutputTesting = "target/" + funnelGroup.getFunnelGroupName() + "_v" + version + "/";
        String emailSubfolder = fileOutputTesting + "email/";
        String funnelGroupSubfolder = fileOutputTesting + "funnel_group/";
        // List of all expected files
        List<File> fileList = new ArrayList<>();
        fileList.add(new File(fileOutputTesting + "bundle.xml"));
        fileList.add(new File(fileOutputTesting + "properties.xml"));
        fileList.add(new File(emailSubfolder + "workflow.xml"));
        fileList.add(new File(funnelGroupSubfolder + "workflow.xml"));
        fileList.add(new File(funnelGroupSubfolder + "coordinator.xml"));
        fileList.add(new File(funnelGroupSubfolder + "job.xml"));
        fileList.add(new File(funnelGroupSubfolder + "index.json"));
        fileList.add(new File(funnelGroupSubfolder + "druid_load.sh"));

        for (File f : fileList) {
            Assert.assertTrue(f.exists() && !f.isDirectory(), "Could not find " + f.getAbsolutePath());
        }
    }
}
