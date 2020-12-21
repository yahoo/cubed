/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import lombok.extern.slf4j.Slf4j;

/**
 * Zip utility functions.
 */
@Slf4j
public class ZipUtil {
    /**
     * Zips a directory into a zip file.
     * @param outputZipFile Output zip file
     * @param inputDirPath Input dir path
     */
    public static void zipDir(String outputZipFile, String inputDirPath) throws Exception {
        log.info("Zipping {} to {}", inputDirPath, outputZipFile);
        File inputDir = new File(inputDirPath);
        String rootPath = inputDir.getParentFile().getAbsolutePath();
        ZipOutputStream out = new ZipOutputStream(new FileOutputStream(outputZipFile));
        addDir(inputDir, out, rootPath);
        out.close();
    }

    /**
     * Add directories recursively to a zip output stream.
     * @dirObj Directory to zip
     * @param out Zip output stream
     */
    private static void addDir(File dirObj, ZipOutputStream out, String rootPath) throws IOException {
        File[] files = dirObj.listFiles();
        byte[] tempBuffer = new byte[1024];

        // For each file
        for (int i = 0; i < files.length; i++) {
            // If the file is a directory
            if (files[i].isDirectory()) {
                // Recurse on the subdirectory
                addDir(files[i], out, rootPath);
                continue;
            }
            // Get the path of the file
            FileInputStream in = new FileInputStream(files[i].getAbsolutePath());
            // Add a new entry to the zip stream
            String filePath = files[i].getAbsolutePath();
            out.putNextEntry(new ZipEntry(filePath.substring(rootPath.length())));
            // Read the file contents and write
            int len;
            while ((len = in.read(tempBuffer)) > 0) {
                out.write(tempBuffer, 0, len);
            }
            // Close in and out
            out.closeEntry();
            in.close();
        }
    }
}
