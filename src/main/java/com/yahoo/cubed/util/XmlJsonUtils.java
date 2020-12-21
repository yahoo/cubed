/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.StringWriter;
import org.dom4j.Document;
import org.dom4j.DocumentHelper;
import org.dom4j.io.OutputFormat;
import org.dom4j.io.XMLWriter;
import lombok.extern.slf4j.Slf4j;

/**
 * XML JSON utilities.
 */
@Slf4j
public class XmlJsonUtils {
    /**
     * Pretty print XML string with indent.
     */
    public static String prettyPrintXmlString(String input, int indent) {
        try {
            Document doc = DocumentHelper.parseText(input);
            StringWriter sw = new StringWriter();
            OutputFormat format = OutputFormat.createPrettyPrint();
            format.setIndent(true);
            format.setTrimText(true);
            format.setIndentSize(indent);
            XMLWriter xw = new XMLWriter(sw, format);
            xw.write(doc);

            return sw.toString();
        } catch (Exception e) {
            log.error("Error: ", e.getMessage());
            return input;
        }
    }

    /**
     * Pretty print JSON string.
     */
    public static String prettyPrintJsonString(String input) {
        Gson gson = new GsonBuilder().setPrettyPrinting().create();
        JsonParser jp = new JsonParser();
        JsonElement je = jp.parse(input.toString());
        return gson.toJson(je);
    }
}
