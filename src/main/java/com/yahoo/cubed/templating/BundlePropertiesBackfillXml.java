/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.templating;

import com.yahoo.cubed.model.Pipeline;
import org.antlr.stringtemplate.StringTemplate;

/**
 * Bundle properties backfill XML generator.
 */
public class BundlePropertiesBackfillXml extends BundlePropertiesXml {
    /**
     * Set additional properties: backfill.
     */
    protected void setExtraProperties(StringTemplate template, Pipeline model) {
        template.setAttribute(BundlePropertiesXml.BUNDLE_PROPERTY_FILE_ATTRIBUTE, "bundle_backfill");
        template.setAttribute(BundlePropertiesXml.FUNNEL_QUERY_RANGE, "1");
        template.setAttribute(BundlePropertiesXml.FUNNEL_REPEAT_INTERVAL, "1");
        template.setAttribute(BundlePropertiesXml.FUNNEL_STEPS, "default");
    }
}
