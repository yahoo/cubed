/*
 * Copyright Verizon Media, Licensed under the terms of the Apache License, Version 2.0. See LICENSE file in project root for terms.
 */

package com.yahoo.cubed.json.filter;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;

/**
 * Deserialize filter JSON.
 */
public class FilterDeserializer extends JsonDeserializer<Filter> {
    private static final String CONDITION_FIELD_NAME = "condition";

    /**
     * Deserialize JSON string to JSON objects.
     */
    @Override
    public Filter deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        ObjectMapper codec = (ObjectMapper) p.getCodec();
        JsonNode node = codec.readTree(p);
        if (node.has(CONDITION_FIELD_NAME)) {
            return (Filter) codec.treeToValue(node, LogicalRule.class);
        } else {
            return (Filter) codec.treeToValue(node, RelationalRule.class);
        }
    }
}
