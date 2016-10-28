/*
 * Copyright 2016 ANI Technologies Pvt. Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


package com.olacabs.fabric.processors.filters;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.olacabs.fabric.common.util.PropertyReader;
import com.olacabs.fabric.compute.ProcessingContext;
import com.olacabs.fabric.compute.processor.InitializationException;
import com.olacabs.fabric.compute.processor.ProcessingException;
import com.olacabs.fabric.compute.processor.StreamingProcessor;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.olacabs.fabric.model.event.Event;
import com.olacabs.fabric.model.event.EventSet;
import com.olacabs.fabric.model.processor.Processor;
import com.olacabs.fabric.model.processor.ProcessorType;
import io.appform.jsonrules.Expression;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;


/**
 * A processor that filters and stops an event from propagation.
 */
@EqualsAndHashCode(callSuper = true)
@VisibleForTesting
@Slf4j
@Data
@Processor(
    namespace = "global",
    name = "json-filter",
    version = "1.0.0",
    description = "A processor that filters out events that match a provided json rule!!",
    cpu = 0.5,
    memory = 512,
    processorType = ProcessorType.EVENT_DRIVEN,
    requiredProperties = {"rule"},
    optionalProperties = {"parse_event"}
    )
public class JsonFilter extends StreamingProcessor {
    private Expression expression;
    private boolean parseEvent = false;

    @Override
    protected EventSet consume(ProcessingContext processingContext, EventSet eventSet) throws ProcessingException {
        List<Event> matchedEvents = eventSet.getEvents()
                .stream()
                .filter(event -> {
                    JsonNode node = event.getJsonNode();
                    if (null == node) {
                        if (parseEvent) {
                            Object data = event.getData();
                            if (null == data) {
                                log.error("No data in event but parse rule is set. This will not match.");
                                return false;
                            }
                            if (data instanceof byte[]) {
                                final byte[] rawData = (byte[])data;
                                try {
                                    node = processingContext.getMapper().readTree(rawData);
                                } catch (IOException e) {
                                    log.error("Could not deserialize byte array to json node. This will not match.", e);
                                    return false;
                                }
                            } else {
                                log.error("Data is not byte array, but parse_event is set. This will not match.");
                                return false;
                            }
                        } else {
                            log.error("No json node is present. And parse_event is not set. This will not match.");
                            return false;
                        }
                    }
                    return expression.evaluate(node);
                })
                .collect(Collectors.toList());
        log.debug("Matched event set size - {}", matchedEvents.size());
        return EventSet.eventFromEventBuilder()
                .partitionId(eventSet.getPartitionId())
                .events(matchedEvents)
                .build();
    }

    @Override
    public void initialize(
            String instanceId,
            Properties globalProperties,
            Properties properties,
            ComponentMetadata componentMetadata) throws InitializationException {

        final String rule = PropertyReader.readString(properties, globalProperties, "rule");

        log.info("Rule: {}", rule);

        parseEvent = PropertyReader.readBoolean(properties, globalProperties, "parse_event", false);

        if (parseEvent) {
            log.info("Events will be parsed to json as parse_event is set to true");
        } else {
            log.info("Events will be not be parsed to json as parse_event is set to false. "
                    + "Make sure this is enabled at topology level");
        }
        try {
            expression = new ObjectMapper().readValue(rule, Expression.class);
        } catch (IOException e) {
            throw new InitializationException("Failed to parse rule: " + rule, e);
        }
    }

    @Override
    public void destroy() {

    }
}


