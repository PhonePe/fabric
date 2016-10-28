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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.olacabs.fabric.compute.ProcessorTestBench;
import com.olacabs.fabric.model.common.ComponentMetadata;
import com.olacabs.fabric.model.event.Event;
import com.olacabs.fabric.model.event.EventSet;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Tests {@link JsonFilter}.
 */
public class JsonFilterTest {
    private final List<Object> validEventByteArray = ImmutableList.of(
                                                "{\"name\" : \"ss\"}".getBytes(),
                                                "{\"name\" : \"np\"}".getBytes(),
                                                "{\"name\" : \"ss\"}".getBytes());
    private final List<Object> invalidEventByteArray = ImmutableList.of(
            "{\"name\" : \"ab\"}".getBytes(),
            "{\"name\" : \"np\"}".getBytes(),
            "{\"name\" : \"cd\"}".getBytes());
    @Test
    public void consumeFromJsonNode() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");

        List<String> events = ImmutableList.of("{\"name\" : \"ss\"}", "{\"name\" : \"np\"}", "{\"name\" : \"ss\"}");
        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());


        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(EventSet.eventFromEventBuilder().events(ImmutableList
                                .of(
                                        Event.builder().jsonNode(mapper.readTree(events.get(0))).build(),
                                        Event.builder().jsonNode(mapper.readTree(events.get(1))).build(),
                                        Event.builder().jsonNode(mapper.readTree(events.get(2))).build()))
                                .build()));


        Assert.assertEquals(1, output.size());
        Assert.assertEquals(2, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromJsonNodeNoMatch() throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");

        List<String> events = ImmutableList.of("{\"name\" : \"kb\"}", "{\"name\" : \"np\"}", "{\"name\" : \"rc\"}");
        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());


        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(EventSet.eventFromEventBuilder().events(ImmutableList
                                .of(
                                        Event.builder().jsonNode(mapper.readTree(events.get(0))).build(),
                                        Event.builder().jsonNode(mapper.readTree(events.get(1))).build(),
                                        Event.builder().jsonNode(mapper.readTree(events.get(2))).build()))
                                .build()));


        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromByte() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");


        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());


        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(validEventByteArray)));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(2, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromByteNoMatch() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");


        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());


        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(invalidEventByteArray)));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromByteNoMatchDueToNoParse() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");

        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());

        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(validEventByteArray)));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromByteNoMatchDueToNonByte() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");

        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());

        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(ImmutableList.of(new Object(), new Object(), new Object()))));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());

    }

    @Test
    public void consumeFromByteNoMatchDueToInvalidJson() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");

        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());

        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(ImmutableList.of("{a}", "{b}", "{c}"))));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());
    }

    @Test
    public void consumeFromByteNoMatchDueToNoData() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");

        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());

        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(ImmutableList.of())));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(0, output.get(0).getEvents().size());
    }

    @Test
    public void consumeFromByteMixedMatch() throws Exception {
        final JsonFilter processor = new JsonFilter();
        Properties globalProperties = new Properties();
        Properties localProperties = new Properties();
        localProperties.setProperty("rule", "{\"type\" : \"equals\", \"path\" : \"/name\", \"value\" : \"ss\"}");
        localProperties.setProperty("parse_event", "true");

        processor.initialize("test", globalProperties, localProperties, new ComponentMetadata());

        ProcessorTestBench processorTestBench = new ProcessorTestBench(false);
        List<EventSet> output = processorTestBench.runStreamingProcessor(processor, ImmutableList
                .of(createEventSetFromArray(
                        ImmutableList.of(
                                new Object(),
                                "{\"name\": \"ss\"}".getBytes(),
                                "{c}",
                                "{\"name\": \"ss\"}"))));

        Assert.assertEquals(1, output.size());
        Assert.assertEquals(1, output.get(0).getEvents().size());
    }

    private static EventSet createEventSetFromArray(List<Object> raw) {
        return EventSet.eventFromEventBuilder().events(raw.stream()
                        .map(data -> Event.builder().data(data).build())
                        .collect(Collectors.toList()))
                .build();
    }
}
