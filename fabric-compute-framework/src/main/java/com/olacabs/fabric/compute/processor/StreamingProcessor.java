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

package com.olacabs.fabric.compute.processor;

import com.olacabs.fabric.compute.EventCollector;
import com.olacabs.fabric.compute.ProcessingContext;
import com.olacabs.fabric.model.event.Event;
import com.olacabs.fabric.model.event.EventSet;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;

import java.util.Collections;
import java.util.List;

/**
 * TODO javadoc.
 */
@Slf4j
public abstract class StreamingProcessor extends ProcessorBase {

    public StreamingProcessor() {
        super(false);
    }

    protected abstract EventSet consume(ProcessingContext context, EventSet eventSet) throws ProcessingException;

    @Override
    public void process(ProcessingContext context, EventCollector eventCollector, EventSet eventSet)
            throws ProcessingException {
        MDC.put("componentId", getId());
        eventCollector.publish(consume(context, eventSet));
        MDC.remove("componentId");
    }

    @Override
    public final List<Event> timeTriggerHandler(ProcessingContext context) {
        MDC.put("componentId", getId());
        log.warn("timeTriggerHandler() called on StreamingProcessor");
        MDC.remove("componentId");
        return Collections.emptyList();
    }
}
