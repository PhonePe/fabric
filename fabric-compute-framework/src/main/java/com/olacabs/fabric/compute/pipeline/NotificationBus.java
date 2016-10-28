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

package com.olacabs.fabric.compute.pipeline;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.olacabs.fabric.compute.comms.ChannelFactory;
import com.olacabs.fabric.compute.comms.CommsChannel;
import com.olacabs.fabric.compute.source.PipelineStreamSource;
import com.olacabs.fabric.compute.tracking.SimpleBitSet;
import com.olacabs.fabric.model.event.EventSet;
import lombok.Builder;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * The backbone for fabric. The notification bus is used to connect all the components together, transfer message
 * from one component to another and acking messages.
 */
@Slf4j
public class NotificationBus {
    private final Map<Long, SimpleBitSet> tracker = Maps.newConcurrentMap();
    private Properties properties;
    private Map<Integer, Connection> connections = Maps.newHashMap();
    private Map<Integer, Communicator> comms = Maps.newHashMap();
    private Map<Integer, PipelineStreamSource> sources = Maps.newHashMap();

    public NotificationBus(final Properties properties) {
        this.properties = properties;
        log.info("Notification bus created...");
    }

    public NotificationBus source(PipelineStreamSource streamSource) {
        sources.put(streamSource.communicationId(), streamSource);
        return this;
    }

    public NotificationBus connect(MessageSource to, PipelineStage... pipelineStages) {
        if (!connections.containsKey(to.communicationId())) {
            connections.put(to.communicationId(), new Connection());
        }
        for (PipelineStage pipelineStage : pipelineStages) {
            connections.get(to.communicationId()).addConnection(pipelineStage.communicationId());
            if (!comms.containsKey(pipelineStage.communicationId())) {
                comms.put(pipelineStage.communicationId(),
                    Communicator.builder()
                        .commsChannel(ChannelFactory.create(properties, pipelineStage.name(), false, pipelineStage))
                        .pipelineStage(pipelineStage)
                        .build());
            }
        }
        return this;
    }

    /**
     * Publish a message to a bus. Any userspace message will be forwarded to the next component. If not other component
     * is connected to the publisher, the message will be acked.
     * @param message The message to be forwarded
     * @param from The component that is publishing the message
     */
    public synchronized void publish(PipelineMessage message, int from) {
        publish(message, from, true);
    }

    /**
     * Publish a message to a bus. Any userspace message will be forwarded to the next component depending on
     * the forward flag. Typically when message is delivered to a
     * {@link com.olacabs.fabric.compute.processor.ScheduledProcessor}, it will not be forwarded to the next components.
     * This is because the
     * {@link com.olacabs.fabric.compute.processor.ScheduledProcessor#consume(ProcessingContext, EventSet)}
     * method does not return an eventset. If not other component is connected to the publisher, the message will be
     * acked.
     *
     * @param message The message to be published
     * @param from The component that is publishing the message
     * @param forward If the message will be dlivered to downstream components
     */
    public synchronized void publish(PipelineMessage message, int from, boolean forward) {
        switch (message.getMessageType()) {
            case TIMER:
                // It is a timer message, ACKing is disabled
                comms.get(from).commsChannel.publish(message);
                break;

            case USERSPACE:
                // It is a userspace message, ACKing is enabled
                PipelineMessage actionableMessage = message;
                ImmutableSet.Builder<EventSet> ackCandidatesBuilder = ImmutableSet.builder();
                if (!message.getMessages().isAggregate()) {
                    // Find out the oldest ancestor of this event set
                    while (true) {
                        if (null == actionableMessage.getParent()) {
                            break;
                        }
                        actionableMessage = actionableMessage.getParent();
                    }

                    if (!tracker.containsKey(actionableMessage.getMessages().getId())) {
                        // Set up the tracker for this event set since it is sent from a source for the first time
                        // Add this event set to the tracker with an empty bitset
                        tracker.put(actionableMessage.getMessages().getId(), new SimpleBitSet(64));
                    }
                    SimpleBitSet msgBitSet = tracker.get(actionableMessage.getMessages().getId());
                    try {
                        // If the event set is being sent forward and the sender sends normal message, set bits
                        // for each of the receivers based on the auto incremented id assigned to them
                        if (forward
                            && connections.containsKey(from)
                            && (!comms.containsKey(from)
                                || comms.get(from).pipelineStage.sendsNormalMessage())) {
                            connections.get(from).pipelineStages.forEach(msgBitSet::set);
                        }
                    } catch (Exception e) {
                        log.error("Error setting tracking bits for generator: " + Integer.toString(from), e);
                    }

                    // unset the bit corresponding to the sender
                    msgBitSet.unset(from);
                    // if all the bits are unset and if this event set is generated by a source, mark the
                    // event set as a candidate for ACKing
                    if (!msgBitSet.hasSetBits() && actionableMessage.getMessages().isSourceGenerated()) {
                        ackCandidatesBuilder.add(actionableMessage.getMessages());
                    }
                }
                try {
                    // publish the message to each of the receivers
                    if (forward && connections.containsKey(from)) {
                        connections.get(from).pipelineStages
                                .forEach(pipeLineStage -> comms.get(pipeLineStage).commsChannel.publish(message));
                    }
                } catch (Throwable t) {
                    log.error("Error sending event to children for " + Integer.toString(from), t);
                }
                // event sets which are eligible for ACKing
                ImmutableSet<EventSet> ackSet = ackCandidatesBuilder.build();

                // ACK all the event sets
                ackSet.forEach(eventSet -> sources.get(eventSet.getSourceId()).ackMessage(eventSet));
                // No event set to ACK, hence we can remove the tracker entry for this event set
                if (!ackSet.isEmpty()) {
                    tracker.remove(actionableMessage.getMessages().getId());
                }

                break;
            default: break;

        }


    }

    /**
     * Start the notification bus.
     */
    public void start() {
        connections = ImmutableMap.copyOf(connections);
        comms.values().forEach(communicator -> communicator.commsChannel.start());
    }

    /**
     * Stop the notification bus. Essentially all message transfers will stop.
     */
    public void stop() {
        comms.values().forEach(communicator -> communicator.commsChannel.stop());
    }

    /**
     * Represents a connection between one component and the other.
     */
    private static class Connection {
        private final Set<Integer> pipelineStages;

        Connection() {
            pipelineStages = Sets.newHashSet();
        }

        Connection addConnection(int pipelineStage) {
            pipelineStages.add(pipelineStage);
            return this;
        }
    }


    /**
     * Communication channel metadata.
     */
    @Builder
    public static class Communicator {
        private MessageSource pipelineStage;
        private CommsChannel<PipelineMessage> commsChannel;
    }

}
