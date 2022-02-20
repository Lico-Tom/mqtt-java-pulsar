/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.github.shoothzj.mjp;

import com.github.shoothzj.mjp.config.MqsarConfig;
import com.github.shoothzj.mjp.config.MqttConfig;
import com.github.shoothzj.mjp.config.PulsarConfig;
import com.github.shoothzj.mjp.module.MqsarBridgeKey;
import com.github.shoothzj.mjp.module.MqttSessionKey;
import com.github.shoothzj.mjp.module.MqttTopicKey;
import com.github.shoothzj.mjp.util.ChannelUtils;
import com.github.shoothzj.mjp.util.ClosableUtils;
import com.github.shoothzj.mjp.util.MqsarReceiver;
import com.github.shoothzj.mjp.util.MqttMessageUtil;
import com.google.common.base.Preconditions;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.pulsar.client.api.*;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.Nullable;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Slf4j
public class MqsarProcessor {

    private final MqsarServer mqsarServer;

    private final MqttConfig mqttConfig;

    private final PulsarConfig pulsarConfig;

    private final PulsarClient pulsarClient;

    private final ReentrantReadWriteLock.ReadLock rLock;

    private final ReentrantReadWriteLock.WriteLock wLock;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionProducerMap;

    private final Map<MqttSessionKey, List<MqttTopicKey>> sessionConsumerMap;

    private final Map<MqttTopicKey, Producer<byte[]>> producerMap;

    private final Map<MqttTopicKey, Consumer<byte[]>> consumerMap;

    private final Map<MqsarBridgeKey, Thread> threadMap;

    public MqsarProcessor(MqsarServer mqsarServer, MqsarConfig mqsarConfig) throws PulsarClientException {
        this.mqsarServer = mqsarServer;
        this.mqttConfig = mqsarConfig.getMqttConfig();
        this.pulsarConfig = mqsarConfig.getPulsarConfig();
/*        this.pulsarClient = PulsarClient.builder()
                .serviceUrl(String.format("pulsar://%s:%d", pulsarConfig.getHost(), pulsarConfig.getTcpPort()))
                .build();*/
        this.pulsarClient = null;
        ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
        rLock = lock.readLock();
        wLock = lock.writeLock();
        this.sessionProducerMap = new HashMap<>();
        this.sessionConsumerMap = new HashMap<>();
        this.producerMap = new HashMap<>();
        this.consumerMap = new HashMap<>();
        this.threadMap = new HashMap<>();
    }

    void processConnect(ChannelHandlerContext ctx, MqttConnectMessage msg) {
        Channel channel = ctx.channel();
        if (msg.decoderResult().isFailure()) {
            Throwable cause = msg.decoderResult().cause();
            if (cause instanceof MqttUnacceptableProtocolVersionException) {
                // Unsupported protocol version
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK,
                                false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(
                                MqttConnectReturnCode.CONNECTION_REFUSED_UNACCEPTABLE_PROTOCOL_VERSION,
                                false), null);
                ctx.writeAndFlush(connAckMessage);
                log.error("connection refused due to invalid protocol, client address [{}]", channel.remoteAddress());
                ctx.close();
                return;
            } else if (cause instanceof MqttIdentifierRejectedException) {
                // ineligible clientId
                MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.CONNACK,
                                false, MqttQoS.AT_MOST_ONCE, false, 0),
                        new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                                false), null);
                ctx.writeAndFlush(connAckMessage);
                log.error("ineligible clientId, client address [{}]", channel.remoteAddress());
                ctx.close();
                return;
            }
            ctx.close();
            return;
        }
        String clientId = msg.payload().clientIdentifier();
        String username = msg.payload().userName();
        byte[] pwd = msg.payload().passwordInBytes();
        if (StringUtils.isBlank(clientId) || StringUtils.isBlank(username)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_IDENTIFIER_REJECTED,
                            false), null);
            ctx.writeAndFlush(connAckMessage);
            log.error("the clientId username pwd cannot be empty, client address[{}]", channel.remoteAddress());
            ctx.close();
            return;
        }

        if (!mqsarServer.auth(username, pwd, clientId)) {
            MqttConnAckMessage connAckMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                    new MqttFixedHeader(MqttMessageType.CONNACK,
                            false, MqttQoS.AT_MOST_ONCE, false, 0),
                    new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_REFUSED_USE_ANOTHER_SERVER,
                            false), null);
            ctx.writeAndFlush(connAckMessage);
            ctx.close();
            return;
        }

        MqttSessionKey mqttSessionKey = new MqttSessionKey();
        mqttSessionKey.setClientId(clientId);
        mqttSessionKey.setUsername(username);
        ChannelUtils.setMqttSession(ctx.channel(), mqttSessionKey);
        wLock.lock();
        try {
            sessionProducerMap.put(mqttSessionKey, Lists.newArrayList());
            sessionConsumerMap.put(mqttSessionKey, Lists.newArrayList());
        } finally {
            wLock.unlock();
        }
        MqttConnAckMessage mqttConnectMessage = (MqttConnAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.CONNACK,
                        false, MqttQoS.AT_MOST_ONCE, false, 0),
                new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, false),
                null);
        ctx.writeAndFlush(mqttConnectMessage);
    }

    void processPubAck(ChannelHandlerContext ctx, MqttPubAckMessage msg) {
    }

    void processPublish(ChannelHandlerContext ctx, MqttPublishMessage msg) {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("client address [{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.EXACTLY_ONCE) {
            log.error("does not support QoS2 protocol. clientId [{}], username [{}] ",
                    mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }

        if (msg.fixedHeader().qosLevel() == MqttQoS.FAILURE) {
            log.error("failure. clientId [{}], username [{}] ", mqttSession.getClientId(), mqttSession.getUsername());
            return;
        }

        int len = msg.payload().readableBytes();
        byte[] messageBytes = new byte[len];
        msg.payload().getBytes(msg.payload().readerIndex(), messageBytes);
        MqttSessionKey mqttSessionKey = ChannelUtils.getMqttSession(ctx.channel());
        Preconditions.checkNotNull(mqttSessionKey);
        String topic = mqsarServer.produceTopic(mqttSessionKey.getUsername(),
                mqttSessionKey.getClientId(), msg.variableHeader().topicName());
        Producer<byte[]> producer;
        try {
            producer = getOrCreateProducer(mqttSessionKey, topic);
        } catch (PulsarClientException e) {
            ctx.close();
            log.error("clientId [{}], username [{}].create pulsar producer fail ",
                    mqttSession.getClientId(), mqttSession.getUsername(), e);
            return;
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_MOST_ONCE) {
            producer.sendAsync(messageBytes).
                    thenAccept(messageId -> log.info("clientId [{}],"
                                    + " username [{}]. send message to pulsar success messageId: {}",
                            mqttSession.getClientId(), mqttSession.getUsername(), messageId))
                    .exceptionally((e) -> {
                        log.error("clientId [{}], username [{}]. send message to pulsar fail: ",
                                mqttSession.getClientId(), mqttSession.getUsername(), e);
                        return null;
                    });
        }
        if (msg.fixedHeader().qosLevel() == MqttQoS.AT_LEAST_ONCE) {
            try {
                MessageId messageId = producer.send(messageBytes);
                MqttPubAckMessage pubAckMessage = (MqttPubAckMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                        MqttMessageIdVariableHeader.from(msg.variableHeader().packetId()), null);
                log.info("clientId [{}], username [{}]. send pulsar success. messageId: {}",
                        mqttSession.getClientId(), mqttSession.getUsername(), messageId);
                ctx.writeAndFlush(pubAckMessage);
            } catch (PulsarClientException e) {
                log.error("clientId [{}], username [{}]. send pulsar error: {}",
                        mqttSession.getClientId(), mqttSession.getUsername(), e.getMessage());
            }
        }
    }

    private Producer<byte[]> getOrCreateProducer(MqttSessionKey mqttSessionKey, String topic)
            throws PulsarClientException {
        MqttTopicKey mqttTopicKey = new MqttTopicKey();
        mqttTopicKey.setTopic(topic);
        mqttTopicKey.setMqttSessionKey(mqttSessionKey);

        wLock.lock();
        try {
            Producer<byte[]> producer = producerMap.get(mqttTopicKey);
            if (producer == null) {
                producer = pulsarClient.newProducer().topic(topic).create();
                if (sessionProducerMap.containsKey(mqttSessionKey)) {
                    List<MqttTopicKey> mqttTopicKeys = sessionProducerMap.get(mqttSessionKey);
                    mqttTopicKeys.add(mqttTopicKey);
                } else {
                    List<MqttTopicKey> mqttTopicKeys = Lists.newArrayList();
                    mqttTopicKeys.add(mqttTopicKey);
                    sessionProducerMap.put(mqttSessionKey, mqttTopicKeys);
                }
                producerMap.put(mqttTopicKey, producer);
            }
            return producer;
        } finally {
            wLock.unlock();
        }
    }

    void processPubRel(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubRec(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processPubComp(ChannelHandlerContext ctx, MqttMessage msg) {
    }

    void processDisconnect(ChannelHandlerContext ctx, MqttMessage msg) {
        Channel channel = ctx.channel();
        closeMqttSession(ChannelUtils.getMqttSession(channel));
        log.info("the close channel ...");
        ctx.close();
    }

    void processConnectionLost(ChannelHandlerContext ctx) {
    }

    void processSubscribe(ChannelHandlerContext ctx, MqttSubscribeMessage msg) {
        log.info("the ctx {}", ctx);
        List<MqttTopicSubscription> mqttTopicSubscriptions = msg.payload().topicSubscriptions();
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("client address [{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        List<String> topicNames = Lists.newArrayList();
        List<Integer> mqttQos = Lists.newArrayList();
        for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
            topicNames.add(mqttTopicSubscription.topicName());
            mqttQos.add(mqttTopicSubscription.qualityOfService().value());
        }
        MqttSubAckMessage mqttMessage = (MqttSubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(msg.variableHeader().messageId()),
                new MqttSubAckPayload(mqttQos)
        );
        ctx.writeAndFlush(mqttMessage);

        for (MqttTopicSubscription mqttTopicSubscription : mqttTopicSubscriptions) {
            String topicName = mqsarServer.produceTopic(mqttSession.getUsername(),
                    mqttSession.getClientId(), mqttTopicSubscription.topicName());
            try {
                Consumer<byte[]> consumer = getOrCreateConsumer(mqttSession, topicName);
                MqsarBridgeKey bridgeKey = new MqsarBridgeKey();
                bridgeKey.setCtx(ctx);
                bridgeKey.setTopic(topicName);
                if (threadMap.containsKey(bridgeKey)) {
                    MqsarReceiver mqsarReceiver = new MqsarReceiver(bridgeKey, mqttTopicSubscription.qualityOfService(), consumer);
                    Thread thread = new Thread(mqsarReceiver);
                    threadMap.put(bridgeKey, thread);
                    thread.start();
                }
            } catch (PulsarClientException e) {
                log.error("clientId [{}], username [{}], topicName [{}].create pulsar producer fail ",
                        mqttSession.getClientId(), mqttSession.getUsername(), topicName, e);
            }
        }
    }

    private Consumer<byte[]> getOrCreateConsumer(MqttSessionKey mqttSessionKey, String topic) throws PulsarClientException {
        MqttTopicKey subTopicKey = new MqttTopicKey();
        subTopicKey.setTopic(topic);
        subTopicKey.setMqttSessionKey(mqttSessionKey);
        Consumer<byte[]> consumer = consumerMap.get(subTopicKey);
        if (consumer == null) {
            consumer = pulsarClient.newConsumer().topic(topic).subscribe();
            List<MqttTopicKey> topicKeys = sessionConsumerMap.get(mqttSessionKey);
            if (topicKeys == null) {
                topicKeys = Lists.newArrayList();
            }
            topicKeys.add(subTopicKey);
            sessionConsumerMap.put(mqttSessionKey, topicKeys);
        }
        return consumer;
    }

    void processUnSubscribe(ChannelHandlerContext ctx, MqttUnsubscribeMessage msg) {
        MqttSessionKey mqttSession = ChannelUtils.getMqttSession(ctx.channel());
        if (mqttSession == null) {
            log.error("client address [{}]", ctx.channel().remoteAddress());
            ctx.close();
            return;
        }
        final List<String> topics = msg.payload().topics();
        for (String topic : topics) {
            String topicName = mqsarServer.produceTopic(mqttSession.getUsername(),
                    mqttSession.getClientId(), topic);
            MqsarBridgeKey bridgeKey = new MqsarBridgeKey();
            bridgeKey.setCtx(ctx);
            bridgeKey.setTopic(topicName);
            wLock.lock();
            try {
                Thread thread = threadMap.get(bridgeKey);
                thread.interrupt();
                threadMap.remove(bridgeKey);
            } finally {
                wLock.unlock();
            }

        }
        rLock.lock();
        try {
            List<MqttTopicKey> mqttTopicKeys = sessionConsumerMap.get(mqttSession);
            if (mqttTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : mqttTopicKeys) {
                    consumerMap.remove(mqttTopicKey);
                }
            }
            sessionConsumerMap.remove(mqttSession);
        } finally {
            rLock.unlock();
        }

        MqttUnsubAckMessage mqttMessage = (MqttUnsubAckMessage) MqttMessageFactory.newMessage(
                new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 0),
                MqttMessageIdVariableHeader.from(msg.variableHeader().messageId()), null
        );
        ctx.writeAndFlush(mqttMessage);

    }

    void processPingReq(ChannelHandlerContext ctx) {
        ctx.writeAndFlush(MqttMessageUtil.pingResp());
    }

    private void closeMqttSession(@Nullable MqttSessionKey mqttSessionKey) {
        if (mqttSessionKey == null) {
            return;
        }
        wLock.lock();
        try {
            // find producers
            List<MqttTopicKey> produceTopicKeys = sessionProducerMap.get(mqttSessionKey);
            if (produceTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : produceTopicKeys) {
                    Producer<byte[]> producer = producerMap.get(mqttTopicKey);
                    if (producer != null) {
                        ClosableUtils.close(producer);
                    }
                }
            }
            List<MqttTopicKey> consumeTopicKeys = sessionConsumerMap.get(mqttSessionKey);
            if (consumeTopicKeys != null) {
                for (MqttTopicKey mqttTopicKey : consumeTopicKeys) {
                    Consumer<byte[]> consumer = consumerMap.get(mqttTopicKey);
                    if (consumer != null) {
                        ClosableUtils.close(consumer);
                    }
                }
            }
        } finally {
            wLock.unlock();
        }
    }
}
