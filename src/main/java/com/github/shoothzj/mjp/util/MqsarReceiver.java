package com.github.shoothzj.mjp.util;

import com.github.shoothzj.mjp.module.MqsarBridgeKey;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.mqtt.*;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClientException;

public class MqsarReceiver implements Runnable {

    private ChannelHandlerContext ctx;
    private Consumer<byte[]> consumer;
    private MqttQoS mqttQoS;

    public MqsarReceiver(MqsarBridgeKey mqsarBridgeKey, MqttQoS mqttQoS, Consumer<byte[]> consumer) {
        this.ctx = mqsarBridgeKey.getCtx();
        this.consumer = consumer;
        this.mqttQoS = mqttQoS;
    }


    @Override
    public void run() {
        while (true) {
            try {
                Message<byte[]> message = consumer.receive();
                String topicName = message.getTopicName();
                MqttPublishMessage publishMessage = (MqttPublishMessage) MqttMessageFactory.newMessage(
                        new MqttFixedHeader(MqttMessageType.PUBLISH, false, mqttQoS, false, 0),
                        new MqttPublishVariableHeader(topicName, 0),
                        message.getData());
                ctx.writeAndFlush(publishMessage);
                consumer.acknowledge(message.getMessageId());
            } catch (PulsarClientException e) {
                e.printStackTrace();
            }
        }
    }
}
