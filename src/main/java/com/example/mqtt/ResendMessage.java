package com.example.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;

public class ResendMessage {
    /**
     * 主题
     */
    protected final String topic;
    /**
     * 消息
     */
    protected final Buffer buffer;
    /**
     * 等级
     */
    protected final MqttQoS qos;

    protected ResendMessage(QosMessageInfo info) {
        this.topic = info.message.topicName();
        this.buffer = info.message.payload();
        this.qos = info.qos;
    }
}
