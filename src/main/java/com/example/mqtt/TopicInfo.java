package com.example.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttEndpoint;

/**
 * 订阅信息
 */
public class TopicInfo {
    /**
     * 等级
     */
    protected MqttQoS qos;
    /**
     * 链接
     */
    protected MqttEndpoint endpoint;

    protected TopicInfo(MqttQoS qos, MqttEndpoint endpoint) {
        this.qos = qos;
        this.endpoint = endpoint;
    }
}
