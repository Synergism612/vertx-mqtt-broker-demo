package com.example.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.messages.MqttPublishMessage;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * qos消息存储
 */
public class QosMessageInfo {
    /**
     * 发布者
     */
    protected final MqttEndpoint endpoint;

    /**
     * 结果计数<br/>
     * 为了加速判断
     */
    protected final AtomicInteger count = new AtomicInteger();

    /**
     * 记录消息接收情况<br/>
     * key:链接用户id<br/>
     * value:是否有回复成功
     */
    protected Map<String, Boolean> record = new HashMap<>();

    /**
     * 原始消息
     */
    protected final MqttPublishMessage message;

    /**
     * 降级后的qos
     */
    protected MqttQoS qos;

    /**
     * 是否被消费
     */
    protected boolean isUsed = false;

    protected QosMessageInfo(MqttEndpoint endpoint, MqttPublishMessage message) {
        this.endpoint = endpoint;
        this.message = message;
    }
}
