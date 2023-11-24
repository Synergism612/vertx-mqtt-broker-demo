package com.example.mqtt;

import io.netty.handler.codec.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.quarkus.logging.Log;
import io.vertx.core.buffer.Buffer;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttTopicSubscription;
import io.vertx.mqtt.messages.*;
import io.vertx.mqtt.messages.codes.MqttDisconnectReasonCode;
import jakarta.enterprise.context.ApplicationScoped;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * mqtt单例类
 */
@ApplicationScoped
public class MQTTBroker extends MQTT {

    /**
     * 向主题发布消息<br/>
     * 该函数使用qos0<br/>
     * 不保证必达
     *
     * @param topic 主题
     * @param msg   消息内容
     */
    public void publish(String topic, String msg) {
        // 没有这个主题或主题下没有订阅者则什么也不做
        if (!topicMap.containsKey(topic) || topicMap.get(topic).isEmpty()) {
            return;
        }
        for (TopicInfo topicInfo : topicMap.get(topic)) {
            topicInfo.endpoint.publish(topic, Buffer.buffer(msg.getBytes(Charset.defaultCharset())), MqttQoS.AT_MOST_ONCE, false, false);
        }
    }

    /**
     * 向客户端重新发送消息
     *
     * @param endpoint 客户端
     * @param message  重发消息
     */
    private void publish(MqttEndpoint endpoint, ResendMessage message) {
        // 只有qos2需要该函数，该消息需要标记为重发消息
        endpoint.publish(message.topic, message.buffer, MqttQoS.EXACTLY_ONCE, true, false);
    }

    /**
     * 客户订阅主题
     *
     * @param topic    主题
     * @param qos      等级
     * @param endpoint 链接
     */
    private void addTopic(String topic, MqttQoS qos, MqttEndpoint endpoint) {
        List<TopicInfo> infos = topicMap.get(topic);
        if (Objects.isNull(infos) || infos.isEmpty()) {
            topicMap.put(topic, new ArrayList<>(List.of(new TopicInfo(qos, endpoint))));
            return;
        }
        for (TopicInfo topicInfo : topicMap.get(topic)) {
            if (topicInfo.endpoint.clientIdentifier().equals(endpoint.clientIdentifier())) {
                topicInfo.qos = qos;
                return;
            }
        }
        topicMap.get(topic).add(new TopicInfo(qos, endpoint));
    }

    /**
     * 客户退订主题
     *
     * @param topic    主题
     * @param endpoint 链接
     */
    private void removeTopic(String topic, MqttEndpoint endpoint) {
        topicMap.get(topic).removeIf(topicInfo ->
                topicInfo.endpoint.clientIdentifier().equals(endpoint.clientIdentifier()
                )
        );
    }

    /**
     * 根据主题名中获取主题池中对应的链接<br/>
     * 主题名中可能存在通配符，通过通配符匹配主题<br/>
     * 结果列表经过去重
     *
     * @param topic 含有通配符的主题
     * @return 主题列表
     */
    private List<TopicInfo> getTopicsForWildcard(String topic) {
        List<String> topics = new ArrayList<>();
        String[] topicSplit = topic.split("/");
        topicMap.keySet().forEach(key -> {
            if (key.equals(topic)) {
                topics.add(key);
            } else {
                String[] keySplit = key.split("/");
                int i = -1, j = -1;
                while (true) {
                    i++;
                    j++;
                    if (i == topicSplit.length || j == keySplit.length) {
                        if (topicSplit.length == keySplit.length) topics.add(key);
                        break;
                    }
                    if (topicSplit[i].equals("#") || keySplit[j].equals("#")) {
                        topics.add(key);
                        break;
                    }
                    if (topicSplit[i].equals("+") || keySplit[j].equals("+")) continue;
                    if (!topicSplit[i].equals(keySplit[j])) break;
                }

            }
        });
        return topics.stream().map(topicMap::get).flatMap(Collection::stream).distinct().toList();
    }

    /**
     * 获取qos等级</br>
     * qos降级</br>
     * 主题qos与消息qos对比，谁小选谁
     *
     * @param s1 主题qos
     * @param s2 消息qos
     * @return qos
     */
    private MqttQoS getQos(MqttQoS s1, MqttQoS s2) {
        return s1.value() < s2.value() ? s1 : s2;
    }

    /**
     * 从各种回复中判断是否完成一次完整的发布
     *
     * @param messageId 消息id
     */
    private void reply(String clientId, Integer messageId) {
        if (qosMap.containsKey(messageId)) {
            QosMessageInfo info = qosMap.get(messageId);
            info.count.getAndDecrement();
            info.record.put(clientId, true);
            // 判断订阅者是否全部收到
            if (info.count.get() <= 0) {
                switch (info.message.qosLevel()) {
                    case AT_LEAST_ONCE -> {
                        Log.debug("订阅者全部收到，向发布者发送qos1回复Ack");
                        info.endpoint.publishAcknowledge(info.message.messageId());
                    }
                    case EXACTLY_ONCE -> {
                        Log.debug("订阅者全部收到，向发布者发送qos2回复Com");
                        info.endpoint.publishComplete(info.message.messageId());
                    }
                }
                // 删除
                qosMap.remove(messageId);
            }
        }
    }

    protected void subscribeHandler(MqttEndpoint endpoint, MqttSubscribeMessage subscribe) {
        List<MqttQoS> grantedQosLevels = new ArrayList<>();
        for (MqttTopicSubscription subscription : subscribe.topicSubscriptions()) {
            String topic = subscription.topicName();
            // 排除空主题
            if (topic.isEmpty())
                continue;
            addTopic(topic, subscription.qualityOfService(), endpoint);
            Log.info("MQTT客户端：%s，订阅主题：%s，等级：%s".formatted(endpoint.clientIdentifier(), topic, subscription.qualityOfService()));
            grantedQosLevels.add(subscription.qualityOfService());
        }
        // 确认订阅请求
        endpoint.subscribeAcknowledge(subscribe.messageId(), grantedQosLevels);
    }

    protected void unsubscribeHandler(MqttEndpoint endpoint, MqttUnsubscribeMessage unsubscribe) {
        for (String topic : unsubscribe.topics()) {
            removeTopic(topic, endpoint);
            Log.debug("MQTT客户端：%s，取消订阅主题：%s".formatted(endpoint.clientIdentifier(), topic));
        }
        // 确认退订请求
        endpoint.unsubscribeAcknowledge(unsubscribe.messageId());
    }

    protected void publishHandler(MqttEndpoint endpoint, MqttPublishMessage message) {
        Log.debug("MQTT客户端：%s，收到消息：%s，指定主题：%s,等级：%s".formatted(endpoint.clientIdentifier(), message.payload().toString(Charset.defaultCharset()), message.topicName(), message.qosLevel()));
        // 消息qos分别处理
        switch (message.qosLevel()) {
            case AT_MOST_ONCE -> {
                // 存储消息
                qosMap.put(message.messageId(), new QosMessageInfo(endpoint, message));
                // 开始发布
                publishToSubscribers(message);
            }
            case AT_LEAST_ONCE -> {
                // 重复的就删除原来的，无意义操作，因为map.put就会覆盖
                // 但是为了直观还是留着了
                if (message.isDup())
                    qosMap.remove(message.messageId());
                // 存储消息
                qosMap.put(message.messageId(), new QosMessageInfo(endpoint, message));
                // 开始发布
                publishToSubscribers(message);
            }
            case EXACTLY_ONCE -> {
                // 若消息qos为2且该消息已被存储，则什么都不做，防止重复
                if (qosMap.containsKey(message.messageId())) return;
                // 存储消息
                qosMap.put(message.messageId(), new QosMessageInfo(endpoint, message));
                // 回复Rec
                Log.debug("向发布者发送qos2回复Rec %s".formatted(message.messageId()));
                endpoint.publishReceived(message.messageId());
            }
        }
    }

    protected void publishToSubscribers(MqttPublishMessage message) {
        // 获取主题下的链接
        List<TopicInfo> topicInfos = getTopicsForWildcard(message.topicName());

        // 写入消费和订阅记录
        QosMessageInfo messageInfo = qosMap.get(message.messageId());
        messageInfo.count = new AtomicInteger(topicInfos.size());
        messageInfo.isUsed = true;

        // 封装函数
        for (TopicInfo topicInfo : topicInfos) {
            MqttQoS qos = getQos(topicInfo.qos, message.qosLevel());
            Log.debug("MQTT客户端：%s，发布消息：%s，指定主题：%s,等级：%s".formatted(topicInfo.endpoint.clientIdentifier(), message.payload().toString(Charset.defaultCharset()), message.topicName(), qos));
            // qos0消息的id为负数，直接发布
            if (qos == MqttQoS.AT_MOST_ONCE) {
                // 删除记录
                qosMap.remove(message.messageId());
                // 发布
                topicInfo.endpoint.publish(message.topicName(), message.payload(), qos, message.isDup(), message.isRetain());
            } else {
                // 记录
                messageInfo.record.put(topicInfo.endpoint.clientIdentifier(), false);
                messageInfo.qos = qos;
                // 发布
                topicInfo.endpoint.publish(message.topicName(), message.payload(), qos, message.isDup(), message.isRetain(), message.messageId());
            }
        }
    }

    protected void publishAcknowledgeMessageHandler(MqttEndpoint endpoint, MqttPubAckMessage ackMessage) {
        Log.debug("收到Ack消息%s".formatted(ackMessage.messageId()));
        reply(endpoint.clientIdentifier(), ackMessage.messageId());
    }

    protected void publishReceivedMessageHandler(MqttEndpoint endpoint, MqttPubRecMessage recMessage) {
        Log.debug("收到Rec消息向客户端发送qos2消息回复Rel %s".formatted(recMessage.messageId()));
        endpoint.publishRelease(recMessage.messageId());
        reply(endpoint.clientIdentifier(), recMessage.messageId());
    }

    protected void publishReleaseMessageHandler(MqttEndpoint endpoint, MqttPubRelMessage relMessage) {
        Log.debug("收到Rel消息开始发布");
        QosMessageInfo info = qosMap.get(relMessage.messageId());
        if (!info.isUsed) {
            publishToSubscribers(info.message);
        } else {
            info.record.forEach((clientId, aBoolean) -> {
                // 将失败的抽取出来，交由重发池
                if (!aBoolean) {
                    MqttEndpoint failEndpoint = null;
                    for (MqttEndpoint mqttEndpoint : endpointList) {
                        if (mqttEndpoint.clientIdentifier().equals(clientId))
                            failEndpoint = mqttEndpoint;
                    }
                    if (!Objects.isNull(failEndpoint)) {
                        if (resendMap.containsKey(clientId)) {
                            resendMap.get(clientId).add(new ResendMessage(info));
                        } else {
                            List<ResendMessage> list = new ArrayList<>();
                            list.add(new ResendMessage(info));
                            resendMap.put(clientId, list);
                        }
                        // 断开链接
                        closeHandler(failEndpoint);
                    }
                }
            });
            // 删除记录，回复结束
            qosMap.remove(relMessage.messageId());
            endpoint.publishComplete(relMessage.messageId());
        }
    }

    protected void closeHandler(MqttEndpoint endpoint) {
        Log.info("MQTT客户端：%s断开链接".formatted(endpoint.clientIdentifier()));
        // 删除
        endpointList.remove(endpoint);
        // 更新订阅池
        topicMap.forEach((topic, topicInfos) -> topicInfos.removeIf(topicInfo -> topicInfo.endpoint.clientIdentifier().equals(endpoint.clientIdentifier())));
    }

    protected void connectMessageHandler(MqttEndpoint endpoint) {
        // 完全一样不作为
        if (endpointList.contains(endpoint)) return;
        endpoint.accept();
        MqttEndpoint deleted = null;
        // 找到旧的
        for (MqttEndpoint mqttEndpoint : endpointList) {
            if (mqttEndpoint.clientIdentifier().equals(endpoint.clientIdentifier())) {
                // 断开
                mqttEndpoint.disconnect(MqttDisconnectReasonCode.NORMAL, new MqttProperties());
                // v5会通往close处理器进行删除，但是为了兼容v3，后面多进行一次删除
                mqttEndpoint.close();
                deleted = mqttEndpoint;
            }
        }
        // 添加新的
        if (!Objects.isNull(deleted)) {
            // v3删除
            endpointList.remove(deleted);
            endpointList.add(endpoint);
            // 更新订阅池
            topicMap.forEach((topic, topicInfos) -> {
                for (TopicInfo topicInfo : topicInfos) {
                    if (topicInfo.endpoint.clientIdentifier().equals(endpoint.clientIdentifier())) {
                        topicInfo.endpoint = endpoint;
                    }
                }
            });
        } else endpointList.add(endpoint);

        // 检查重发池，重发消息
        if (resendMap.containsKey(endpoint.clientIdentifier())) {
            for (ResendMessage resendMessage : resendMap.get(endpoint.clientIdentifier())) {
                publish(endpoint, resendMessage);
            }
        }

        Log.info("MQTT客户端：%s链接通过".formatted(endpoint.clientIdentifier()));
    }

}
