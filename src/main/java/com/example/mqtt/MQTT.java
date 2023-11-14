package com.example.mqtt;

import io.quarkus.logging.Log;
import io.vertx.core.Vertx;
import io.vertx.mqtt.MqttEndpoint;
import io.vertx.mqtt.MqttServer;
import io.vertx.mqtt.MqttServerOptions;
import io.vertx.mqtt.messages.*;

import java.util.*;

import static com.example.mqtt.MQTTConfig.MQTT_PORT;

public abstract class MQTT {

    /**
     * 核心服务
     */
    private MqttServer server;

    /**
     * 链接池
     */
    protected List<MqttEndpoint> endpointList;

    /**
     * 主题池
     */
    protected Map<String, ArrayList<TopicInfo>> topicMap;

    /**
     * qos存储池<br/>
     * key:消息id<br/>
     * value:消息信息记录
     */
    protected Map<Integer, QosMessageInfo> qosMap;

    /**
     * 重发记录池<br/>
     * key:用户id<br/>
     * value:需要重发的消息
     */
    protected Map<String, List<ResendMessage>> resendMap;

    public void init() {
        Log.info("MQTT服务初始化");
        Vertx vertx = Vertx.vertx();
        endpointList = new ArrayList<>();
        topicMap = new HashMap<>();
        qosMap = new HashMap<>();
        resendMap = new HashMap<>();
        server = MqttServer.create(vertx, new MqttServerOptions()
                // 开启基于ws的mqtt
                // .setUseWebSocket(true)
                .setPort(MQTT_PORT)
        );
        server.endpointHandler(this::endpointHandler);
        Log.info("MQTT服务初始化完毕");
    }

    /**
     * 客户端处理函数
     * <h2>链接与断开</h2>
     * 链接时添加到连接池<br/>
     * 断开时从连接池删除并清空其点阅池相关
     * <h2>订阅与退订</h2>
     * 支持客户端对主题的订阅与退订<br/>
     * 使用{@code Map<String, ArrayList<TopicInfo>> topicMap}作为存储<br/>
     * 存储所有订阅相关信息</br>
     * <h2>消息转发</h2>
     * <ul>当发布qos=0</ul>
     * <li>发布者-Publish(qos=0)->服务器</li>
     * <li>服务器-Publish(订阅qos)->订阅者</li>
     * <ul>当发布qos=1</ul>
     * <li>发布者-Publish(qos=1)->服务器</li>
     * <li>服务器-Publish(订阅qos)->订阅者</li>
     * <li>服务器-PublishAcknowledge收到->发布者</li>
     * <ul>当发布qos=2</ul>
     * <li>发布者-Publish(qos=2)->服务器</li>
     * <li>服务器-PublishReceived接受->发布者</li>
     * <li>发布者-PublishRelease释放->服务器</li>
     * <li>服务器-Publish(订阅qos)->订阅者</li>
     * <li>服务器-PublishComplete结束->发布者</li>
     *
     * @param endpoint 客户端
     */
    private void endpointHandler(MqttEndpoint endpoint) {
        // 显示主要连接信息
        Log.info("MQTT客户端：%s尝试链接".formatted(endpoint.clientIdentifier()));

        // 订阅主题
        endpoint.subscribeHandler(subscribe -> subscribeHandler(endpoint, subscribe));

        // 退订主题
        endpoint.unsubscribeHandler(unsubscribe -> unsubscribeHandler(endpoint, unsubscribe));

        // 收到发布消息处理
        endpoint.publishHandler(message -> publishHandler(endpoint, message));

        // 接收Ack消息处理
        endpoint.publishAcknowledgeMessageHandler(ackMessage -> publishAcknowledgeMessageHandler(endpoint, ackMessage));

        // 接收Rec消息处理
        endpoint.publishReceivedMessageHandler(recMessage -> publishReceivedMessageHandler(endpoint, recMessage));

        // 接收Rel消息处理
        endpoint.publishReleaseMessageHandler((relMessage) -> publishReleaseMessageHandler(endpoint, relMessage));

        // 断开处理 v3 ws不需要区分
        // endpoint.disconnectHandler(v -> closeHandler(endpoint));
        // 断开处理 v5
        endpoint.closeHandler(v -> closeHandler(endpoint));

        // 接受远程客户端连接
        connectMessageHandler(endpoint);
    }


    /**
     * mqtt服务端口监听
     */
    public void listen() {
        Log.info("MQTT服务监听端口任务启动");
        server.listen(asyncResult -> {
            if (!asyncResult.succeeded()) {
                Log.error("MQTT服务端口任务启动失败，请检查端口占用 %s".formatted(asyncResult.cause().getMessage()));
            }
        });
    }

    /**
     * 订阅主题处理
     *
     * @param endpoint  链接
     * @param subscribe 订阅消息
     */
    abstract void subscribeHandler(MqttEndpoint endpoint, MqttSubscribeMessage subscribe);

    /**
     * 退订主题处理
     *
     * @param endpoint    链接
     * @param unsubscribe 退订消息
     */
    abstract void unsubscribeHandler(MqttEndpoint endpoint, MqttUnsubscribeMessage unsubscribe);

    /**
     * 收到消息处理
     *
     * @param endpoint 链接
     * @param message  消息
     */
    abstract void publishHandler(MqttEndpoint endpoint, MqttPublishMessage message);

    /**
     * 发布给订阅者
     *
     * @param message 需要发布的消息
     */
    abstract void publishToSubscribers(MqttPublishMessage message);

    /**
     * 收到Ack消息处理<br/>
     * 向订阅者发送qos1消息时会收到Ack消息<br/>
     * 不需要回复订阅者
     *
     * @param ackMessage ack消息
     */
    abstract void publishAcknowledgeMessageHandler(MqttEndpoint endpoint, MqttPubAckMessage ackMessage);

    /**
     * 收到Rec消息处理<br/>
     * 向订阅者发送qos2消息时会收到Rec消息<br/>
     * 需要回复订阅者Rel消息
     *
     * @param endpoint   订阅者链接
     * @param recMessage rec消息
     */
    abstract void publishReceivedMessageHandler(MqttEndpoint endpoint, MqttPubRecMessage recMessage);

    /**
     * 收到Rel消息处理<br/>
     * 接收到发布者qos2确认的Rel消息<br/>
     * 接收到该消息开始进行向订阅者发布
     *
     * @param relMessage rel消息
     */
    abstract void publishReleaseMessageHandler(MqttEndpoint endpoint, MqttPubRelMessage relMessage);

    /**
     * 断开处理
     *
     * @param endpoint 链接
     */
    abstract void closeHandler(MqttEndpoint endpoint);

    /**
     * 链接处理
     *
     * @param endpoint 链接
     */
    abstract void connectMessageHandler(MqttEndpoint endpoint);

}
