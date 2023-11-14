# mqtt Broker

## 介绍

该案例项目基于vertx

抽取自我的私有项目

是我自己摸索，按照mqtt协议制作，请多多包含，欢迎探讨

根据vertx.mqtt.MqttServer基本实现mqtt协议

支持qos等级消息

支持qos2消息在异常断开后暂存，等待设备重连后发送

## 注意

本项目只作为案例，不保证性能等事项

开启ws协议模式后无法使用mqtt协议链接

## 使用

该项目强依赖于`io.vertx.mqtt`包，使用quarkus框架完成，但并不依赖框架

只要加入该包的依赖，任意框架都可以使用

`MyQuarkusApplication`为项目启动类

```
// 初始化
mqttBroker.init();
// 开始监听
mqttBroker.listen();
```

### 初始化

`mqttBroker.init();`

该函数首先创建vertx

`Vertx vertx = Vertx.vertx();`

创建了所需要的数据存储池

```
endpointList = new ArrayList<>();
topicMap = new HashMap<>();
qosMap = new HashMap<>();
resendMap = new HashMap<>();
```

创建mqtt服务

```
server = MqttServer.create(vertx, new MqttServerOptions()
// 开启基于ws的mqtt
// .setUseWebSocket(true)
.setPort(MQTT_PORT)
);
```

写入链接处理函数

`server.endpointHandler(this::endpointHandler);`

### 开始监听

`mqttBroker.listen();`

该函数调用服务监听函数并处理错误

```
server.listen(asyncResult -> {
    if (!asyncResult.succeeded()) {
        Log.error("MQTT服务端口任务启动失败，请检查端口占用 %s".formatted(asyncResult.cause().getMessage()));
    }
});
```

## 配置

### mqtt配置

#### 端口

mqtt监听端口在

`com/example/mqtt/MQTTConfig.java`

中配置

#### 使用ws

在

`com/example/mqtt/MQTT.java`的`init()`

中解除注释即可

`//.setUseWebSocket(true)`

开启ws协议模式后无法使用mqtt协议链接

### 框架配置

本框架的端口号在

`src/main/resources/application.properties`

中修改

## 链接

推荐使用`MQTTX`进行测试与链接

## 其它

我的实现想法基本都在

`com.example.mqtt.MQTT.endpointHandler()`

这个函数的注释中