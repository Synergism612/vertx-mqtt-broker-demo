package com.example;

import com.example.mqtt.MQTTBroker;
import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import jakarta.inject.Inject;


@QuarkusMain
public class MyQuarkusApplication implements QuarkusApplication {
    private final MQTTBroker mqttBroker;

    @Inject
    public MyQuarkusApplication(MQTTBroker mqttBroker) {
        this.mqttBroker = mqttBroker;
    }


    @Override
    public int run(String... args) {
        // 初始化
        mqttBroker.init();
        // 开始监听
        mqttBroker.listen();
        Quarkus.waitForExit();
        return 0;
    }
}
