package com.howbuy.kafka.demo1;

/**
 * Created by luohui on 15/10/21.
 */
public interface KafkaProperties {
    final static String zkConnect = "127.0.0.1:2181";
    final static String groupId = "group1";
    final static String topic = "test111";
    final static String kafkaServerURL = "127.0.0.1";
    final static int kafkaServerPort = 9092;
    final static String clientId = "SimpleConsumerDemoClient";
}
