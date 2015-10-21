package com.howbuy.kafka.demo1;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
/**
 * Created by luohui on 15/10/21.
 */
public class Consumer extends Thread {
    private final ConsumerConnector consumer;
    private final String topic;

    public Consumer(String topic) {
        this.topic = topic;
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(createConsumerConfig());
    }

    private ConsumerConfig createConsumerConfig() {
        Properties props = new Properties();
        props.put("zookeeper.connect", KafkaProperties.zkConnect);
        props.put("group.id", KafkaProperties.groupId);//当前消费者group
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, 1);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
        for (MessageAndMetadata<byte[], byte[]> messageAndMetadata : stream) {
            try {
                String key = new String(messageAndMetadata.key());
                String message = new String(messageAndMetadata.message());
                System.out.println("Received message: (" + key +", "+ message + ")");
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
