package com.howbuy.kafka.demo1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
/**
 * Created by luohui on 15/10/21.
 */
public class Producer extends Thread {
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private final Boolean isAsync;

    public Producer(String topic, Boolean isAsync) {
        this.topic = topic;
        this.isAsync = isAsync;

        Properties props = new Properties();
        props.put("bootstrap.servers",KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort);
        props.put("client.id", KafkaProperties.clientId);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<String, String>(props);
    }

    public void run() {
        int messageNo = 1;
        while(true){
            String messageStr = "message_" + messageNo;
            long start = System.currentTimeMillis();
            if(isAsync){
                producer.send(new ProducerRecord<String, String>(topic, String.valueOf(messageNo), messageStr), new DemoCallback(start, messageNo, messageStr));
            }else{
                try {
                    producer.send(new ProducerRecord<String, String>(topic, String.valueOf(messageNo), messageStr)).get();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    e.printStackTrace();
                }
            }
            ++messageNo;
        }
    }
}
