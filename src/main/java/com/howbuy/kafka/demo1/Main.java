package com.howbuy.kafka.demo1;

/**
 * Created by luohui on 15/10/21.
 */
public class Main {

    public static void main(String[] args) {
        Boolean isAsync = true;
        new Producer(KafkaProperties.topic,isAsync).start();
        new Consumer(KafkaProperties.topic).start();
    }
}
