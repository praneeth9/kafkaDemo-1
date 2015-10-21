package com.howbuy.kafka.demo1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.RecordMetadata;
/**
 * Created by luohui on 15/10/21.
 */
public class DemoCallback implements Callback {
    private long startTime;
    private int key;
    private String message;

    public DemoCallback(long startTime, int key, String message) {
        this.startTime = startTime;
        this.key = key;
        this.message = message;
    }

    public void onCompletion(RecordMetadata metadata, Exception e) {
        long elapsedTime = System.currentTimeMillis() - startTime;
        if(metadata!=null){
            System.out.println("message ("+key+","+message+") send to partition "+metadata.partition()+",offset "+metadata.offset()+" in "+elapsedTime +" ms");
        }else{
            e.printStackTrace();
        }
    }
}
