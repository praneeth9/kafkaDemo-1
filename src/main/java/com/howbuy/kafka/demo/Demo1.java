package com.howbuy.kafka.demo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
public class Demo1{
	
	public static void main(String[] args) {
		Boolean isAsync = true;
		new Producer(KafkaProperties.topic,isAsync).start();
		new Consumer(KafkaProperties.topic).start();
	}
	
	static interface KafkaProperties{
	    final static String zkConnect = "127.0.0.1:2181";
	    final static String groupId = "group1";
	    final static String topic = "test111";
	    final static String kafkaServerURL = "127.0.0.1";
	    final static int kafkaServerPort = 9092;
	    final static String clientId = "SimpleConsumerDemoClient";
	}
	
	static class Producer extends Thread{
		private final KafkaProducer<String, String> producer;
		private final String topic;
		private final Boolean isAsync;
		  
		public Producer(String topic,boolean isAsync) {
		    Properties props = new Properties();
		    props.put("bootstrap.servers",KafkaProperties.kafkaServerURL+":"+KafkaProperties.kafkaServerPort);
		    props.put("client.id", KafkaProperties.clientId);
		    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		    producer = new KafkaProducer<String, String>(props);
		    this.topic = topic;
		    this.isAsync = isAsync;
		}

		@Override
		public void run() {
			int messageNo = 1;
			while(true){
				String messageStr = "message_" + messageNo;
				long start = System.currentTimeMillis();
				if(isAsync){
					producer.send(new ProducerRecord<String, String>(topic, String.valueOf(messageNo), messageStr), new DemoCallback(messageNo, messageStr, start));
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
	
	static class DemoCallback implements Callback{
		private long startTime;
		private int key;
		private String message;
		
		public DemoCallback(int messageNo, String messageStr, long start) {
		    this.startTime = start;
		    this.key = messageNo;
		    this.message = messageStr;
		}

		public void onCompletion(RecordMetadata metadata, Exception exception) {
			long elapsedTime = System.currentTimeMillis() - startTime;
			if(metadata!=null){
				System.out.println("message ("+key+","+message+") send to partition "+metadata.partition()+",offset "+metadata.offset()+" in "+elapsedTime +" ms");
			}else{
				exception.printStackTrace();
			}
		}
	}
	
	static class Consumer extends Thread{
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

		@Override
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
}
