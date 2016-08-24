package com.xpandit.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * Created by xpandit on 8/17/16.
 */
public class SimpleKafkaProducer {


    private KafkaProducer<String,String> producer;

    public SimpleKafkaProducer(String brokers) {

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        producer = new KafkaProducer<>(props);
    }

    public void sendMessage(String topic, String msg){
        ProducerRecord<String,String> producerRecord = new ProducerRecord<>(topic, null/*key*/, msg);
        producer.send(producerRecord);          //async send
    }

    public void close(){
        producer.close();
    }
}
