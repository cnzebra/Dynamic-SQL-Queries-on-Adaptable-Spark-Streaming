package com.xpandit.kafka;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;

import java.io.File;


/**
 * Created by xpandit on 7/29/16.
 */
public class EventKafkaProducer {

    private static final String BROKERS = "localhost:9092";
    private static final String TOPIC = "events";

    private static final int INPUT_SIZE = 1000000;           //number of events being produced to kafka each INTERVAL_TIME_MS
    private static final int INTERVAL_TIME_MS = 1000;


    public static void main(String[] args){

        SimpleKafkaProducer kafkaProducer = new SimpleKafkaProducer(BROKERS);
        LineIterator it = null;

        try {

            it = FileUtils.lineIterator(new File("src/main/resources/input/events.txt"), "UTF-8");

            int currentEvent = 0;
            int nextStop = INPUT_SIZE;

            while(it.hasNext()) {

                while(currentEvent < nextStop) {

                    String line = it.nextLine();
                    String msg = System.currentTimeMillis() + "|" + line;

                    kafkaProducer.sendMessage(TOPIC, msg);
                    currentEvent++;
                }

                System.out.println("Injected " + INPUT_SIZE + " events to Kafka  [Total: " + currentEvent + "]");

                nextStop += INPUT_SIZE;

                //Thread.sleep(INTERVAL_TIME_MS);
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
        finally {
            kafkaProducer.close();
            LineIterator.closeQuietly(it);
        }
    }
}
