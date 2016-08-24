package com.xpandit.utils

import com.xpandit.kafka.SimpleKafkaProducer

/**
  * Created by xpandit on 8/17/16.
  */
object KafkaProducerHolder {

  Runtime.getRuntime.addShutdownHook(new Thread() {
    override def run(): Unit = if (producer != null) producer.close()
  })

  @transient lazy val producer = new SimpleKafkaProducer("localhost:9092")

}
