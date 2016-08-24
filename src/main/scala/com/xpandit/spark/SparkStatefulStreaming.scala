package com.xpandit.spark

import _root_.kafka.serializer.StringDecoder
import com.xpandit.utils.KafkaProducerHolder
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.json.{JSONArray, JSONObject}

import scala.collection.mutable.Set


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("SparkStatefulStreaming")
      .setMaster("local[4]")
      .set("spark.driver.memory", "2g")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/tmp/spark/checkpoint") // set checkpoint directory


    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092, localhost:9093, localhost:9094, localhost:9095",
                                               "auto.offset.reset" -> "smallest")

    val kafkaStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, scala.collection.immutable.Set("events"))

    val events = kafkaStream.map((tuple) => createEvent(tuple._2))

    val groupedEvents = events.transform((rdd) => rdd.groupBy(_.key))  //TODO change key

    //mapWithState function
    val updateState = (batchTime: Time, key: String, newEvents: Option[Iterable[Event]], state: State[Set[Event]]) => {

      var eventsSet = state.getOption().getOrElse(Set.empty)  //previous state events


      //TODO dynamic sql-like filtering over eventsSet


      state.update(eventsSet)

      Some((key, eventsSet))
    }

    val spec = StateSpec.function(updateState)
    val mappedStatefulStream = groupedEvents.mapWithState(spec)



    mappedStatefulStream.foreachRDD( (rdd) => {

      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val events = rdd.flatMap { case (key, value) => {
        value.map( event => ... ))          //TODO map to case class to get schema
      } }

      events.toDF().registerTempTable("events")

      sqlContext.sql("select * from events").show()

    })


    mappedStatefulStream.print()

    ssc.start()
    ssc.awaitTermination()
  }

  def createEvent(strEvent: String): Event = {

    //TODO parse strEvent

    new Event(123, "key")
  }
}
