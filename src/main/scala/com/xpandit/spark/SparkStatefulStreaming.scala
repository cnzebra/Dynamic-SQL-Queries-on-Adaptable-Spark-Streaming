package com.xpandit.spark

import _root_.kafka.serializer.StringDecoder
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.josql.Query

import scala.collection.JavaConversions._
import scala.io.Source

object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)
  val CheckpointPath = "/tmp/spark/checkpoint"
  val DynamicConfigPath = "src/main/resources/streaming-query.conf"

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


    val updateFunc = (newValues: Seq[MedicalConsultationWaitingPacientEvent], state: Option[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]]) => {

      val prevStateEventsList = if (state.isEmpty) new java.util.ArrayList[MedicalConsultationWaitingPacientEvent]() else state.get

      if(newValues.nonEmpty) {    //if there is any event in current batch

        var maxTimestamp = 0L
        var mostRecentEvent : MedicalConsultationWaitingPacientEvent = null

        newValues.foreach { e => //find more recent event (biggest timestamp)

          if (mostRecentEvent == null) {
            maxTimestamp = e.requestTime
            mostRecentEvent = e
          }
          else if (e.requestTime >= maxTimestamp) {
            maxTimestamp = e.requestTime
            mostRecentEvent = e
          }
        }


        if (!prevStateEventsList.isEmpty && prevStateEventsList.get(0).requestTime <= mostRecentEvent.requestTime) {
          //replace if most recent arrived event is more recent than current held one
          prevStateEventsList.add(mostRecentEvent)
          prevStateEventsList.remove(0)
        }
        else {
          prevStateEventsList.add(mostRecentEvent)
        }

        //retrieving parameters from config file
        val dynamicConfig = retrieveParamsFromFile(DynamicConfigPath)   //TODO change config location (file for testing purposes only)

        var query = new Query()
        val test = s"SELECT * FROM com.xpandit.spark.MedicalConsultationWaitingPacientEvent WHERE ${dynamicConfig.get("filtering_where_clause").get}"
        println(test)
        query.parse(s"SELECT * FROM com.xpandit.spark.MedicalConsultationWaitingPacientEvent WHERE ${dynamicConfig.get("filtering_where_clause").get}")

        val execute = query.execute(prevStateEventsList)
        Some(execute.getResults.asInstanceOf[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]])

      }
      else{   //otherwise just pass the previous state
        Some(prevStateEventsList)
      }
    }

    val updatedStream = events.updateStateByKey(updateFunc)

    updatedStream.foreachRDD((rdd) => {

      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val eventsRDD = rdd.flatMap { case (key, value) =>
        value.toStream.map(event => new MedicalConsultationWaitingPacientEventCase(event.healthServiceNumber, event.name, event.age, event.receptionUrgency, event.requestTime))
      }

      eventsRDD.toDF().registerTempTable("waiting_patients")

      //retrieving parameters from config file
      val dynamicConfig = retrieveParamsFromFile(DynamicConfigPath)   //TODO change config location (file for testing purposes only)

      sqlContext.sql(dynamicConfig.get("query").get).show()
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def createEvent(strEvent: String): (String, MedicalConsultationWaitingPacientEvent) = {

    val eventData = strEvent.split('|')

    val healthServiceNumber = eventData(0).toLong
    val name = eventData(1).toString
    val age = eventData(2).toInt
    val receptionUrgency = eventData(3).toInt
    val requestTime = eventData(4).toLong

    val event = new MedicalConsultationWaitingPacientEvent(healthServiceNumber, name, age, receptionUrgency, requestTime)
    (event.patientID, event)
  }

  def retrieveParamsFromFile(dynamicConfigPath: String) = {
    var params: Map[String, String] = Map.empty

    for (line <- Source.fromFile(dynamicConfigPath).getLines) {
      var splitted = line.split("=:")
      params += splitted(0) -> splitted(1)
    }
    params
  }
}

//case class just defined to 'Inferring the Schema Using Reflection'
case class MedicalConsultationWaitingPacientEventCase(healthServiceNumber: Long, name: String, age: Int, receptionUrgency: Int, requestTime: Long)
