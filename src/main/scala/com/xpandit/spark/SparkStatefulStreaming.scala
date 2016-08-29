package com.xpandit.spark

import _root_.kafka.serializer.StringDecoder
import com.xpandit.config.QueryConfigs
import com.xpandit.data.EventData
import com.xpandit.mutations.RowDataUtils
import com.xpandit.utils.Constants
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.josql.{Query, QueryParseException}


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)
  val CheckpointPath = "/tmp/spark/checkpoint"

  val queryConfigs = new QueryConfigs()
  var failedQueryAccum : Accumulator[Long] = null

  //TODO replace following hardcoded maps
  val columnIndex: Map[String, Int] = Map.empty
  val columnType: Map[String, String] = Map.empty





  def main(args: Array[String]): Unit = {

    val ssc = setSparkStreamingContext()

    failedQueryAccum = ssc.sparkContext.accumulator(0L, "Failed filtering query") //to find out whether or not filtering query has failed

    var isBatchEmpty: Boolean = false   //does current batch has events?

    val kafkaStream = kafkaStreamConnect(ssc).map( (t) => createEventData(t._2) )





    val stream = kafkaStream.transform{ (rdd) =>
      isBatchEmpty = rdd.isEmpty()
      queryConfigs.updateConfigs()
      rdd
    }


    //TODO
    events.updateStateByKey(updateFunction).foreachRDD((rdd) => {

      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
      import sqlContext.implicits._

      val eventsRDD = rdd.flatMap { case (key, value) =>
        value.toStream.map(event => new MedicalConsultationWaitingPacientEventCase(event.healthServiceNumber, event.name, event.age, event.receptionUrgency, event.requestTime))
      }

      eventsRDD.toDF().registerTempTable("waiting_patients")

      sqlContext.sql(queryConfigs.getQuery(false)).show()


      if(failedQueryAccum.value == 0L && !isBatchEmpty){  //filtering success - query is not malformed and we can save current configs
        queryConfigs.saveConfigAsLastSuccessful()
      }

      failedQueryAccum.setValue(0L) //resetting accumulator

    })

    ssc.start()
    ssc.awaitTermination()
  }


  def setSparkStreamingContext(): StreamingContext = {

    val conf = new SparkConf()
      .setAppName("SparkStatefulStreaming")
      .setMaster("local[4]")
      .set("spark.driver.memory", "2g")
      .set("spark.streaming.kafka.maxRatePerPartition", "50000")
      .set("spark.streaming.backpressure.enabled", "true")

    val ssc = new StreamingContext(conf, Seconds(5))
    ssc.checkpoint("/tmp/spark/checkpoint")
    ssc
  }

  def kafkaStreamConnect(ssc: StreamingContext) : InputDStream[(String, String)] = {
    val kafkaParams: Map[String, String] = Map("metadata.broker.list" -> "localhost:9092, localhost:9093, localhost:9094, localhost:9095",
      "auto.offset.reset" -> "smallest")

    KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, scala.collection.immutable.Set("events"))
  }


  def createEventData(strEvent: String): (String, (EventData) = {

    val rowData = RowDataUtils.processRowData(strEvent, attributes, PKsList, null, Constants.parserJSON, toLowerCase = true)

    //TODO stopped here...

    (rowData.getFormattedPKs, (rowData, rowData.getOperationPos)

  }

  /**
    * Function to apply in updateStateByKey
    *
    * @param newValues - Seq of events arrived for each key in current batch
    * @param state  - state mantained for each key over multiple batches
    * @return - events that  will be passed as state in the next batch
    */
  def updateFunction(newValues: Seq[MedicalConsultationWaitingPacientEvent], state: Option[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]]) : Option[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]] =  {
    val updatedStateList = if (state.isEmpty) new java.util.ArrayList[MedicalConsultationWaitingPacientEvent]() else state.get

    println(queryConfigs.getFilteringWhereClause(false))

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

      if (!updatedStateList.isEmpty && updatedStateList.get(0).requestTime <= mostRecentEvent.requestTime) {
        //replace if most recent arrived event is more recent than current held one
        updatedStateList.add(mostRecentEvent)
        updatedStateList.remove(0)
      }
      else {
        updatedStateList.add(mostRecentEvent)
      }

      try {
        //filtering event objects within the 'prevStateEventsList'

        val queryStr = s"SELECT * FROM com.xpandit.spark.MedicalConsultationWaitingPacientEvent WHERE ${queryConfigs.getFilteringWhereClause(false)}"
        val filteredEventsList = applySQLQueryOnJavaCollection(queryStr, updatedStateList).asInstanceOf[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]]

        return Some(filteredEventsList)
      }
      catch{
        case e : QueryParseException =>
          failedQueryAccum.add(1)

          if(queryConfigs.hasSuccessfulConfig()) {  //current filtering query is malformed so we will use last successful configuration queries
            val queryStr = s"SELECT * FROM com.xpandit.spark.MedicalConsultationWaitingPacientEvent WHERE ${queryConfigs.getFilteringWhereClause(true)}"
            val filteredEventsList = applySQLQueryOnJavaCollection(queryStr, updatedStateList).asInstanceOf[java.util.ArrayList[MedicalConsultationWaitingPacientEvent]]

            return Some(filteredEventsList)
          }
          else{ //current filtering query is malformed and there is no successful configuration saved. Exiting
            throw new RuntimeException("Malformed filtering query specified.")
          }
      }
    }

    //if returns here there are no events for this key in this batch or sql-filtering failed. We just pass the events to the next state
    Some(updatedStateList)
  }


  /**
    * Applies SQL queries on Java collections using JoSQL library
    *
    * @param strQuery - query to apply ex. "SELECT * FROM com.package.Item  where price > 10.0"
    * @param list - list of objects to filter
    * @return - list of filtered objects
    */
  def applySQLQueryOnJavaCollection(strQuery: String, list: java.util.List[_]) : java.util.List[_] = {
    val query = new Query()
    query.parse(strQuery)
    query.execute(list).getResults
  }
}

//case class just defined to 'Inferring the Schema Using Reflection'
case class MedicalConsultationWaitingPacientEventCase(healthServiceNumber: Long, name: String, age: Int, receptionUrgency: Int, requestTime: Long)
