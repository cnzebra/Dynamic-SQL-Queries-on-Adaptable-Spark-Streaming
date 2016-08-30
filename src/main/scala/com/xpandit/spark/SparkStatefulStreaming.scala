package com.xpandit.spark

import _root_.kafka.serializer.StringDecoder
import com.xpandit.config.QueryConfigs
import com.xpandit.data.EventData
import com.xpandit.mutations.{OperationType, RowData, RowDataInsert, RowDataUtils}
import com.xpandit.utils.{Constants, SQLOnJavaCollections, TypeConverter}
import org.apache.log4j.Logger
import org.apache.spark._
import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka._
import org.josql.QueryParseException


object SparkStatefulStreaming {

  val logger = Logger.getLogger(SparkStatefulStreaming.getClass)
  val CheckpointPath = "/tmp/spark/checkpoint"

  val queryConfigs = new QueryConfigs()
  var failedQueryAccum : Accumulator[Long] = null


  //TODO replace following hardcoded maps
  val tableDescription = getTableDescription
  val tablePrivateKeys = tableDescription._1
  val tableColumnNamesArray = tableDescription._2
  val tableColumnTypesArray = tableDescription._3

  val columnIndex = {
    var map : Map[String, Int] = Map.empty

    for(i <- tableColumnNamesArray.indices){
      map += tableColumnNamesArray(i) -> i
    }
    map
  }

  val columnScalaType = {
    var map : Map[String, String] = Map.empty

    for(i <- tableColumnTypesArray.indices){
      map += tableColumnNamesArray(i) -> TypeConverter.hiveToScalaStrType(tableColumnTypesArray(i))
    }
    map
  }
  //UNTIL HERE


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

    //holding only most recent event for each key, discarding the rest
    val events = stream.reduceByKey( (e1, e2) => if(e1.rowData.getOperationPos > e2.rowData.getOperationPos) e1 else e2 )

    events.updateStateByKey(updateFunction).print()


//    //TODO
//    events.updateStateByKey(updateFunction).foreachRDD( (rdd) => {
//
//      val sqlContext = SQLContext.getOrCreate(rdd.sparkContext)
//      import sqlContext.implicits._
//
//      val eventsRDD = rdd.flatMap { case (key, value) =>
//        value.toStream.map(event => new MedicalConsultationWaitingPacientEventCase(event.healthServiceNumber, event.name, event.age, event.receptionUrgency, event.requestTime))
//      }
//
//      eventsRDD.toDF().registerTempTable("waiting_patients")
//
//      sqlContext.sql(queryConfigs.getQuery(false)).show()
//
//      if(failedQueryAccum.value == 0L && !isBatchEmpty){  //filtering success - query is not malformed and we can save current configs
//        queryConfigs.saveConfigAsLastSuccessful()
//      }
//
//      failedQueryAccum.setValue(0L) //resetting accumulator
//
//    })

    ssc.start()
    ssc.awaitTermination()
  }


  /**
    * Get table description from source ex. Hive
    *
    * @return (privateKeyArray, attributes, types)
    */
  def getTableDescription : (Array[String], Array[String], Array[String]) = {
    //TODO replace hardcoded

    val tablePrivateKeys = Array("HEALTHSERVICENUMBER", "NAME")
    val tableAttributeNames = Array("HEALTHSERVICENUMBER", "NAME", "AGE", "RECEPTIONURGENCY", "REQUESTTIME")
    val tableAttributeTypes = Array("BIGINT", "STRING", "INT", "INT", "BIGINT")

    (tablePrivateKeys, tableAttributeNames, tableAttributeTypes)
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

  def createEventData(strEvent: String): (String, (EventData)) = {

    //val rowData = RowDataUtils.processRowData(strEvent, columnNamesArray, columnPrivateKeys , null, Constants.parserJSON, toLowerCase = true)
    val rowData = createRowData(strEvent)

    val eventData = new EventData(columnIndex, columnScalaType, rowData)

    (eventData.getPrivateKeysValues, eventData)
  }

  //TEMPORARY function
  def createRowData(strEvent: String) : RowData = {
    val eventFields = strEvent.split('|')
    new RowDataInsert(eventFields, tablePrivateKeys, eventFields(eventFields.length - 1).toLong)
  }

  /**
    * Function to apply in updateStateByKey
    *
    * @param newValues - Seq of events arrived for each key in current batch
    * @param state  - state mantained for each key over multiple batches
    * @return - events that  will be passed as state in the next batch
    */
  def updateFunction(newValues: Seq[EventData], state: Option[java.util.ArrayList[EventData]]) : Option[java.util.ArrayList[EventData]] =  {
    val updatedStateList = if (state.isEmpty) new java.util.ArrayList[EventData]() else state.get

    if(newValues.nonEmpty) {  //if there is any event in current batch

      if (newValues.head.rowData.getOperationType == OperationType.DELETE.toString) return None //Delete operation, nothing will be passed to the next state to this key

      updatedStateList.clear() //replacing event received in current batch with previous passed state event [INSERT or UPDATE] operation
      updatedStateList.add(newValues.head)
    }

    if(!updatedStateList.isEmpty) { //apply filter on event

      try {
        //filtering event objects within the 'updatedStateList'
        val filteredEventsList = performEventDataSQLFiltering(updatedStateList, updatedStateList.get(0), lastSuccessfulConfiguration = false)
        return Some(filteredEventsList)
      }
      catch {
        case e: QueryParseException =>
          failedQueryAccum.add(1)

          if (queryConfigs.hasSuccessfulConfig) {
            //current filtering query is malformed so we will use last successful configuration queries

            val filteredEventsList = performEventDataSQLFiltering(updatedStateList, updatedStateList.get(0), lastSuccessfulConfiguration = true)
            return Some(filteredEventsList)
          }
          else {
            //current filtering query is malformed and there is no successful configuration saved. Exiting
            throw new RuntimeException("Malformed filtering query specified.")
          }
      }
    }

    //if returns here there are no events for this key in this batch or sql-filtering failed. We just pass the events to the next state
    Some(updatedStateList)
  }


  /**
    * Performs filtering on given objects with previously user defined sql where clause
    *
    * @param objList - list of objects to filter
    * @param eventData - EventData that represents each e event object
    * @param lastSuccessfulConfiguration - use last successfully applied user defined query
    * @return - list of filtered objects
    */
  def performEventDataSQLFiltering(objList: java.util.ArrayList[EventData], eventData: EventData, lastSuccessfulConfiguration: Boolean): java.util.ArrayList[EventData] = {
    val replacedWhereClause = SQLOnJavaCollections.buildWhereClause(queryConfigs.getFilteringWhereClause(false), eventData)
    var query = SQLOnJavaCollections.buildQuery("com.xpandit.data.EventData", replacedWhereClause)
    SQLOnJavaCollections.apply(query, objList).asInstanceOf[java.util.ArrayList[EventData]]
  }
}
