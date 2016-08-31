package com.xpandit.config

import java.io.{FileInputStream, PrintWriter}
import java.util.Properties

import scala.collection.JavaConversions._

/**
  * Created by xpandit on 8/26/16.
  */
class QueryConfigs extends Serializable {

  val configPath = "src/main/resources/streaming-query.conf"
  val configOneTimeQueryPath = "src/main/resources/streaming-one-time-query.conf"
  var lastSuccessfulConfig: Properties = null
  var config : Properties = null

  def hasSuccessfulConfig = lastSuccessfulConfig != null

  /**
    * Get filtering where clause to apply, which decides what events should be passed as state to the next batch
    *
    * @param lastSuccConfig -  if true then return where clause from last successful saved configuration (in case current one is malformed)
    * @return - String where clause
    */
  def getFilteringWhereClause(lastSuccConfig: Boolean): String = (if (lastSuccConfig) lastSuccessfulConfig else config).getProperty("filtering_where_clause")

  /**
    * Gets a set of sql queries to execute
    *
    * @param lastSuccConfig - if true then return queries from last successful saved configuration (in case current one is malformed)
    * @return - (index, query)
    */
  def getQueries(lastSuccConfig: Boolean): scala.collection.mutable.Set[(String, String)] = {
    val properties = if (lastSuccConfig) lastSuccessfulConfig else config
    val querySet = properties.stringPropertyNames().filter( _.toLowerCase.startsWith("query"))

    if(!lastSuccConfig)
      properties.stringPropertyNames().filter( _.toLowerCase.startsWith("one.time.query")).foreach(querySet.add(_))

    querySet.map((query) => (query.split('_')(1), properties.getProperty(query)))
  }


  /**
    * Reads configurations from source
    */
  def updateConfigs() = {

    //TODO read configurations from source

    val props = new Properties()
    props.load(new FileInputStream(configPath))
    props.load(new FileInputStream(configOneTimeQueryPath))

    //deleting one time queries so that they are not executed in the next batch
    new PrintWriter(configOneTimeQueryPath).close()

    config = props
  }

  def saveConfigAsLastSuccessful() = { lastSuccessfulConfig = config }

}
