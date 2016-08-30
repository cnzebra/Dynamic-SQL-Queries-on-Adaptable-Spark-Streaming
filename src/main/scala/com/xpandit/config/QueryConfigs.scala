package com.xpandit.config

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by xpandit on 8/26/16.
  */
class QueryConfigs extends Serializable {

  val configPath = "src/main/resources/streaming-query.conf"
  var lastSuccessfulConfig: Properties = null
  var config : Properties = null

  def hasSuccessfulConfig = lastSuccessfulConfig != null

  def getFilteringWhereClause(lastSuccConfig: Boolean): String = (if (lastSuccConfig) lastSuccessfulConfig else config).getProperty("filtering_where_clause")

  def getQuery(lastSuccConfig: Boolean): String = (if (lastSuccConfig) lastSuccessfulConfig else config).getProperty("query")

  def updateConfigs() = {

    val props = new Properties()
    props.load(new FileInputStream(configPath))

    config = props
  }

  def saveConfigAsLastSuccessful() = { lastSuccessfulConfig = config }

}
