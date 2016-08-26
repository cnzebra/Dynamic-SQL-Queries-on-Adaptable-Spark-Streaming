package com.xpandit.config

import java.io.FileInputStream
import java.util.Properties

/**
  * Created by xpandit on 8/26/16.
  */
class QueryConfigs extends Serializable {

  val configPath = "src/main/resources/streaming-query.conf"
  var previousConfig: Properties = null
  var config : Properties = null



  def getFilteringWhereClause(prevConf: Boolean): String = (if (prevConf) previousConfig else config).getProperty("filtering_where_clause")

  def getQuery(prevConf: Boolean): String = (if (prevConf) previousConfig else config).getProperty("query")

  def updateConfigs() = {

    val props = new Properties()
    props.load(new FileInputStream(configPath))

    config = props
  }

  def setPreviousConfig() = { previousConfig = config }

}
