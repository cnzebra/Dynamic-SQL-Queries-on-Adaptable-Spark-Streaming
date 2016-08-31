package com.xpandit.utils

import com.xpandit.data.EventData
import org.josql.Query

/**
  * Applies SQL queries on Java collections using JoSQL library
  *
  **/

object SQLOnJavaCollections{

  /**
    * Applies SQL query on given List of objects
    *
    * @param strQuery - query to apply ex. "SELECT * FROM com.package.Item  where price > 10.0"
    * @param list - list of objects to filter
    * @return - list of filtered objects
    */
  def apply(strQuery: String, list: java.util.List[_]) : java.util.List[_] = {
    val query = new Query()
    query.addFunctionHandler(new FunctionHandlers())
    query.parse(strQuery)
    query.execute(list).getResults
  }


  /**
    * Builds query that  will be applied
    *
    * @param objClassPath - classpath to the object being filtered
    * @param whereClause - query's where clause to apply
    * @return  - string final query
    */
  def buildQuery(objClassPath: String, whereClause: String) : String  = "SELECT * FROM " + objClassPath + " WHERE " + whereClause


  /**
    * Workaround to make possible JoSQL library work.
    * Problem: query will be applied using class' fields as table columns, but we don't previously know what columns will our data have
    * Solution: convert each field access to method invokation on query (which is also supported by the library) that will return corresponding value
     *
    * @param userTypedWhere - user typed sql from configuration
    * @param eventData - current object being analyzed
    */
  def buildWhereClause(userTypedWhere: String, eventData: EventData): String = {
    val splitWhere = userTypedWhere.split(" ")
    val sqlReservedKeywords = Set("ABSOLUTE", "ACTION", "ADD", "ALL", "AND", "ANY") //TODO complete sql reserved words list

    val replacedWhere = splitWhere.map { (word) => {

      if (!sqlReservedKeywords.contains(word.toUpperCase) && eventData.columnIndex.contains(word.toUpperCase())) {
        //is column name
        "getAs" + eventData.columnType.get(word.toUpperCase).get + "('" + word.toUpperCase + "', :_currobj)" //Ex: converting age to getAsInt('age', :_currobj)
      }
      else {
        //is sql reserfved word or '<', '>', '=' etc.
        word.toUpperCase
      }
    }}

    replacedWhere.mkString(" ")
  }
}

/**
  * User defined functions that can be used in sql query to apply
  * Requirement: query.addFunctionHandler(new FunctionHandlers())
  */
class FunctionHandlers{

  def getAsSTRING(column: String, eventData: EventData): String = eventData.getAsString(column)
  def getAsBOOLEAN(column: String, eventData: EventData): Boolean = eventData.getAsBoolean(column)
  def getAsBYTE(column: String, eventData: EventData): Byte = eventData.getAsByte(column)
  def getAsSHORT(column: String, eventData: EventData): Short = eventData.getAsShort(column)
  def getAsINT(column: String, eventData: EventData): Int = eventData.getAsInt(column)
  def getAsLONG(column: String, eventData: EventData): Long = eventData.getAsLong(column)
  def getAsFLOAT(column: String, eventData: EventData): Float = eventData.getAsFloat(column)
  def getAsDOUBLE(column: String, eventData: EventData): Double = eventData.getAsDouble(column)
  def getAsBIGDECIMAL(column: String, eventData: EventData): java.math.BigDecimal = eventData.getAsBigDecimal(column)

  //TODO complete with other available types
}
