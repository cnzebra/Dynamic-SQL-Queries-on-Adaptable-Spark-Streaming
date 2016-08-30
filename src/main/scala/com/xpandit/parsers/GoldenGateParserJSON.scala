package com.xpandit.parsers

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */

import com.xpandit.mutations.OperationType
import org.json.{JSONObject, JSONTokener}

import scala.collection.mutable

object GoldenGateParserJSON extends GoldenGateParser {

  override def parse(row: String, schema: Array[String], PKsList: Array[String], partitionKeys: Seq[String], toLowerCase: Boolean): (Array[String], Array[String], Array[String], Array[String], Long) = {
    var dataMap = mutable.Map[String, String]()
    val data = new Array[String](schema.length + 1)
    for (i <- data.indices)
      data(i) = ""
    var operationType = ""

    var afterPKsData = new Array[String](PKsList.length)
    var previousPKsData = new Array[String](PKsList.length)
    var previousPartitionData = new Array[String](partitionKeys.length)

    val tokener = new JSONTokener(row)
    val root = new JSONObject(tokener)

    val pos = root.getString("op_pos").toLong

    if (root.getString("op_type").equalsIgnoreCase(OperationType.INSERT.toString)) {

      populateJSONObjectToMap(dataMap, "after", root, toLowerCase)
      operationType = OperationType.INSERT.toString

    } else if (root.getString("op_type").equalsIgnoreCase(OperationType.DELETE.toString)) {

      populateJSONObjectToMap(dataMap, "before", root, toLowerCase)
      operationType = OperationType.DELETE.toString

    } else if (root.getString("op_type").equalsIgnoreCase(OperationType.UPDATE.toString)) {

      val dataBeforeMap = mutable.Map[String, String]()
      val dataAfterMap = mutable.Map[String, String]()

      populateJSONObjectToMap(dataBeforeMap, "before", root, toLowerCase)
      populateJSONObjectToMap(dataAfterMap, "after", root, toLowerCase)

      var partitionChange = false
      partitionKeys.foreach(partitionKey => {
        if (!dataBeforeMap(partitionKey).equalsIgnoreCase(dataAfterMap(partitionKey)))
          partitionChange = true
      })
      var pkChange = false
      PKsList.foreach(PK => {
        if (!dataBeforeMap(PK).equalsIgnoreCase(dataAfterMap(PK)))
          pkChange = true
      })

      if (pkChange || partitionChange) {
        if (pkChange && partitionChange) {
          previousPKsData = PKsList.map(PK => dataBeforeMap(PK))
          previousPartitionData = partitionKeys.toArray.map(partitionKey => dataBeforeMap(partitionKey))

          operationType = OperationType.MIXUPDATE.toString
        } else if (pkChange) {
          previousPKsData = PKsList.map(PK => dataBeforeMap(PK))

          operationType = OperationType.PKUPDATE.toString
        } else if (partitionChange) {
          previousPartitionData = partitionKeys.toArray.map(partitionKey => dataBeforeMap(partitionKey))

          operationType = OperationType.PARTITIONUPDATE.toString
        } else {
          throw new IllegalArgumentException("Unknown update type")
        }
      } else {
        operationType = OperationType.UPDATE.toString
      }

      dataMap = dataAfterMap

    } else {
      throw new IllegalArgumentException("Unknown operation type")
    }

    dataMapToArray(schema, dataMap, data)
    data(data.length - 1) = operationType

    afterPKsData = PKsList.map(PK => dataMap(PK))

    (data, afterPKsData, previousPKsData, previousPartitionData, pos)
  }


  private def populateJSONObjectToMap(map: mutable.Map[String, String], key: String, root: JSONObject, toLowerCase: Boolean) = {
    val obj = root.getJSONObject(key)

    val objIterator = obj.keys()
    while (objIterator.hasNext) {
      val objElem = objIterator.next()
      val objKey = objElem.toString

      if (toLowerCase)
        map += objKey.toLowerCase -> obj.getString(objKey)
      else
        map += objKey -> obj.getString(objKey)
    }
  }

  private def dataMapToArray(schema: Array[String], dataMap: mutable.Map[String, String], data: Array[String]) = {
    for ((attr, i) <- schema.view.zipWithIndex) {
      if (dataMap.contains(attr))
        data(i) = dataMap(attr)
    }
  }

}
