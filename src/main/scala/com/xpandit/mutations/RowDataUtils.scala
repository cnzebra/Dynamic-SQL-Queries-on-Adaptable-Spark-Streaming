package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */

import com.xpandit.parsers.GoldenGateParserJSON
import com.xpandit.utils.Constants

object RowDataUtils {

  def processRowData(row: String, schema: Array[String], PKsList: Array[String], partitionKeysSeq: Seq[String], parser: String, toLowerCase: Boolean) = {
    var partitionKeys = Seq[String]()
    if (partitionKeysSeq != null)
      partitionKeys = partitionKeysSeq

    if (parser.equalsIgnoreCase(Constants.parserJSON)) {
      (makeRowData _).tupled(GoldenGateParserJSON.parse(row, schema, PKsList, partitionKeys, toLowerCase))
    } else
      throw new IllegalArgumentException("Unsupported parser")
  }

  def makeRowData(data: Array[String], dataPKs: Array[String], previousPKsData: Array[String], previousPartitionData: Array[String], pos: Long) = {
    val operationType = data(data.length - 1)

    if (OperationType.INSERT.toString.equalsIgnoreCase(operationType))
      new RowDataInsert(data.slice(0, data.length - 1), dataPKs, pos)
    else if (OperationType.DELETE.toString.equalsIgnoreCase(operationType))
      new RowDataDelete(data.slice(0, data.length - 1), dataPKs, pos)
    else if (OperationType.UPDATE.toString.equalsIgnoreCase(operationType))
      new RowDataUpdate(data.slice(0, data.length - 1), dataPKs, pos)
    else if (OperationType.PKUPDATE.toString.equalsIgnoreCase(operationType))
      new RowDataPkUpdate(data.slice(0, data.length - 1), dataPKs, previousPKsData, pos)
    else if (OperationType.PARTITIONUPDATE.toString.equalsIgnoreCase(operationType))
      new RowDataPartitionUpdate(data.slice(0, data.length - 1), dataPKs, previousPartitionData, pos)
    else if (OperationType.MIXUPDATE.toString.equalsIgnoreCase(operationType))
      new RowDataMixUpdate(data.slice(0, data.length - 1), dataPKs, previousPKsData, previousPartitionData, pos)
    else
      throw new IllegalArgumentException("Unknown operation type")
  }

  def getRowDataKeys(data: Array[String], schema: Array[String], PKsList: Array[String]): String = {
    var key = ""

    for (i <- PKsList.indices) {
      for (j <- schema.indices) {
        if (PKsList(i).equalsIgnoreCase(schema(j)))
          key += data(j)
      }
    }

    key
  }

}
