package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */



class RowDataPartitionUpdate(data: Array[String] = null, dataPKs: Array[String] = null, val previousPartitionData: Array[String] = null, pos: Long) extends RowDataUpdate(data, dataPKs, pos) {

  if (previousPartitionData == null)
    throw new IllegalArgumentException("No previousPartitionData provided in the constructor!")

  override def getOperationType: String = OperationType.PARTITIONUPDATE.toString

}
