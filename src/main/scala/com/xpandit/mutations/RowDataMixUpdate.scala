package com.xpandit.mutations

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */



class RowDataMixUpdate(data: Array[String] = null, dataPKs: Array[String] = null, val previousPKsData: Array[String] = null, val previousPartitionData: Array[String] = null, pos: Long) extends RowDataUpdate(data, dataPKs, pos) {

  if (previousPKsData == null || previousPartitionData == null)
    throw new IllegalArgumentException("No previousPKsData or previousPartitionData provided in the constructor!")

  override def getOperationType: String = OperationType.PARTITIONUPDATE.toString

}