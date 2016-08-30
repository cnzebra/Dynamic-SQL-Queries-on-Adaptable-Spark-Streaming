package com.xpandit.parsers

/*
 * @author "Vasco Lopes" <vasco.lopes@xpand-it.com>
 */


trait GoldenGateParser {

  /*
   * It must return the data array that the RowData contains, with the attributes in the correct position (sorted by the schema) and in the last position the operation type
   */
  def parse(row: String, schema: Array[String], PKsList: Array[String], partitionKeys: Seq[String], toLowerCase: Boolean): (Array[String], Array[String], Array[String], Array[String], Long)

}
