package com.github.oycii.dict

import java.io.File

object LayerConst {
  val ROOT_FOLDER_URI = "hdfs://localhost:9000"
  val CONCAT_FILE_NAME = "part-0000.csv"
  val TMP_FILE_CONCAT = "concat.csv"
  val NAME_STAGE_FOLDER = "stage"
  val NAME_ODS_FOLDER = "ods"
  val EXTANTION = "csv"
  val PATH_SEPARATOR =  File.separator
}
