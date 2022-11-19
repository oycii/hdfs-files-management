package com.github.oycii

import com.github.oycii.dict.LayerConst._
import com.github.oycii.service.LayerService
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileSystem
import org.apache.log4j.Level
import org.apache.log4j.Logger.{getLogger, getRootLogger}

import java.net.URI

object Main extends LazyLogging {

  def main(args: Array[String]): Unit = {
    getRootLogger().setLevel(Level.OFF)

    logger.info("Start...")
    val conf = new Configuration()
    implicit val fileSystem: FileSystem = FileSystem.get(new URI(ROOT_FOLDER_URI), conf)
    val layerService = LayerService.stageToOds(NAME_STAGE_FOLDER, NAME_ODS_FOLDER, EXTANTION)

    logger.info("Application finished")
  }
}
