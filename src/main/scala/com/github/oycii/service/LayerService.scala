package com.github.oycii.service

import com.github.oycii.dao.{FileDto, HdfsDao}
import com.github.oycii.dict.LayerConst._
import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.FileSystem

object LayerService extends LazyLogging {

  private def concatFolderFiles(concatTmpFilePath: String, folderPath: String, pathConactFile:String)(implicit hdfsDao: HdfsDao): Unit = {
    val folderFiles = hdfsDao.getFolderFiles(folderPath)
    hdfsDao.concatFiles(concatTmpFilePath, folderFiles, pathConactFile)
  }

  private def filterFilesByExt(files: List[FileDto], ext: String): List[FileDto] = {
    files.filter(fileDao => fileDao.path.takeRight(ext.length) == ext)
  }

  private def copyFiles(fromDirPath: String, toDirPath: String, ext: String)(implicit hdfsDao: HdfsDao): Int = {
    val folderFiles = filterFilesByExt(hdfsDao.getFolderFiles(fromDirPath), ext)
    hdfsDao.copyFiles(fromDirPath, toDirPath, folderFiles)
  }

  private def dropFiles(fromDirPath: String, ext: String)(implicit hdfsDao: HdfsDao):Unit = {
    val folderFiles = filterFilesByExt(hdfsDao.getFolderFiles(fromDirPath), ext)
    folderFiles.foreach(f =>  hdfsDao.dropFile(f.path))
  }

  def stageToOds(stageDirPath: String, odsDirPath: String, ext: String)(implicit fileSystem: FileSystem): Unit = {
    logger.info(s"Start move data from: ${stageDirPath} to: ${odsDirPath} with extension: ${ext}")
    implicit val hdfsDao = new HdfsDao(fileSystem)
    val listFolders = hdfsDao.getFolders(PATH_SEPARATOR + stageDirPath)
    listFolders.foreach(fileDto => {
      val fromDir = fileDto.path
      val toDir = fromDir.replaceFirst(stageDirPath, odsDirPath)
      val countCopyFiles = copyFiles(fromDir, toDir, ext)
      if (countCopyFiles > 0) {
        val filePathConcatOfFolder = fileDto.path.replaceFirst(NAME_STAGE_FOLDER, NAME_ODS_FOLDER)
        val pathTmpConcatFile = filePathConcatOfFolder + PATH_SEPARATOR + TMP_FILE_CONCAT
        val pathConcatFile = filePathConcatOfFolder + PATH_SEPARATOR + CONCAT_FILE_NAME
        concatFolderFiles(pathTmpConcatFile, toDir, pathConcatFile)
      }
      dropFiles(fromDir, ext)
      logger.info(s"moved data from: ${fromDir} to ${toDir}, count files: ${countCopyFiles}")
    })
  }
}
