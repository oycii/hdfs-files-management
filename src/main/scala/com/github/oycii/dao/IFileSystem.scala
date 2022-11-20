package com.github.oycii.dao

import java.io.{InputStream}

case class FileDto(path: String, isFolder: Boolean)

trait IFileSystem {

  def getFile(filename: String): InputStream
  def createFolder(folderPath: String): Unit
  def getFolders(folderPath: String): List[FileDto]
  def getFolderFiles(folderPath: String): List[FileDto]
  def copyFiles(fromDirPath: String, toFolderPath: String, files: List[FileDto]): List[String]
  def concatFiles(targetFilePath: String, files: List[FileDto], pathConcatFile: String): Unit
  def dropFile(filePath: String): Unit
  def renameFile(fromPathFile: String, toPathFile: String): Boolean
}
