package com.github.oycii.dao

import java.io.{InputStream}

case class FileDto(path: String, isFolder: Boolean)

trait IFileSystem {
  def saveFile(filepath: String): Unit

  def removeFile(filename: String): Boolean

  def getFile(filename: String): InputStream

  def createFolder(folderPath: String): Unit

  def getFolders(folderPath: String): List[FileDto]

  def getFolderFiles(folderPath: String): List[FileDto]
}
