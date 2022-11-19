package com.github.oycii.dao

import com.typesafe.scalalogging.Logger

import java.io.{BufferedInputStream, File, FileInputStream, InputStream}
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._

import scala.collection.mutable

class HdfsDao(fileSystem: FileSystem) extends IFileSystem {
  val logger = Logger(getClass.getName)

  def createEmptyFile(filepath: String): Unit = {
    val out = fileSystem.create(new Path(filepath))
    out.close()
  }

  override def saveFile(filepath: String): Unit = {
    val file = new File(filepath)
    val out = fileSystem.create(new Path(file.getName))
    val in = new BufferedInputStream(new FileInputStream(file))
    try {
      var b = new Array[Byte](1024)
      var numBytes = in.read(b)
      while (numBytes > 0) {
        out.write(b, 0, numBytes)
        numBytes = in.read(b)
      }
    } finally {
      if (in != null)
        in.close()
      if (out != null)
        out.close()
    }
  }

  override  def removeFile(filename: String): Boolean = {
    val path = new Path(filename)
    fileSystem.delete(path, true)
  }

  override def getFile(filename: String): InputStream = {
    val path = new Path(filename)
    fileSystem.open(path)
  }

  override def createFolder(folderPath: String): Unit = {
    val path = new Path(folderPath)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    }
  }

  private def getFiles(folderPathHdfs: Path) = {
    val files = mutable.Set[FileDto]()
    val fileStatusListIterator = fileSystem.listFiles(folderPathHdfs, true)
    while (fileStatusListIterator.hasNext) {
      val fileStatus: LocatedFileStatus = fileStatusListIterator.next
      if (fileStatus.isFile)
        files.addOne(FileDto(fileStatus.getPath.toString, true))
    }

    files.toList

  }

  private def createFileDtoOfFolder(fileStatus: LocatedFileStatus): FileDto = {
    FileDto(fileStatus.getPath.getParent.toString, true)
  }

  override def getFolders(folderPath: String): List[FileDto] = {
    val files = mutable.Set[FileDto]()
    val folderPathHdfs: Path = new Path(folderPath)
    val needPathDepth = folderPathHdfs.depth() + 2

    val fileStatusListIterator = fileSystem.listFiles(folderPathHdfs, true)
    while (fileStatusListIterator.hasNext) {
      val fileStatus = fileStatusListIterator.next
      if (fileStatus.getPath.depth() == needPathDepth)
       files.addOne(FileDto(fileStatus.getPath.getParent.toString, true))
    }

    files.toList
  }

  override def getFolderFiles(folderPath: String): List[FileDto] = {
    val folderPathHdfs: Path = new Path(folderPath)
    getFiles(folderPathHdfs)
  }

  def concatFiles(targetFilePath: String, files: List[FileDto], pathConcatFile: String): Unit = {
    val psrcs: Array[Path] = files.map(fileDto => new Path(fileDto.path)).toArray
    logger.info("concatFiles to: " + targetFilePath + " from: " + psrcs.mkString(","))

    createEmptyFile(targetFilePath)
    fileSystem.concat(new Path(targetFilePath), psrcs)
    renameFile(targetFilePath, pathConcatFile)
  }

  def renameFile(fromPathFile: String, toPathFile: String): Boolean = {
    fileSystem.rename(new Path(fromPathFile), new Path(toPathFile))
  }

  def copyFile(fromPath: Path, toPath: Path): Unit = {
    FileUtil.copy(fileSystem, fromPath, fileSystem, toPath, false, fileSystem.getConf)
  }

  private def getFileSizeByPath(pathFile: String): Long = {
    val path = new Path(pathFile)
    val hdfs = path.getFileSystem(new Configuration())
    val cSummary: ContentSummary = hdfs.getContentSummary(path)
    val length = cSummary.getLength
    length
  }

  def copyFiles(fromDirPath: String, toFolderPath: String, files: List[FileDto]): Int = {
    val copyFiles = files.filter(f => {
      if (getFileSizeByPath(f.path) > 0)
        true
      else {
        logger.warn(s"File size of ${f.path} equal 0")
        false
      }

    })

    copyFiles.foreach(f => {
      val sizeFile =  getFileSizeByPath(f.path)
      if (sizeFile > 0) {
        val pathFileFrom = new Path(f.path)
        val pathFileTo = new Path(f.path.replaceFirst(fromDirPath, toFolderPath))
        copyFile(pathFileFrom, pathFileTo)
      }
    })

    copyFiles.length
  }

  def dropFile(filePath: String): Unit = {
    fileSystem.delete(new Path(filePath), false)
  }
}
