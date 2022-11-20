package com.github.oycii.dao

import com.typesafe.scalalogging.Logger

import java.io.{BufferedInputStream, File, FileInputStream, IOException, InputStream, OutputStream}
import org.apache.hadoop.conf._
import org.apache.hadoop.fs._
import org.apache.hadoop.io.IOUtils

import scala.collection.{immutable, mutable}

class HdfsDao(fileSystem: FileSystem) extends IFileSystem {
  val logger = Logger(getClass.getName)

  def createEmptyFile(filepath: String): Unit = {
    val out = fileSystem.create(new Path(filepath))
    out.close()
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

  override def concatFiles(targetFilePath: String, files: List[FileDto], pathConcatFile: String): Unit = {
    val psrcs: Array[Path] = files.map(fileDto => new Path(fileDto.path)).toArray
    logger.info("concatFiles to: " + targetFilePath + " from: " + psrcs.mkString(","))

    createEmptyFile(targetFilePath)
    fileSystem.concat(new Path(targetFilePath), psrcs)
    renameFile(targetFilePath, pathConcatFile)
  }

  override def renameFile(fromPathFile: String, toPathFile: String): Boolean = {
    fileSystem.rename(new Path(fromPathFile), new Path(toPathFile))
  }

  private def copyFile(fromPath: Path, toPath: Path): Unit = {
    FileUtil.copy(fileSystem, fromPath, fileSystem, toPath, false, fileSystem.getConf)
  }

  private def copyFile(fromPath: Path, toPath: Path, withAppendText: String): Unit = {
    var in: FSDataInputStream = null
    var out: FSDataOutputStream = null
    try {
      in =  fileSystem.open(fromPath)
      out = fileSystem.create(toPath, false)
      IOUtils.copyBytes(in, out, fileSystem.getConf, false)
      out.write(withAppendText.getBytes)
    } catch {
      case e: IOException =>
        IOUtils.closeStream(out)
        IOUtils.closeStream(in)
        throw e
    } finally {
      if (in != null) IOUtils.closeStream(in)
      if (out != null ) IOUtils.closeStream(out)
    }
  }

  private def getFileSizeByPath(pathFile: String): Long = {
    val path = new Path(pathFile)
    val hdfs = path.getFileSystem(new Configuration())
    val cSummary: ContentSummary = hdfs.getContentSummary(path)
    val length = cSummary.getLength
    length
  }

  override def copyFiles(fromDirPath: String, toFolderPath: String, files: List[FileDto]): List[String] = {
    val copyFiles = files.filter(f => {
      if (getFileSizeByPath(f.path) > 0)
        true
      else {
        logger.warn(s"File size of ${f.path} equal 0")
        false
      }
    })

    val listCopyFiles = mutable.ListBuffer[String]()

    copyFiles.foreach(f => {
      val sizeFile =  getFileSizeByPath(f.path)
      if (sizeFile > 0) {
        val pathFileFrom = new Path(f.path)
        val pathFileTo = new Path(f.path.replaceFirst(fromDirPath, toFolderPath))
        copyFile(pathFileFrom, pathFileTo, "\n")
        listCopyFiles.addOne(pathFileTo.toString)
      }
    })

    listCopyFiles.toList
  }

  override def dropFile(filePath: String): Unit = {
    fileSystem.delete(new Path(filePath), false)
  }

}
