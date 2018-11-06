package com.github.meandor

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}

object MobiusChair extends LazyLogging {
  def outputPath(fileSystem: FileSystem, basePath: String, name: String, version: String): String = {
    val jobOutputPath = s"$basePath/$name/$version"
    if (createIfNotAvailable(fileSystem, jobOutputPath)) {
      logger.info("Output folder created")
    }
    val generation = nextGeneration(fileSystem, jobOutputPath)
    logger.info(s"Next generation: $generation")
    s"$jobOutputPath/$generation"
  }

  def nextGeneration(fileSystem: FileSystem, path: String): String = {
    val currentGeneration = latestGeneration(fileSystem, path)
    "%04d".format(currentGeneration.getOrElse("0000").toInt + 1)
  }

  def latestGeneration(fileSystem: FileSystem, path: String): Option[String] = {
    val fullPath = new Path(path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString.split("/").last)
      .filter(s => s.matches("^\\d+$"))

    if (generations.isEmpty) {
      logger.info("Did not find current generation")
      return None
    }

    val latest = generations.max
    logger.info(s"Latest generation: $latest")
    Some(latest)
  }

  def cleanUpGenerations(fileSystem: FileSystem, path: String, noToKeep: Int): Seq[String] = {
    val fullPath = new Path(path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString)
      .filter(s => s.matches("^.*/\\d{4}$"))
      .sorted

    val toBeDeleted = generations.take(generations.length - noToKeep)
    toBeDeleted.foreach(pathString => {
      val path = new Path(pathString)
      fileSystem.delete(path, true)
    })
    toBeDeleted
  }

  def createIfNotAvailable(fileSystem: FileSystem, pathName: String): Boolean = {
    val path = new Path(pathName)
    if (!fileSystem.exists(path)) {
      fileSystem.mkdirs(path)
    } else {
      false
    }
  }
}
