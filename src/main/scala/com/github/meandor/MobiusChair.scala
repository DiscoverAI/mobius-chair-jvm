package com.github.meandor

import java.io.File

import com.typesafe.scalalogging.LazyLogging
import org.apache.hadoop.fs.{FileSystem, Path}

object MobiusChair extends LazyLogging {

  val versionizedPathFormat = "^.*/\\d{4}$"

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
    val generationsFolders = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
    val generations = generationsFolders
      .filter(s => s.getPath.toString.matches(versionizedPathFormat))

    if (generations.isEmpty) {
      logger.info("Did not find current generation")
      return None
    }

    val generationsNames = generations.map(_.getPath.getName)

    val latest = generationsNames.max
    logger.info(s"Latest generation: $latest")
    Some(latest)
  }

  def versionFromPath(path: String): Int = {
    Integer.parseInt(new File(path).getName)
  }

  def cleanUpGenerations(fileSystem: FileSystem, path: String, noToKeep: Int): Seq[String] = {
    val fullPath = new Path(path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString)
      .filter(s => s.matches(versionizedPathFormat))
      .sortWith(versionFromPath(_) < versionFromPath(_))

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
