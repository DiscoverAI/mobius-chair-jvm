package com.github.meandor

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

object MobiusChair {

  val configs: Config = ConfigFactory.load("mobius.conf")
  val fileSystemBasePath: String = configs.getString("hdfs.basePath")

  def defaultHDFS(): FileSystem = {
    val hdfsConf = new Configuration()
    hdfsConf.set("fs.defaultFS", fileSystemBasePath)
    FileSystem.get(hdfsConf)
  }

  def latestGeneration(fileSystem: FileSystem, path: String): String = {
    val fullPath = new Path(fileSystemBasePath + path)
    val generations = fileSystem.listStatus(fullPath)
      .filter(status => status.isDirectory)
      .map(_.getPath.toString.split("/").last)
      .filter(s => s.matches("^\\d+$"))

    if (generations.isEmpty) {
      return "0000"
    }

    generations.max
  }
}
