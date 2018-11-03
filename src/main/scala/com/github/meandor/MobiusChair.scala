package com.github.meandor

import org.apache.hadoop.fs.{FileSystem, Path}

object MobiusChair {

  def latestGeneration(fileSystem: FileSystem, path: String): String = {
    val fullPath = new Path(path)
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
