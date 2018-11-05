package com.github.meandor

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocalFileSystem}
import org.scalatest.{FeatureSpec, Matchers}

class MobiusChairTest extends FeatureSpec with Matchers {
  val basePath = "src/test/resources/filesystem"
  val localHDFS: LocalFileSystem = FileSystem.getLocal(new Configuration())

  feature("should get latest generation in path") {
    scenario("should return 0000 when nothing existing yet") {
      MobiusChair.latestGeneration(localHDFS, s"$basePath/inFoo/0001") shouldBe "0000"
    }

    scenario("should return latest when n generations existing") {
      MobiusChair.latestGeneration(localHDFS, s"$basePath/inFooBar/0002") shouldBe "0009"
    }
  }

  feature("get next generation in path") {
    scenario("one generation already existing") {
      MobiusChair.nextGeneration(localHDFS, s"$basePath/inFoo") shouldBe "0002"
    }

    scenario("nothing existing yet") {
      MobiusChair.nextGeneration(localHDFS, s"$basePath/inFoo/0001") shouldBe "0001"
    }
  }
}