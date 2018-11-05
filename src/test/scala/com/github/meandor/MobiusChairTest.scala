package com.github.meandor

import java.io.File
import java.nio.file.{Files, Paths}

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

  feature("should get next generation in path") {
    scenario("one generation already existing") {
      MobiusChair.nextGeneration(localHDFS, s"$basePath/inFoo") shouldBe "0002"
    }

    scenario("nothing existing yet") {
      MobiusChair.nextGeneration(localHDFS, s"$basePath/inFoo/0001") shouldBe "0001"
    }
  }

  feature("should clean up old generations") {
    scenario("keep only 3 generations and one generation existing") {
      MobiusChair.cleanUpGenerations(localHDFS, s"$basePath/inFoo", 3) shouldBe Seq()
    }

    scenario("keep only 3 generations and n < 3 generation existing") {
      MobiusChair.cleanUpGenerations(localHDFS, s"$basePath/inFooBar/0002", 3) shouldBe Seq()
    }

    scenario("keep only 2 generations and 3 generation existing") {
      try {
        Files.createDirectories(Paths.get(s"$basePath/infoofoo/0006"))
        Files.createFile(Paths.get(s"$basePath/0006/_SUCCESS"))
      } catch {
        case _: Exception =>
      }

      val deletedFolders = MobiusChair.cleanUpGenerations(localHDFS, s"$basePath/infoofoo", 2)
      deletedFolders.length shouldBe 1
      deletedFolders.head should endWith(s"$basePath/infoofoo/0006")

      val tmpBasePath = new File(s"$basePath/infoofoo")
      tmpBasePath.list().toSeq should contain theSameElementsAs Seq("0007", "0008")
    }
  }
}