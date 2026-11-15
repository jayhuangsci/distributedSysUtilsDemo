package io.github.jayhuangsci.ratings.tools

import io.github.jayhuangsci.ratings.core.Csv
import io.github.jayhuangsci.ratings.hadoop.HadoopAnalytics
import io.github.jayhuangsci.ratings.spark.SparkAnalytics
import org.apache.hadoop.conf.Configuration
import org.apache.spark.{SparkConf, SparkContext}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths, StandardOpenOption}
import java.time.Instant

/** Reproducible local experiment; timing includes execution, validation, and publishing. */
object Demo {
  def main(args: Array[String]): Unit = {
    if (args.length > 4 || args.contains("--help")) {
      println("demo [input=sample_input/smalldata.txt] [new-output-root=demo-output/run-TIMESTAMP] [partitions=2] [repetitions=1]")
      return
    }
    val input = if (args.nonEmpty) args(0) else "sample_input/smalldata.txt"
    val output = Paths.get(if (args.length > 1) args(1) else "demo-output/run-" + Instant.now().toEpochMilli)
    val partitions = if (args.length > 2) args(2).toInt else 2
    val repetitions = if (args.length > 3) args(3).toInt else 1
    require(partitions > 0 && repetitions > 0, "Positive partition and repetition counts are required")
    Files.createDirectories(output.toAbsolutePath.getParent)
    Files.createDirectory(output) // Existing runs are never overwritten.
    val hadoop = new Configuration()
    hadoop.set("mapreduce.framework.name", "local")
    hadoop.set("fs.defaultFS", "file:///")
    hadoop.setInt("mapreduce.task.io.sort.mb", 16)
    val spark = new SparkContext(new SparkConf().setAppName("Ratings Lab local experiment")
      .setMaster("local[2]").set("spark.ui.enabled", "false")
      .set("spark.driver.host", "127.0.0.1").set("spark.driver.bindAddress", "127.0.0.1"))
    spark.setLogLevel("WARN")
    val report = Files.newBufferedWriter(output.resolve("report.csv"), StandardCharsets.UTF_8, StandardOpenOption.CREATE_NEW)
    try {
      report.write("repetition,task,partitions,hadoop_ms,spark_ms,result_records,status\n")
      for (repetition <- 1 to repetitions;
           task <- Seq("top-raters", "total", "user-counts", "agreement", "movie-stats", "recommend", "word-count")) {
        val hadoopOutput = output.resolve(s"$repetition-hadoop-$task")
        val sparkOutput = output.resolve(s"$repetition-spark-$task")
        def options(destination: Path): Array[String] = Array("--task", task, "--input", input,
          "--output", destination.toString, "--partitions", partitions.toString)
        val start = System.nanoTime()
        HadoopAnalytics.run(hadoop, options(hadoopOutput))
        val hadoopMs = (System.nanoTime() - start) / 1000000
        val scalaStart = System.nanoTime()
        SparkAnalytics.run(spark, options(sparkOutput))
        val sparkMs = (System.nanoTime() - scalaStart) / 1000000
        CompareOutputs.verify(hadoopOutput, sparkOutput)
        report.write(Csv.row(Int.box(repetition), task, Int.box(partitions), Long.box(hadoopMs), Long.box(sparkMs),
          Int.box(CompareOutputs.read(hadoopOutput).size()), "MATCH") + "\n")
        report.flush()
      }
    } finally {
      report.close()
      spark.stop()
    }
    println("Verified results and measured local timings: " + output.toAbsolutePath.resolve("report.csv"))
  }
}
