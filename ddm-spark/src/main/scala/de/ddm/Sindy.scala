package de.ddm

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import scala.collection.mutable

object Sindy {

  // reads CSV data
  private def readData(input: String, spark: SparkSession): Dataset[Row] = {
    spark.read
      .option("inferSchema", "false")
      .option("header", "true")
      .option("quote", "\"")
      .option("delimiter", ";")
      .option("mode", "DROPMALFORMED")
      .csv(input)
  }

  // makes strings from list
  private def stringProcessorTask(list: List[String]): String = {
    val strBuild = new StringBuilder()
    list.foreach { s => strBuild.append(s).append(", ") }
    strBuild.toString
  }

  // task function for cell creation
  private def cellGenerationTask(row: Row): Array[(String, String)] = {
    val fields = row.schema.fieldNames
    val arrBuffer = mutable.ArrayBuffer[(String, String)]()
    var iterator = 0
    while (iterator < row.length) {
      val value = Option(row.get(iterator))
        .map(_.toString)
        .getOrElse("")
      if (value.nonEmpty) {
        arrBuffer += ((value, fields(iterator)))
      }
      iterator += 1
    }
    arrBuffer.toArray
  }

  def discoverINDs(inputs: List[String], spark: SparkSession): Unit = {
    import spark.implicits._
    println("Discovering INDs...")
    val flatData = inputs.par.map { input => readData(input, spark).flatMap(cellGenerationTask) }.reduce(_ union _)
    // processing pipeline
    val outcome = flatData
      .groupByKey(_._1)
      .mapGroups { (_, iter) => iter.map(_._2).toSet.toList }
      .flatMap(inclusionGenerationTask)
      .groupByKey(_._1)
      .reduceGroups { (u, v) => (u._1, u._2.intersect(v._2)) }
      .filter(_._2._2.nonEmpty)
      .map { case (_, (col, includedIn)) => s"$col < ${stringProcessorTask(includedIn)}" }
      .sort("value")
      .collect()

    println("Discovered INDs:")
    outcome.foreach(println)
    println(s"Total number of INDs: " + outcome.map(_.count(_ == ',')).sum)
  }

  // creates inclusion list
  private def inclusionGenerationTask(cols: List[String]): Array[(String, List[String])] = {
    cols.map { col => (col, cols.filter(_ != col)) }.toArray
  }
}