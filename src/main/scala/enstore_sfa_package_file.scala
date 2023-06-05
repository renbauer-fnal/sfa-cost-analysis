/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package scala

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object EnstoreSFAPackageFile {

  val MAX_CRC = 4293984228.0
  val MAX_ID = 764942953.0 + 1.0

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Enstore SFA Analysis")
      .config("spark.speculation", "false")
      .config("spark.task.maxFailures", 12)
      .config("spark.task.cpus", 10)
      .config("spark.sql.broadcastTimeout", "36000")
      .getOrCreate()
    val my_args = Array(32, 10)
    if (args.length > 0) {
       my_args(0) = args(0).toInt
    }
    if (args.length > 1) {
       my_args(1) = args(1).toInt
    }
    runEnstoreSFAAnalysis(spark, my_args(0), my_args(1))

    spark.stop()
  }

  private def runEnstoreSFAAnalysis(spark: SparkSession, numPartitions: Int, stages: Int): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val partition = (0 to stages - 1).toList.par
    partition.foreach { x =>
      val stage_size = (MAX_ID / stages).ceil.toInt
      val id_lower = x * stage_size
      val id_upper = (x + 1) * stage_size
      // val query = s"(select * from file where id >= $id_lower and id < $id_upper and package_id is not NULL) as package_files"
      // val query = s"(select * from file where package_id is not NULL) as package_files"
      val jdbcDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
        .option("dbtable", "file")
        .option("user", "enstore")
        .option("fetchSize", "10000")
        .option("partitionColumn", "id")
        .option("numPartitions", numPartitions)
        .option("lowerBound", id_lower)
        .option("upperBound", id_upper)
        .load()  // Now a DataFrame instead of DataFrameReader
        .filter("package_id is not NULL")

      // jdbcDF.filter(jdbcDF("id") > id_lower)
      // jdbcDF.filter(jdbcDF("id") < id_upper)
      // jdbcDF.filter(jdbcDF("package_id").isNotNull)
      // jdbcDF.write.mode("append").saveAsTable(s"packagefile$x")
      jdbcDF.write
        .format("jdbc")
        .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
        .option("dbtable", s"packagefile_by_id_$x")
        .option("user", "enstore")
        .mode(SaveMode.Append)
        .save()
    }

    // val connectionProperties = new Properties()
    // connectionProperties.put("user", "enstore_reader")
    // val jdbcDF2 = spark.read
      // .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // Specifying the custom data types of the read schema
    // connectionProperties.put("customSchema", "id DECIMAL(38, 0), name STRING")
    // val jdbcDF3 = spark.read
      // .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Saving data to a JDBC source
    // jdbcDF.write
      // .format("jdbc")
      // .option("url", "jdbc:postgresql:dbserver")
      // .option("dbtable", "schema.tablename")
      // .option("user", "username")
      // .option("password", "password")
      // .save()

    // jdbcDF2.write
      // .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)

    // Specifying create table column data types on write
    // jdbcDF.write
      // .option("createTableColumnTypes", "name CHAR(64), comments VARCHAR(1024)")
      // .jdbc("jdbc:postgresql:dbserver", "schema.tablename", connectionProperties)
    // $example off:jdbc_dataset$
  }
}
