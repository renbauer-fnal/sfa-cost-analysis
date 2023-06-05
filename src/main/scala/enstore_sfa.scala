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

object EnstoreSFAAnalysis {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark Enstore SFA Analysis")
      .getOrCreate()

    runEnstoreSFAAnalysis(spark)

    spark.stop()
  }

  private def runEnstoreSFAAnalysis(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
      .load()
      // .option("dbtable", "file")
      // .option("user", "enstore_reader")
      // .option("partitionColumn", "sanity_crc")
      // .option("numPartitions", 8)
      // .option("lowerBound", 0)
      // .option("upperBound", 999999999)

    jdbcDF.groupBy("original_library").count().show()

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
