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
import java.sql.{Connection, DriverManager, Statement, ResultSet}

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

object EnstoreSFACalculateMountWaitsBillinginfoPreMounts {

  val MAX_SFBR_ID = 19442848 + 1

  def main(args: Array[String]): Unit = {
    var numPartitions = 400
    if (args.length > 0) {
        numPartitions = args(0).toInt
    }
    val spark = SparkSession
      .builder()
      .appName("Spark Enstore SFA Analysis")
      .config("spark.speculation", "false")
      .config("spark.task.maxFailures", 12)
      .config("spark.task.cpus", 10)
      .config("spark.sql.broadcastTimeout", 36000)
      .config("spark.local.dir", "/storage/local/data1/spark_temp")
      .getOrCreate()

    runEnstoreSFAAnalysis(spark, numPartitions)

    spark.stop()
  }

  private def runEnstoreSFAAnalysis(spark: SparkSession, numPartitions: Int): Unit = {
    val storageinfoDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
      .option("dbtable", "small_file_billinginfo_reads")
      .option("user", "enstore")
      .option("fetchSize", "10000")
      .option("partitionColumn", "sfbr_id")
      .option("numPartitions", numPartitions)
      .option("lowerBound", 6500000)
      .option("upperBound", MAX_SFBR_ID)
      .load()  // Now a DataFrame instead of DataFrameReader

    storageinfoDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          classOf[org.postgresql.Driver]
          val con_str = "jdbc:postgresql://131.225.190.12:5432/billing?user=enstore"
          val conn = DriverManager.getConnection(con_str)
          try {
    partition.foreach { row: org.apache.spark.sql.Row =>
      if (1 == 1) {
        if (!row.isNullAt(1) && !row.isNullAt(4) && !row.isNullAt(5)) {
          val datestamp = row.getTimestamp(1)
          val sfbr_id = row.getInt(4)
          val volume = row.getString(5)
  
          val mount_query = s"select finish, tapemount_id from tape_mounts"
          val datestamp_filter = s"start > (TIMESTAMP '$datestamp' - INTERVAL '100 days') and start < (TIMESTAMP '$datestamp')"
          val correct_volume_filter = s"tape_mounts.volume = '$volume'"
          val rs2 = conn.createStatement().executeQuery(s"$mount_query where $datestamp_filter and $correct_volume_filter order by start desc limit 1")
          if (rs2.next()) { // Found a mount for the volume, good!
            val finish = rs2.getTimestamp(1)
            if (finish != null){
              val mount_id = rs2.getInt(2)
              rs2.close()
              val age = s"AGE('$finish', '$datestamp')"
              val mod_query = s"update small_file_billinginfo_reads set previous_mount_100d_id = $mount_id, previous_mount_100d_wait = $age where sfbr_id = $sfbr_id"
              conn.createStatement().execute(mod_query)
            }
          } else {
            rs2.close()
          }
        }
      }
    ()
    }
          } finally {
            conn.close()
          }
      ()
    }

  }
}
