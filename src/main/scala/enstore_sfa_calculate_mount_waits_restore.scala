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

object EnstoreSFACalculateMountWaitsRestore {

  val MAX_STORAGEINFO_ID = 224571861 + 1

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
      .option("dbtable", "storageinfo_sfa")
      .option("user", "enstore")
      .option("fetchSize", "10000")
      .option("partitionColumn", "storageinfo_id")
      .option("numPartitions", numPartitions)
      .option("lowerBound", 0)
      .option("upperBound", MAX_STORAGEINFO_ID)
      .load()  // Now a DataFrame instead of DataFrameReader

    storageinfoDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          classOf[org.postgresql.Driver]
          val con_str = "jdbc:postgresql://131.225.190.12:5432/billing?user=enstore"
          val conn = DriverManager.getConnection(con_str)
          try {
    partition.foreach { row: org.apache.spark.sql.Row =>
      if (!row.isNullAt(20) && !row.isNullAt(4) && !row.isNullAt(7)) {
        val updated = row.getBoolean(20)
        val action = row.getString(4)
        val errorcode = row.getInt(7)
        if (!updated && (action == "restore") && errorcode == 0 && !row.isNullAt(6) && !row.isNullAt(11) && !row.isNullAt(15)) {
          val datestamp = row.getTimestamp(6)
          val storageinfo_id = row.getInt(11)
          val volume_label = row.getString(15)
  
          val mount_query = s"select start from tape_mounts"
          val datestamp_filter = s"start > '$datestamp' and start < (TIMESTAMP '$datestamp' + INTERVAL '100 days')"
          val correct_volume_filter = s"tape_mounts.volume = '$volume_label'"
          val rs2 = conn.createStatement().executeQuery(s"$mount_query where $datestamp_filter and $correct_volume_filter order by start limit 1")
          if (rs2.next()) { // Found a mount for the volume, good!
            val start = rs2.getTimestamp(1)
            rs2.close()
            val mod_query = s"update storageinfo_sfa set updated = True, next_mount_time = '$start', mount_wait = AGE('$start', '$datestamp') where storageinfo_id = $storageinfo_id"
            conn.createStatement().execute(mod_query)
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
