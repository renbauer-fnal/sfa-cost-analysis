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

object EnstoreSFACalculateRestoreVolumes {

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
      if (!row.isNullAt(20) && !row.isNullAt(4) && !row.isNullAt(7) && !row.isNullAt(12)) {
        val updated = row.getBoolean(20)
        val action = row.getString(4)
        val errorcode = row.getInt(7)
        val bfid = row.getString(12)
        if (!updated && (action == "restore") && (errorcode == 0) && bfid.substring(0, 4) == "CDMS" && !row.isNullAt(11) && !row.isNullAt(13)) {
          val epoch = bfid.substring(4,14).toInt
          val storageinfo_id = row.getInt(11)
          val file_family = row.getString(13)

          val mount_query = s"select * from filtered_tape_mounts"
          val datestamp_filter = s"start > TO_TIMESTAMP(${epoch}) and start < TO_TIMESTAMP(${epoch + 8640000})"
          val correct_file_family_filter = s"filtered_tape_mounts.file_family = '$file_family'"
          val full_query = s"$mount_query where $datestamp_filter and $correct_file_family_filter order by start limit 1"
          val rs2 = conn.createStatement().executeQuery(full_query)
          if (rs2.next()) { // Found a mount for the file family, good!
            val volume = rs2.getString(1)
            rs2.close()
            val mod_query = s"update storageinfo_sfa set updated = True, volume = '$volume' where storageinfo_id = $storageinfo_id"
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
