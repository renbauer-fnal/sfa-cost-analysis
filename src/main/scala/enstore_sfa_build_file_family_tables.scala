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

object EnstoreSFABuildFileFamilyTables {

  val MAX_VOLUME_ID = 203498 + 1

  def main(args: Array[String]): Unit = {
    var numPartitions = 100
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
      .option("dbtable", "(select min(id) as min_id, file_family from volume group by file_family) as file_families")
      .option("user", "enstore")
      .option("fetchSize", "10000")
      .option("partitionColumn", "min_id")
      .option("numPartitions", numPartitions)
      .option("lowerBound", 0)
      .option("upperBound", MAX_VOLUME_ID)
      .load()  // Now a DataFrame instead of DataFrameReader

    storageinfoDF.foreachPartition { partition: Iterator[org.apache.spark.sql.Row] =>
          classOf[org.postgresql.Driver]
          val con_str = "jdbc:postgresql://131.225.190.12:5432/billing?user=enstore"
          val conn = DriverManager.getConnection(con_str)
          try {
    partition.foreach { row: org.apache.spark.sql.Row =>
      if (!row.isNullAt(1)) {
        val file_family = row.getString(1)
        val ff_tablename = s"volume_labels_ff_${file_family.replaceAll("[-/]", "_")}"
        val ff_table_create_query = s"create unlogged table if not exists $ff_tablename as (select label from volume where volume.file_family = '$file_family')"
        conn.createStatement().execute(ff_table_create_query)
        conn.createStatement().execute(s"create index if not exists ${ff_tablename}_idx on ${ff_tablename}(label)")
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
