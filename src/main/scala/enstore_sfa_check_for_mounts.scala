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

object EnstoreSFACheckForMounts {

  val MAX_PACKAGEFILE_ID = 764942953 + 1
  val MAX_BILLINGINFO_ID = 1402571980 + 1 

  def main(args: Array[String]): Unit = {
    var shuffle_partitions = 1600
    var packagefilePartitions = 100
    var billinginfoPartitions = 400
    if (args.length > 0) {
        shuffle_partitions = args(0).toInt
    }
    if (args.length > 1) {
        packagefilePartitions = args(1).toInt
    }
    if (args.length > 2) {
        billinginfoPartitions = args(2).toInt
    }
    val spark = SparkSession
      .builder()
      .appName("Spark Enstore SFA Analysis")
      .config("spark.speculation", "false")
      .config("spark.task.maxFailures", 12)
      .config("spark.task.cpus", 10)
      .config("spark.sql.broadcastTimeout", 36000)
      .config("spark.sql.shuffle.partitions", shuffle_partitions)
      .config("spark.local.dir", "/storage/local/data1/spark_temp")
      .getOrCreate()

    runEnstoreSFAAnalysis(spark, packagefilePartitions, billinginfoPartitions)

    spark.stop()
  }

  private def runEnstoreSFAAnalysis(spark: SparkSession, numPackagefilePartitions: Int, numBillinginfoPartitions: Int): Unit = {
    val packagefileDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
      .option("dbtable", "packagefile_by_id_0")
      .option("user", "enstore")
      .option("fetchSize", "10000")
      .option("partitionColumn", "id")  // We partition billinginfo still because it is big big
      .option("numPartitions", numPackagefilePartitions)
      .option("lowerBound", 0)
      .option("upperBound", MAX_PACKAGEFILE_ID)
      .load()  // Now a DataFrame instead of DataFrameReader

    val billinginfoDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
      .option("dbtable", "billinginfo")
      .option("user", "enstore")
      .option("fetchSize", "10000")
      .option("partitionColumn", "billinginfo_id")  // We partition billinginfo still because it is big big
      .option("numPartitions", numBillinginfoPartitions)
      .option("lowerBound", 0)
      .option("upperBound", MAX_BILLINGINFO_ID)
      .load()  // Now a DataFrame instead of DataFrameReader

    val joinDF = billinginfoDF.join(packagefileDF, "pnfsid")  // Default is inner join

    joinDF.write
      .format("jdbc")
      .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
      .option("dbtable", s"packagefile_x_billinginfo")
      .option("user", "enstore")
      .mode(SaveMode.Append)
      .save()
  }
}
