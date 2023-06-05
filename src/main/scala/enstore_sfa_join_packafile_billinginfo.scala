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

object EnstoreSFAJoinPackagefileBillinginfo {

  val MAX_CRC = 4293984228.0
  val MAX_ID = 764942953.0 + 1.0
  val MAX_BILLINGINFO_ID = 1402571980 + 1

  def main(args: Array[String]): Unit = {
    var shuffle_partitions = 1600
    if (args.length > 2) {
        shuffle_partitions = args(2).toInt
    }
    val spark = SparkSession
      .builder()
      .appName("Spark Enstore SFA Analysis")
      .config("spark.speculation", "false")
      .config("spark.task.maxFailures", 12)
      .config("spark.task.cpus", 10)
      .config("spark.sql.broadcastTimeout", 36000)
      .config("spark.sql.shuffle.partitions", shuffle_partitions)
      .getOrCreate()
    var my_args = Array(32, 10)
    if (args.length > 0) {
       my_args(0) = args(0).toInt
    }
    if (args.length > 1) {
       my_args(1) = args(1).toInt
    }
    runEnstoreSFAAnalysis(spark, my_args(0), my_args(1))

    spark.stop()
  }

  private def runEnstoreSFAAnalysis(spark: SparkSession, billinginfoStages: Int, packagefileStages: Int): Unit = {
    val partition = (0 to packagefileStages - 1).toList  // .par  // perform merge on these partitions in order
    partition.foreach { x =>
      val stage_size = (MAX_ID / packagefileStages).ceil.toInt
      val id_lower = x * stage_size
      val id_upper = (x + 1) * stage_size
      // work with small bits of packagefile table at a time
      // val query = s"(select * from packagefile_by_id_0 where id >= $id_lower and id < $id_upper) as packagefile_partition"
      val packagefileDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
        .option("dbtable", "packagefile_by_id_0")
        .option("user", "enstore")
        .option("fetchSize", "10000")
        .load()  // Now a DataFrame instead of DataFrameReader
        // .option("dbtable", query)
        // No longer partition the little bits we take
        // .option("partitionColumn", "id")
        // .option("billinginfoStages", billinginfoStages)
        // .option("lowerBound", id_lower)
        // .option("upperBound", id_upper)

      val bi_partition = (0 to billinginfoStages - 1).toList.par
      bi_partition.foreach { x =>
        val bi_stage_size = (MAX_BILLINGINFO_ID / billinginfoStages).ceil.toInt
        val bi_id_lower = x * bi_stage_size
        val bi_id_upper = (x + 1) * bi_stage_size
        // val bi_query = s"(select * from billinginfo where billinginfo_id >= $bi_id_lower and billinginfo_id < $bi_id_upper) as billinginfo_partition"
        val billinginfoDF = spark.read
        .format("jdbc")
        .option("url", "jdbc:postgresql://131.225.190.12:5432/billing")
        .option("dbtable", "billinginfo")
        .option("user", "enstore")
        .option("fetchSize", "10000")
        .load()  // Now a DataFrame instead of DataFrameReader
        // .option("dbtable", bi_query)
        // .option("partitionColumn", "billinginfo_id")  // We partition billinginfo still because it is big big
        // .option("billinginfoStages", numBillinginfoPartitions)
        // .option("lowerBound", 0)
        // .option("upperBound", MAX_BILLINGINFO_ID)

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
  }
}
