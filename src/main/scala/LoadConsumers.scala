package com.paidy.dar.interview

import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType, TimestampType, StringType}
import java.sql.Timestamp


object LoadConsumers {

  val TargetTableName: String = "consumers"
  val TargetTableSchema: String =
    """
      |    id STRING NOT NULL,
      |    status STRING NOT NULL,
      |    email STRING NOT NULL,
      |    phone STRING NOT NULL,
      |    name1 STRING,
      |    name2 STRING,
      |    date_of_birth DATE,
      |    address_line1 STRING,
      |    address_line2 STRING,
      |    address_city STRING,
      |    address_state STRING,
      |    address_zip STRING,
      |    source STRING,
      |    agent STRING,
      |    email_verification_status STRING,
      |    email_verified_at TIMESTAMP,
      |    created_at TIMESTAMP NOT NULL,
      |    updated_at TIMESTAMP NOT NULL,
      |    enabled_at TIMESTAMP,
      |    disabled_at TIMESTAMP,
      |    closed_at TIMESTAMP,
      |    created_at_year SHORT NOT NULL
      |""".stripMargin

  final def main(args: Array[String]): Unit = {
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")
    args.foreach(arg => println(s"Received argument: $arg"))
    println("++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++")

    // Parse the arguments for the time interval of the source data we want to process
    val (start, end) = args match {
      case Array() => (Timestamp.valueOf("2020-01-01 00:00:00"), new Timestamp(System.currentTimeMillis()))
      case Array(arg0) => (Timestamp.valueOf(arg0), new Timestamp(System.currentTimeMillis()))
      case Array(arg0, arg1) => (Timestamp.valueOf(arg0), Timestamp.valueOf(arg1))
      case _ => throw new IllegalArgumentException("At most two arguments formatted as yyyy-mm-dd hh:mm:ss timestamps are accepted.")
    }

    val spark = createSparkSession()
    
    if (!spark.catalog.tableExists(TargetTableName)) {
    spark.createDataFrame(spark.sparkContext.emptyRDD[Row], DataType.fromDDL(TargetTableSchema).asInstanceOf[StructType])
        .write.partitionBy("created_at_year")
        .format("delta") // or "parquet" or "orc" based on your storage format
        .saveAsTable(TargetTableName)
    }

    val df = spark.read.format("delta").table(TargetTableName)
    println("\n\n============================== Table before update =============================================\n\n")
    df.show()

    // Load and filter the event data
    val sourceDF = ConsumerUtils.loadEventData(spark, start, end)
    println("\n\n============================== Source events =============================================\n\n")
    sourceDF.show()

    // Perform quality checks
    QualityChecks.checkCompleteness(sourceDF, Seq("id", "event", "reason", "timestamp"))

    // Transform
    val transformedEventsDF = ConsumerUtils.transformEventsData(sourceDF, spark)
    val dedupedTransformedEventsDF = transformedEventsDF.dropDuplicates("id")    // table merge cannot be performed because multiple source rows attempt to update the table
    println("\n\n============================== Transformed Events before table update =============================================\n\n")
    transformedEventsDF.show()

    // Perform more quality checks
    QualityChecks.checkAccuracy(dedupedTransformedEventsDF)
    QualityChecks.checkCompleteness(dedupedTransformedEventsDF, Seq("id"))
    QualityChecks.checkConsistency(dedupedTransformedEventsDF)
    QualityChecks.checkUniqueness(dedupedTransformedEventsDF, "id")

    // Update the consumers table
    ConsumerUtils.updateTargetTable(dedupedTransformedEventsDF, TargetTableName, spark)
    println("\n\n============================== Updated table =============================================\n\n")
    val fdf = spark.read.format("delta").table(TargetTableName)
    fdf.show()

    // //Truncate table if needed
    // truncateTable(TargetTableName, spark)

    spark.stop()
  }

  private def createSparkSession(): SparkSession = {
    val builder = SparkSession.builder

    // Delta specific configuration
    builder.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    builder.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    builder.enableHiveSupport()
    builder.master("local[4]")

    builder.getOrCreate()
  }
}
