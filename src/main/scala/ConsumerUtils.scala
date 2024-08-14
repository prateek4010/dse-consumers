package com.paidy.dar.interview

import io.delta.tables._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, StructType, TimestampType}
import java.sql.Timestamp

object ConsumerUtils {

    def loadEventData(spark: SparkSession, start: Timestamp, end: Timestamp): DataFrame = {
        val eventsDF = spark.read.json("src/test/resources/consumer_events")
        eventsDF.filter(col("timestamp").between(start, end))     // filtering the incoming data for a specific time interval
    }

    def transformEventsData(eventsDF: DataFrame, spark: SparkSession): DataFrame = {
        val dfWithFields = eventsDF
        .withColumn("email", get_json_object(col("event"), "$.email"))
        .withColumn("phone", get_json_object(col("event"), "$.phone"))
        .withColumn("name1", get_json_object(col("event"), "$.name1"))
        .withColumn("name2", get_json_object(col("event"), "$.name2"))
        .withColumn("dateOfBirth", get_json_object(col("event"), "$.dateOfBirth"))
        .withColumn("address", get_json_object(col("event"), "$.address"))
        .withColumn("source", get_json_object(col("event"), "$.source"))
        .withColumn("emailVerificationInfo", get_json_object(col("event"), "$.emailVerificationInfo"))
        .withColumn("agent", get_json_object(col("event"), "$.agent"))
        .withColumn("eventReason", get_json_object(col("event"), "$.reason")) // For reason in event JSON

        // Extract address fields
        val dfWithAddress = dfWithFields
        .withColumn("address_line1", get_json_object(col("address"), "$[1].line1"))
        .withColumn("address_line2", get_json_object(col("address"), "$[1].line2"))
        .withColumn("address_city", get_json_object(col("address"), "$[1].city"))
        .withColumn("address_state", get_json_object(col("address"), "$[1].state"))
        .withColumn("address_zip", get_json_object(col("address"), "$[1].zip"))

        // Extract fields from emailVerificationInfo list
        val dfWithEmailVerification = dfWithAddress
        .withColumn("emailVerificationStatus", get_json_object(col("emailVerificationInfo"), "$[1].status"))
        .withColumn("emailVerifiedAt", get_json_object(col("emailVerificationInfo"), "$[1].verifiedAt"))

        // Handle different 'reason' values to determine status
        val dfWithStatus = dfWithEmailVerification.withColumn("status", 
        when(col("reason") === "Disabled", "disabled")
        .when(col("reason") === "Enabled", "enabled")
        .when(col("reason") === "Updated", "closed")
        .otherwise(lit(null))
        )

        // Add timestamp columns
        val dfWithTimestamps = dfWithStatus
        .withColumn("created_at", to_timestamp(col("timestamp")))
        .withColumn("updated_at", to_timestamp(col("timestamp")))
        .withColumn("created_at_year", year(col("created_at")))

        val dfFinal = dfWithTimestamps.select(
        col("id"),
        col("status"),
        col("email"),
        col("phone"),
        col("name1"),
        col("name2"),
        col("dateOfBirth").cast("date").as("date_of_birth"),
        col("address_line1"),
        col("address_line2"),
        col("address_city"),
        col("address_state"),
        col("address_zip"),
        col("source"),
        col("agent"),
        col("emailVerificationStatus").as("email_verification_status"),
        to_timestamp(col("emailVerifiedAt")).as("email_verified_at"),
        col("created_at"),
        col("updated_at"),
        when(col("status") === "enabled", col("created_at")).as("enabled_at"),
        when(col("status") === "disabled", col("created_at")).as("disabled_at"),
        when(col("status") === "closed", col("created_at")).as("closed_at"),
        col("created_at_year")
        )

        dfFinal
    }

    def updateTargetTable(eventsDF: DataFrame, targetTableName: String, spark: SparkSession): Any = {
        val deltaTable = DeltaTable.forName(spark, targetTableName)
        
        deltaTable.as("target")
        .merge(
            eventsDF.as("source"),
            "source.id = target.id"
        )
        .whenMatched()
        .update(Map(
            "status" -> col("source.status"),
            "email" -> col("source.email"),
            "phone" -> col("source.phone"),
            "name1" -> col("source.name1"),
            "name2" -> col("source.name2"),
            "date_of_birth" -> col("source.date_of_birth"),
            "address_line1" -> col("source.address_line1"),
            "address_line2" -> col("source.address_line2"),
            "address_city" -> col("source.address_city"),
            "address_state" -> col("source.address_state"),
            "address_zip" -> col("source.address_zip"),
            "source" -> col("source.source"),
            "agent" -> col("source.agent"),
            "email_verification_status" -> col("source.email_verification_status"),
            "email_verified_at" -> col("source.email_verified_at"),
            "updated_at" -> col("source.updated_at"),
            "enabled_at" -> col("source.enabled_at"),
            "disabled_at" -> col("source.disabled_at"),
            "closed_at" -> col("source.closed_at")
        ))
        .whenNotMatched()
        .insertAll()
        .execute()
    }

    def truncateTable(targetTableName: String, spark: SparkSession): Unit = {
        val deltaTable = DeltaTable.forName(spark, targetTableName)
        deltaTable.delete()  // Deletes all rows in the Delta table
    }
}
