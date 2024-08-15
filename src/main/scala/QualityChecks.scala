package com.paidy.dar.interview

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object QualityChecks {
    def checkTimeliness(df: DataFrame, timestampColumn: String, timeWindow: Int, spark: SparkSession): Unit = {
        // Check for time window
        import spark.implicits._
        val latestTimestamp = df.agg(max(col(timestampColumn))).as[Timestamp].head()
        val cutoff = new Timestamp(System.currentTimeMillis() - timeWindow * 60 * 60 * 1000L) // Time window in hours
        assert(latestTimestamp.after(cutoff), s"Data is not updated within the last $timeWindow hours")
    }

    def checkUniqueness(df: DataFrame, uniqueKey: String): Unit = {
        // Check for duplicate records
        val duplicateCount = df.groupBy(uniqueKey).count().filter(col("count") > 1).count()
        assert(duplicateCount == 0, s"Duplicate records found based on key $uniqueKey")
    }

    def checkConsistency(df: DataFrame): Unit = {
        // Example: Check that 'email_verified_at' is not null if 'email_verification_status' is 'verified'
        val inconsistentRecords = df.filter(col("email_verification_status") === "verified" && col("email_verified_at").isNull)
        assert(inconsistentRecords.count() == 0, "Inconsistent records found in email verification status")
    }

    def checkAccuracy(df: DataFrame): Unit = {
        // Check for valid email addresses
        val emailPattern = "^[A-Za-z0-9+_.-]+@(.+)$"
        val invalidEmails = df.filter(!col("email").rlike(emailPattern))
        assert(invalidEmails.count() == 0, s"Found invalid email addresses")
    }

    def checkCompleteness(df: DataFrame, requiredColumns: Seq[String]): Unit = {
        // Check if all required columns are present
        requiredColumns.foreach { column =>
            val countNulls = df.filter(col(column).isNull).count()
            assert(countNulls == 0, s"Column $column contains null values")
        }
    }
}