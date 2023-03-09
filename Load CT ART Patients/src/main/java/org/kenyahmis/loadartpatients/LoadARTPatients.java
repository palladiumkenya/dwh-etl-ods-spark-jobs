package org.kenyahmis.loadartpatients;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class LoadARTPatients {
    private static final Logger logger = LoggerFactory.getLogger(LoadARTPatients.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load ART Patients");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceARTPatients.sql";
        String sourceQuery;
        InputStream inputStream = LoadARTPatients.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source ART patients query from file", e);
            return;
        }

        logger.info("Loading source ART Patients");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();


        Dataset<Row> lookupExitReasonDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.exitReason"))
                .load();
        Dataset<Row> lookupRegimenDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.regimen"))
                .load();

        Dataset previousRegimenLookup = lookupRegimenDf.alias("previous_regimen_lookup");
        Dataset lastRegimenLookup = lookupRegimenDf.alias("last_regimen_lookup");
        Dataset startRegimenLookup = lookupRegimenDf.alias("start_regimen_lookup");

        Dataset<Row> lookupPatientSourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.patientSource"))
                .load();

        // clean source art records
        sourceDf = sourceDf
                .withColumn("DOB", when(col("DOB").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("DOB").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DOB")))
                .withColumn("StartARTDate", when(col("StartARTDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("StartARTDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("StartARTDate")))
                .withColumn("StartARTAtThisFacility", when(col("StartARTAtThisFacility").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("StartARTAtThisFacility").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("StartARTAtThisFacility")))
                .withColumn("LastARTDate", when(col("LastARTDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("LastARTDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("LastARTDate")))
                .withColumn("RegistrationDate", when(col("RegistrationDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("RegistrationDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("RegistrationDate")))
                .withColumn("PreviousARTStartDate", when(col("PreviousARTStartDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("PreviousARTStartDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("PreviousARTStartDate")))
                .withColumn("ExpectedReturn", when(col("ExpectedReturn").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("ExpectedReturn").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ExpectedReturn")))
                .withColumn("LastVisit", when(col("LastVisit").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("LastVisit").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("LastVisit")))
                .withColumn("ExitDate", when(col("ExitDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("ExitDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ExitDate")))
                .withColumn("Emr", when(col("Emr").equalTo("Open Medical Records System - OpenMRS"), "OpenMRS")
                        .when(col("Emr").equalTo("Ampath AMRS"), "AMRS")
                        .otherwise(col("Emr")))
                .withColumn("Project", when(col("Project").isin("Ampathplus", "AMPATH"), "Ampath Plus")
                        .when(col("Project").isin("UCSF Clinical Kisumu", "CHAP Uzima", "DREAM", "IRDO"), "Kenya HMIS II")
                        .otherwise(col("Project")))
                .withColumn("Duration", when(col("Duration").cast(DataTypes.FloatType).lt(lit(0)), lit(999))
                        .otherwise(col("Duration")))
                .withColumn("AgeARTStart", when(col("AgeARTStart").lt(lit(0))
                        .or(col("AgeARTStart").gt(lit(120))), lit(999))
                        .otherwise(col("AgeARTStart")))
                .withColumn("AgeLastVisit", when(col("AgeLastVisit").lt(lit(0))
                        .or(col("AgeLastVisit").gt(lit(120))), lit(999))
                        .otherwise(col("AgeLastVisit")))
                .withColumn("AgeEnrollment", when(col("AgeEnrollment").lt(lit(0))
                        .or(col("AgeEnrollment").gt(lit(120))), lit(999))
                        .otherwise(col("AgeEnrollment")));

        // Add values from lookup tables
        sourceDf = sourceDf
                .join(lookupExitReasonDf, sourceDf.col("ExitReason").equalTo(lookupExitReasonDf.col("source_name")), "left")
                .join(previousRegimenLookup, sourceDf.col("PreviousARTRegimen").equalTo(previousRegimenLookup.col("source_name")), "left")
                .join(startRegimenLookup, sourceDf.col("StartRegimen").equalTo(startRegimenLookup.col("source_name")), "left")
                .join(lastRegimenLookup, sourceDf.col("LastRegimen").equalTo(lastRegimenLookup.col("source_name")), "left")
                .join(lookupPatientSourceDf, sourceDf.col("PatientSource").equalTo(lookupPatientSourceDf.col("source_name")), "left")
                .withColumn("ExitReason", when(lookupExitReasonDf.col("target_name").isNotNull(), lookupExitReasonDf.col("target_name"))
                        .otherwise(col("ExitReason")))
                .withColumn("PreviousARTRegimen", when(col("previous_regimen_lookup.target_name").isNotNull(), col("previous_regimen_lookup.target_name"))
                        .otherwise(col("ExitReason")))
                .withColumn("StartRegimen", when(col("start_regimen_lookup.target_name").isNotNull(), col("start_regimen_lookup.target_name"))
                        .otherwise(col("StartRegimen")))
                .withColumn("LastRegimen", when(col("last_regimen_lookup.target_name").isNotNull(), col("last_regimen_lookup.target_name"))
                        .otherwise(col("LastRegimen")));

        sourceDf.persist(StorageLevel.DISK_ONLY());
        logger.info("Loading target ART Patients");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();
        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patients");
        targetDf.createOrReplaceTempView("target_patients");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patients s LEFT ANTI JOIN target_patients t ON s.PatientPK <=> t.PatientPK AND" +
                " s.SiteCode <=> t.SiteCode");
        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newVisitCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newVisitCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("SELECT" +
                " PatientID,PatientPK,SiteCode,FacilityName,AgeEnrollment," +
                "AgeARTStart,AgeLastVisit,RegistrationDate,PatientSource,Gender,StartARTDate,PreviousARTStartDate," +
                "PreviousARTRegimen,StartARTAtThisFacility,StartRegimen,StartRegimenLine,LastARTDate,LastRegimen," +
                "LastRegimenLine,Duration,ExpectedReturn,Provider,LastVisit,ExitReason,ExitDate,Emr," +
                "Project,DOB,PreviousARTUse,PreviousARTPurpose,DateLastUsed,DateAsOf,PatientPKHash,PatientIDHash" +
                " FROM new_records");

        // Write to target table
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
