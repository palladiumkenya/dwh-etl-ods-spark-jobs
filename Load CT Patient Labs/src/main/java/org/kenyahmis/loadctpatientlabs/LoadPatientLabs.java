package org.kenyahmis.loadctpatientlabs;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.kenyahmis.core.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class LoadPatientLabs {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientLabs.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Patient Labs");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceCTPatientLabs.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientLabs.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source CT patient labs query from file", e);
            return;
        }

        logger.info("Loading source CT patient labs");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        Dataset<Row> lookupTestNamesDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_test_name")
                .option("query", "select source_name,target_name from dbo.lkp_test_name")
                .load();

        sourceDf = sourceDf
                .withColumn("ReportedbyDate", when(col("ReportedbyDate").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("ReportedbyDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ReportedbyDate")))
                .withColumn("OrderedbyDate", when(col("OrderedbyDate").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("OrderedbyDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("OrderedbyDate")))
                .withColumn("Emr", when(col("Emr").equalTo("Open Medical Records System - OpenMRS"), "OpenMRS")
                        .when(col("Emr").equalTo("Ampath AMRS"), "AMRS")
                        .otherwise(col("Emr")))
                .withColumn("TestResult",when(col("TestResult").cast(DataTypes.FloatType).lt(lit(0)), "Viral Load")
                .otherwise(col("TestResult")));

        // set values from lookup tables
        sourceDf = sourceDf
                .join(lookupTestNamesDf, sourceDf.col("TestName")
                        .equalTo(lookupTestNamesDf.col("source_name")), "left")
                .withColumn("TestName", when(lookupTestNamesDf.col("target_name").isNotNull(), lookupTestNamesDf.col("target_name"))
                        .otherwise(col("TestName")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target CT patient labs");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_PatientLabs")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_patient_labs");
        targetDf.createOrReplaceTempView("target_patient_labs");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patient_labs s LEFT ANTI JOIN target_patient_labs t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "ID,PatientID,PatientPk,SiteCode,FacilityName,VisitID,OrderedbyDate,ReportedbyDate," +
                "TestName,EnrollmentTest,TestResult,Emr,Project,DateSampleTaken,SampleType,reason,Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", columnList));

        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_PatientLabs")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_PatientLabs", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
