package org.kenyahmis.loadpatientpharmacy;

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

public class LoadPatientPharmacy {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientPharmacy.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Patient Pharmacy");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourcePatientPharmacy.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientPharmacy.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load visits query from file", e);
            return;
        }

        logger.info("Loading source patient pharmacy");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        // load lookup tables
        Dataset<Row> lookupRegimenDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_regimen")
                .option("query", "select source_name,target_name from dbo.lkp_regimen")
                .load();

        Dataset<Row> lookupTreatmentDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_treatment_type")
                .option("query", "select source_name,target_name from dbo.lkp_treatment_type")
                .load();

        Dataset<Row> lookupProphylaxisDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_prophylaxis_type")
                .option("query", "select source_name,target_name from dbo.lkp_prophylaxis_type")
                .load();

        sourceDf = sourceDf
                .withColumn("Duration", when(col("Duration").cast(DataTypes.FloatType).lt(lit(0)), lit(999)))
                .withColumn("ExpectedReturn", when(col("ExpectedReturn").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1)))),
                        lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ExpectedReturn")))
                .withColumn("PeriodTaken", when(col("PeriodTaken").cast(DataTypes.FloatType).leq(lit(0)), lit(999)))
                .withColumn("Emr", when(col("Emr").equalTo("Open Medical Records System - OpenMRS"), "OpenMRS")
                        .when(col("Emr").equalTo("Ampath AMRS"), "AMRS")
                        .otherwise(col("Emr")))
                .withColumn("Project", when(col("Project").isin("Ampathplus", "AMPATH"), "Ampath Plus")
                        .when(col("Project").isin("UCSF Clinical Kisumu", "CHAP Uzima", "DREAM", "IRDO"), "Kenya HMIS II")
                        .otherwise(col("Project")));

        // set values from lookup tables
        sourceDf = sourceDf
                .join(lookupRegimenDf, sourceDf.col("Drug")
                        .equalTo(lookupRegimenDf.col("source_name")), "left")
                .join(lookupTreatmentDf, sourceDf.col("TreatmentType")
                        .equalTo(lookupTreatmentDf.col("source_name")), "left")
                .join(lookupProphylaxisDf, sourceDf.col("ProphylaxisType")
                        .equalTo(lookupProphylaxisDf.col("source_name")), "left")
                .withColumn("Drug", when(lookupRegimenDf.col("target_name").isNotNull(), lookupRegimenDf.col("target_name"))
                        .otherwise(col("Drug")))
                .withColumn("TreatmentType", when(lookupTreatmentDf.col("target_name").isNotNull(), lookupTreatmentDf.col("target_name"))
                        .otherwise(col("TreatmentType")))
                .withColumn("ProphylaxisType", when(lookupProphylaxisDf.col("target_name").isNotNull(), lookupProphylaxisDf.col("target_name"))
                        .otherwise(col("ProphylaxisType")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target patient pharmacy");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.CT_PatientPharmacy")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patient_pharmacy");
        targetDf.createOrReplaceTempView("target_patient_pharmacy");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patient_pharmacy s LEFT ANTI JOIN target_patient_pharmacy t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,SiteCode,FacilityName,PatientPK,VisitID,Drug,DispenseDate," +
                "Duration,ExpectedReturn,TreatmentType,PeriodTaken,ProphylaxisType,Emr,Project,RegimenLine," +
                "RegimenChangedSwitched,RegimenChangeSwitchReason,StopRegimenReason,StopRegimenDate," +
                "Date_Created,Date_Last_Modified";

        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_PatientPharmacy")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_PatientPharmacy", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
