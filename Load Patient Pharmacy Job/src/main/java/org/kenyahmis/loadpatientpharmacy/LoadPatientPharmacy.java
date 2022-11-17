package org.kenyahmis.loadpatientpharmacy;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        // load lookup tables
        Dataset<Row> lookupRegimenDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.regimen"))
                .load();

        Dataset<Row> lookupTreatmentDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.treatment"))
                .load();

        Dataset<Row> lookupProphylaxisDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.prophylaxis"))
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
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patient_pharmacy");
        targetDf.createOrReplaceTempView("target_patient_pharmacy");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patient_pharmacy t LEFT ANTI JOIN source_patient_pharmacy s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.PatientPharmacyUnique_ID <=> t.PatientPharmacyUnique_ID AND s.VisitID <=>t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");


        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,SiteCode,FacilityName,PatientPK,VisitID," +
                "Drug,DispenseDate,Duration,ExpectedReturn,TreatmentType,PeriodTaken,ProphylaxisType,Emr,Project,CKV," +
                "RegimenLine,RegimenChangedSwitched,RegimenChangeSwitchReason,StopRegimenReason,StopRegimenDate," +
                "PatientUnique_ID,PatientPharmacyUnique_ID FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,SiteCode,FacilityName,PatientPK,VisitID," +
                "Drug,DispenseDate,Duration,ExpectedReturn,TreatmentType,PeriodTaken,ProphylaxisType,Emr,Project,CKV," +
                "RegimenLine,RegimenChangedSwitched,RegimenChangeSwitchReason,StopRegimenReason,StopRegimenDate," +
                "PatientUnique_ID,PatientPharmacyUnique_ID FROM source_patient_pharmacy");

        sourceMergeDf2.printSchema();
        unmatchedMergeDf1.printSchema();


        // Will "update" all rows matched, insert new rows and maintain any unmatched rows
        Dataset<Row> finalMergeDf = unmatchedMergeDf1.union(sourceMergeDf2);

        long mergedFinalCount = finalMergeDf.count();
        logger.info("Merged final count: " + mergedFinalCount);

        logger.info("Writing final dataframe to target table");
        // Write to target table
        finalMergeDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
