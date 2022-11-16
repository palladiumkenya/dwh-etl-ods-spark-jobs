package org.kenyahmis.loadpatientstatus;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class LoadPatientStatus {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientStatus.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Patient Status");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourcePatientStatus.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientStatus.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source patient status query from file", e);
            return;
        }

        logger.info("Loading source patient status");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDf = sourceDf
                .withColumn("ExitDate", when(col("ExitDate").lt(lit(Date.valueOf(LocalDate.of(2004, 1, 1))))
                        .or(col("ExitDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ExitDate")))
                .withColumn("Emr", when(col("Emr").equalTo("Ampath AMRS"), "AMRS")
                        .otherwise(col("Emr")))
                .withColumn("Project", when(col("Project").equalTo("Ampathplus"), "Ampath Plus")
                        .when(col("Project").isin("UCSF Clinical Kisumu", "CHAP Uzima", "DREAM Kenya Trusts", "IRDO"), "Kenya HMIS II")
                        .otherwise(col("Project")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target patient status");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_patient_status");
        targetDf.createOrReplaceTempView("target_patient_status");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patient_status t LEFT ANTI JOIN source_patient_status s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.PatientStatusUnique_ID <=> t.PatientStatusUnique_ID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,SiteCode,FacilityName,ExitDescription,ExitDate,ExitReason," +
                "    PatientPK,Emr,Project,CKV,TOVerified,TOVerifiedDate,ReEnrollmentDate," +
                "    DeathDate,PatientUnique_ID,PatientStatusUnique_ID" +
                " FROM final_unmatched");
        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,SiteCode,FacilityName,ExitDescription,ExitDate,ExitReason," +
                "    PatientPK,Emr,Project,CKV,TOVerified,TOVerifiedDate,ReEnrollmentDate," +
                "    DeathDate,PatientUnique_ID,PatientStatusUnique_ID" +
                " FROM source_patient_status");

        sourceDf.printSchema();
        unmatchedMergeDf1.printSchema();

        Dataset<Row> dfMergeFinal = unmatchedMergeDf1.union(sourceMergeDf2);
        long mergedFinalCount = dfMergeFinal.count();
        logger.info("Merged final count: " + mergedFinalCount);

        logger.info("Writing final dataframe to target table");
        // Write to target table
        dfMergeFinal
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
