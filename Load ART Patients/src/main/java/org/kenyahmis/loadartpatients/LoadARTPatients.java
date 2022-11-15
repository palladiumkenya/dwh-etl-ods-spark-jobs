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

import static org.apache.spark.sql.functions.lit;

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

        // Find rows in target table unmatched in source table
        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patients t LEFT ANTI JOIN source_patients s ON s.PatientPK <=> t.PatientPK AND" +
                " s.SiteCode <=> t.SiteCode");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);

        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT" +
                " PatientID,PatientPK,SiteCode,FacilityName,AgeEnrollment," +
                "AgeARTStart,AgeLastVisit,RegistrationDate,PatientSource,Gender,StartARTDate,PreviousARTStartDate," +
                "PreviousARTRegimen,StartARTAtThisFacility,StartRegimen,StartRegimenLine,LastARTDate,LastRegimen," +
                "LastRegimenLine,Duration,ExpectedReturn,Provider,LastVisit,ExitReason,ExitDate,Emr," +
                "Project,DOB,CKV,PreviousARTUse,PreviousARTPurpose,DateLastUsed,DateAsOf" +
                "FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT" +
                " PatientID,PatientPK,SiteCode,FacilityName,AgeEnrollment," +
                "AgeARTStart,AgeLastVisit,RegistrationDate,PatientSource,Gender,StartARTDate,PreviousARTStartDate," +
                "PreviousARTRegimen,StartARTAtThisFacility,StartRegimen,StartRegimenLine,LastARTDate,LastRegimen," +
                "LastRegimenLine,Duration,ExpectedReturn,Provider,LastVisit,ExitReason,ExitDate,Emr," +
                "Project,DOB,CKV,PreviousARTUse,PreviousARTPurpose,DateLastUsed,DateAsOf" +
                "FROM source_patients");

        sourceDf.printSchema();
        unmatchedMergeDf1.printSchema();

        Dataset<Row> dfMergeFinal = unmatchedMergeDf1.union(sourceMergeDf2);
        long mergedFinalCount = dfMergeFinal.count();
        logger.info("Merged final count: " + mergedFinalCount);

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
