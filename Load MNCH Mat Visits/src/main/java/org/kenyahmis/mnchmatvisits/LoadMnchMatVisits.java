package org.kenyahmis.mnchmatvisits;

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

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class LoadMnchMatVisits {
    private static final Logger logger = LoggerFactory.getLogger(LoadMnchMatVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Mat Visits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMnchMatVisits.sql";
        String sourceQuery;
        InputStream inputStream = LoadMnchMatVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch mat visits from file");
        }
        logger.info("Loading source mnch mat visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.mnchcentral.url"))
                .option("driver", rtConfig.get("spark.mnchcentral.driver"))
                .option("user", rtConfig.get("spark.mnchcentral.user"))
                .option("password", rtConfig.get("spark.mnchcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target mnch mat visits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", rtConfig.get("spark.ods.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_mnch_mat_visits");
        sourceDf.createOrReplaceTempView("source_mnch_mat_visits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_mat_visits s LEFT ANTI JOIN " +
                "target_mnch_mat_visits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PatientMnchIDHash", upper(sha2(col("PatientMnchID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        newRecordsJoinDf = session.sql("select PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status,StatusDate," +
                "DateExtracted,FacilityId,PatientMnchID,FacilityName,VisitID,VisitDate,AdmissionNumber,ANCVisits," +
                "DateOfDelivery,DurationOfDelivery,GestationAtBirth,ModeOfDelivery,PlacentaComplete,UterotonicGiven," +
                "VaginalExamination,BloodLoss,BloodLossVisual,ConditonAfterDelivery,MaternalDeath,DeliveryComplications," +
                "NoBabiesDelivered,BabyBirthNumber,SexBaby,BirthWeight,BirthOutcome,BirthWithDeformity,TetracyclineGiven," +
                "InitiatedBF,ApgarScore1,ApgarScore5,ApgarScore10,KangarooCare,ChlorhexidineApplied,VitaminKGiven," +
                "StatusBabyDischarge,MotherDischargeDate,SyphilisTestResults,HIVStatusLastANC,HIVTestingDone,HIVTest1," +
                "HIV1Results,HIVTest2,HIV2Results,HIVTestFinalResult,OnARTANC,BabyGivenProphylaxis,MotherGivenCTX," +
                "PartnerHIVTestingMAT,PartnerHIVStatusMAT,CounselledOn,ReferredFrom,ReferredTo,ClinicalNotes," +
                "PatientPKHash,PatientMnchIDHash" +
                " from new_records");

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", rtConfig.get("spark.ods.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
