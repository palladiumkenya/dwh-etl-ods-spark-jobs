package org.kenyahmis.mnchancvisits;

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

public class LoadMnchAncVisits {
    private static final Logger logger = LoggerFactory.getLogger(LoadMnchAncVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH ANC Visits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMnchAncVisits.sql";
        String sourceQuery;
        InputStream inputStream = LoadMnchAncVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch anc visits query from file");
        }
        logger.info("Loading source mnch anc visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.mnchcentral.url"))
                .option("driver", rtConfig.get("spark.mnchcentral.driver"))
                .option("user", rtConfig.get("spark.mnchcentral.user"))
                .option("password", rtConfig.get("spark.mnchcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.mnchcentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());


        logger.info("Loading target mnch anc visits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.MNCH_AncVisits")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_mnch_anc_visits");
        sourceDf.createOrReplaceTempView("source_mnch_anc_visits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_anc_visits s LEFT ANTI JOIN " +
                "target_mnch_anc_visits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND " +
                "s.VisitID <=> t.VisitID AND s.ANCClinicNumber <=> t.ANCClinicNumber");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PatientMnchIDHash", upper(sha2(col("PatientMnchID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        final String columnList = "PatientMnchID,ANCClinicNumber,PatientPk,SiteCode,FacilityName,EMR,Project," +
                "VisitID,VisitDate,ANCVisitNo,GestationWeeks,Height,Weight,Temp,PulseRate,RespiratoryRate," +
                "OxygenSaturation,MUAC,BP,BreastExam,AntenatalExercises,FGM,FGMComplications,Haemoglobin,DiabetesTest," +
                "TBScreening,CACxScreen,CACxScreenMethod,WHOStaging,VLSampleTaken,VLDate,VLResult,SyphilisTreatment," +
                "HIVStatusBeforeANC,HIVTestingDone,HIVTestType,HIVTest1,HIVTest1Result,HIVTest2,HIVTest2Result," +
                "HIVTestFinalResult,SyphilisTestDone,SyphilisTestType,SyphilisTestResults,SyphilisTreated," +
                "MotherProphylaxisGiven,MotherGivenHAART,AZTBabyDispense,NVPBabyDispense,ChronicIllness,CounselledOn," +
                "PartnerHIVTestingANC,PartnerHIVStatusANC,PostParturmFP,Deworming,MalariaProphylaxis,TetanusDose," +
                "IronSupplementsGiven,ReceivedMosquitoNet,PreventiveServices,UrinalysisVariables,ReferredFrom," +
                "ReferredTo,ReferralReasons,NextAppointmentANC,ClinicalNotes,Date_Last_Modified,PatientPKHash,PatientMnchIDHash";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));
        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.MNCH_AncVisits")
                .mode(SaveMode.Append)
                .save();
    }
}
