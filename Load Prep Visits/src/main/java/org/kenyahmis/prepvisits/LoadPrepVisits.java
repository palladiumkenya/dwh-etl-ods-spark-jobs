package org.kenyahmis.prepvisits;

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

public class LoadPrepVisits {

    private static final Logger logger = LoggerFactory.getLogger(LoadPrepVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PrEP Visits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPrepVisits.sql";
        String sourceQuery;
        InputStream inputStream = LoadPrepVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load prep visits from file");
        }
        logger.info("Loading source prep visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.prepcentral.url"))
                .option("driver", rtConfig.get("spark.prepcentral.driver"))
                .option("user", rtConfig.get("spark.prepcentral.user"))
                .option("password", rtConfig.get("spark.prepcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.prepcentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target prep visits");
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
        targetDf.createOrReplaceTempView("target_prep_visits");
        sourceDf.createOrReplaceTempView("source_prep_visits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_prep_visits s LEFT ANTI JOIN " +
                "target_prep_visits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND t.VisitID <=> s.VisitID");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PrepNumberHash", upper(sha2(col("PrepNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status,StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber,\n" +
                "VisitDate,VisitID,BloodPressure,Temperature,Weight,Height,BMI,STIScreening,STISymptoms,STITreated,Circumcised,VMMCReferral,LMP,MenopausalStatus,\n" +
                "PregnantAtThisVisit,EDD,PlanningToGetPregnant,PregnancyPlanned,PregnancyEnded,PregnancyEndDate,PregnancyOutcome,BirthDefects,Breastfeeding,FamilyPlanningStatus,\n" +
                "FPMethods,AdherenceDone,AdherenceOutcome,AdherenceReasons,SymptomsAcuteHIV,ContraindicationsPrep,PrepTreatmentPlan,PrepPrescribed,RegimenPrescribed,MonthsPrescribed,\n" +
                "CondomsIssued,Tobegivennextappointment,Reasonfornotgivingnextappointment,HepatitisBPositiveResult,HepatitisCPositiveResult,VaccinationForHepBStarted,TreatedForHepB,\n" +
                "VaccinationForHepCStarted,TreatedForHepC,NextAppointment,ClinicalNotes,Date_Created,Date_Last_Modified," +
                "PatientPKHash,PrepNumberHash" +
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
