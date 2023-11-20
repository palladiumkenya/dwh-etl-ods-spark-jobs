package org.kenyahmis.mcnhpncvisits;

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
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

import static org.apache.spark.sql.functions.*;

public class LoadMnchPncVisits {
    private static final Logger logger = LoggerFactory.getLogger(LoadMnchPncVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Pnc Visits");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMnchPncVisits.sql";
        String sourceQuery;
        InputStream inputStream = LoadMnchPncVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch pnc visits from file");
        }
        logger.info("Loading source mnch pnc visits");
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

        logger.info("Loading target mnch pnc visits from file");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.MNCH_PncVisits")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_mnch_pnc_visits");
        sourceDf.createOrReplaceTempView("source_mnch_pnc_visits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_pnc_visits s LEFT ANTI JOIN " +
                "target_mnch_pnc_visits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientMnchIDHash", upper(sha2(col("PatientMnchID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "PatientMnchID,PatientPk,PNCRegisterNumber,SiteCode,EMR,FacilityName,Project," +
                "DateExtracted,VisitID,VisitDate,PNCVisitNo,DeliveryDate,ModeOfDelivery,PlaceOfDelivery,Height,Weight," +
                "Temp,PulseRate,RespiratoryRate,OxygenSaturation,MUAC,BP,BreastExam,GeneralCondition,HasPallor,Pallor," +
                "Breast,PPH,CSScar,UterusInvolution,Episiotomy,Lochia,Fistula,MaternalComplications,TBScreening," +
                "ClientScreenedCACx,CACxScreenMethod,CACxScreenResults,PriorHIVStatus,HIVTestingDone,HIVTest1," +
                "HIVTest1Result,HIVTest2,HIVTest2Result,HIVTestFinalResult,InfantProphylaxisGiven,MotherProphylaxisGiven," +
                "CoupleCounselled,PartnerHIVTestingPNC,PartnerHIVResultPNC,CounselledOnFP,ReceivedFP,HaematinicsGiven," +
                "DeliveryOutcome,BabyConditon,BabyFeeding,UmbilicalCord,Immunization,InfantFeeding,PreventiveServices," +
                "ReferredFrom,ReferredTo,NextAppointmentPNC,ClinicalNotes,Date_Last_Modified,InfactCameForHAART," +
                "MotherCameForHIVTest,MotherGivenHAART,VisitTimingBaby,VisitTimingMother";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.MNCH_PncVisits")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientMnchID", "PatientMnchIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.MNCH_PncVisits", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
