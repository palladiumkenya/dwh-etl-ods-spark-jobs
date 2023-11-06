package org.kenyahmis.loadcervicalscreening;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
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

public class LoadCervicalScreening {
    private static final Logger logger = LoggerFactory.getLogger(LoadCervicalScreening.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Cervical Screening");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceCervicalScreening.sql";
        String sourceQuery;
        InputStream inputStream = LoadCervicalScreening.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load Cervical screening query from file", e);
            return;
        }

        logger.info("Loading Cervical Screening");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target Cervical Screening");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_CervicalCancerScreening")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_screening");
        targetDf.createOrReplaceTempView("target_screening");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_screening s LEFT ANTI JOIN target_screening t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK and s.visitID <=> t.visitID");

        long newCervicalScreeningCount = newRecordsJoinDf.count();
        logger.info("New cervical screening count is {} ",newCervicalScreeningCount);

        final String sourceColumnList = "SiteCode,PatientPK,PatientID,Emr,Project,Voided,Processed,Id,FacilityName," +
                "VisitID,VisitDate,VisitType,ScreeningMethod,TreatmentToday,ReferredOut,NextAppointmentDate,ScreeningType," +
                "ScreeningResult,PostTreatmentComplicationCause,OtherPostTreatmentComplication,ReferralReason,Created," +
                "Date_Created,Date_Last_Modified";

        newRecordsJoinDf.createOrReplaceTempView("new_records");
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_CervicalCancerScreening")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("CT_CervicalCancerScreening", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
