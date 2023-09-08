package org.kenyahmis.enhancedadherencecounselling;

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
import static org.apache.spark.sql.functions.col;

public class LoadEnhancedAdherenceCounselling {
    private static final Logger logger = LoggerFactory.getLogger(LoadEnhancedAdherenceCounselling.class);
    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load Enhanced Adherence Counselling");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadEnhancedAdherenceCounselling.sql";
        String sourceQuery;
        InputStream inputStream = LoadEnhancedAdherenceCounselling.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load Enhanced Adherence Counselling query from file", e);
            return;
        }

        logger.info("Loading source Enhanced Adherence Counselling");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDf = sourceDf
                .withColumn("DateOfFirstSession", when(col("DateOfFirstSession").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("DateOfFirstSession").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DateOfFirstSession")))
                .withColumn("EACFollowupDate", when(col("EACFollowupDate").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("EACFollowupDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("EACFollowupDate")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target Enhanced Adherence Counselling");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_EnhancedAdherenceCounselling")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_enhanced_adherence_counselling");
        targetDf.createOrReplaceTempView("target_enhanced_adherence_counselling");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_enhanced_adherence_counselling s LEFT ANTI JOIN target_enhanced_adherence_counselling t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=>t.VisitID");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,PatientPK,SiteCode,FacilityName,VisitID,VisitDate,Emr,Project," +
                "SessionNumber,DateOfFirstSession,PillCountAdherence,MMAS4_1,MMAS4_2,MMAS4_3,MMAS4_4,MMSA8_1,MMSA8_2," +
                "MMSA8_3,MMSA8_4,MMSAScore,EACRecievedVL,EACVL,EACVLConcerns,EACVLThoughts,EACWayForward," +
                "EACCognitiveBarrier,EACBehaviouralBarrier_1,EACBehaviouralBarrier_2,EACBehaviouralBarrier_3," +
                "EACBehaviouralBarrier_4,EACBehaviouralBarrier_5,EACEmotionalBarriers_1,EACEmotionalBarriers_2," +
                "EACEconBarrier_1,EACEconBarrier_2,EACEconBarrier_3,EACEconBarrier_4,EACEconBarrier_5,EACEconBarrier_6," +
                "EACEconBarrier_7,EACEconBarrier_8,EACReviewImprovement,EACReviewMissedDoses,EACReviewStrategy," +
                "EACReferral,EACReferralApp,EACReferralExperience,EACHomevisit,EACAdherencePlan,EACFollowupDate," +
                "Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_EnhancedAdherenceCounselling")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_EnhancedAdherenceCounselling", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
