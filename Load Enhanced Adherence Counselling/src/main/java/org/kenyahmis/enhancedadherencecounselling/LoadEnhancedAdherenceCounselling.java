package org.kenyahmis.enhancedadherencecounselling;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
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
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_enhanced_adherence_counselling");
        targetDf.createOrReplaceTempView("target_enhanced_adherence_counselling");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_enhanced_adherence_counselling t LEFT ANTI JOIN source_enhanced_adherence_counselling s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=>t.VisitID AND s.EnhancedAdherenceCounsellingUnique_ID <=> t.EnhancedAdherenceCounsellingUnique_ID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,PatientPK,SiteCode,FacilityName,VisitID," +
                "VisitDate,Emr,Project,SessionNumber,DateOfFirstSession,PillCountAdherence,MMAS4_1,MMAS4_2,MMAS4_3," +
                "MMAS4_4,MMSA8_1,MMSA8_2,MMSA8_3,MMSA8_4,MMSAScore,EACRecievedVL,EACVL,EACVLConcerns,EACVLThoughts," +
                "EACWayForward,EACCognitiveBarrier,EACBehaviouralBarrier_1,EACBehaviouralBarrier_2," +
                "EACBehaviouralBarrier_3,EACBehaviouralBarrier_4,EACBehaviouralBarrier_5,EACEmotionalBarriers_1," +
                "EACEmotionalBarriers_2,EACEconBarrier_1,EACEconBarrier_2,EACEconBarrier_3,EACEconBarrier_4," +
                "EACEconBarrier_5,EACEconBarrier_6,EACEconBarrier_7,EACEconBarrier_8,EACReviewImprovement," +
                "EACReviewMissedDoses,EACReviewStrategy,EACReferral,EACReferralApp,EACReferralExperience,EACHomevisit," +
                "EACAdherencePlan,EACFollowupDate,DateImported,CKV,PatientUnique_ID,EnhancedAdherenceCounsellingUnique_ID" +
                " FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,PatientPK,SiteCode,FacilityName,VisitID," +
                "VisitDate,Emr,Project,SessionNumber,DateOfFirstSession,PillCountAdherence,MMAS4_1,MMAS4_2,MMAS4_3," +
                "MMAS4_4,MMSA8_1,MMSA8_2,MMSA8_3,MMSA8_4,MMSAScore,EACRecievedVL,EACVL,EACVLConcerns,EACVLThoughts," +
                "EACWayForward,EACCognitiveBarrier,EACBehaviouralBarrier_1,EACBehaviouralBarrier_2," +
                "EACBehaviouralBarrier_3,EACBehaviouralBarrier_4,EACBehaviouralBarrier_5,EACEmotionalBarriers_1," +
                "EACEmotionalBarriers_2,EACEconBarrier_1,EACEconBarrier_2,EACEconBarrier_3,EACEconBarrier_4," +
                "EACEconBarrier_5,EACEconBarrier_6,EACEconBarrier_7,EACEconBarrier_8,EACReviewImprovement," +
                "EACReviewMissedDoses,EACReviewStrategy,EACReferral,EACReferralApp,EACReferralExperience,EACHomevisit," +
                "EACAdherencePlan,EACFollowupDate,DateImported,CKV,PatientUnique_ID,EnhancedAdherenceCounsellingUnique_ID" +
                " FROM source_enhanced_adherence_counselling");

        sourceMergeDf2.printSchema();
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
