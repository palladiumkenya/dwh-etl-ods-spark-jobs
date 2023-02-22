package org.kenyahmis.loadctallergies;

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

public class LoadCTAllergies {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTAllergies.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Allergies");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTAllergies.sql";
        String query;
        InputStream inputStream = LoadCTAllergies.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct allergies query from file", e);
            return;
        }

        logger.info("Loading source ct allergies data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();
        sourceDataFrame.persist(StorageLevel.DISK_ONLY());

        Dataset<Row> lookupChronicIllnessDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.chronicIllness"))
                .load();
        Dataset<Row> lookupAllergyCausativeAgentDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.allergyCausativeAgent"))
                .load();
        Dataset<Row> lookupAllergicReactionDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.allergicReaction"))
                .load();

        // Clean source values
        sourceDataFrame = sourceDataFrame
                .withColumn("ChronicOnsetDate", when(col("ChronicOnsetDate").lt(lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .or(col("ChronicOnsetDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("ChronicOnsetDate")))
                .withColumn("AllergySeverity", when(col("AllergySeverity").equalTo("Fatal"), "Fatal")
                        .when(col("AllergySeverity").isin("Mild|Mild|Mild", "Mild|Mild", "Mild"), "Mild")
                        .when(col("AllergySeverity").isin("Moderate|Moderate", "Moderate"), "Moderate")
                        .when(col("AllergySeverity").equalTo("Severe"), "Severe")
                        .when(col("AllergySeverity").isin("Unknown", "Moderate|Mild"), "Unknown")
                        .otherwise(col("AllergySeverity")));

        // Set values from lookup tables
        sourceDataFrame = sourceDataFrame
                .join(lookupChronicIllnessDf, sourceDataFrame.col("ChronicIllness")
                        .equalTo(lookupChronicIllnessDf.col("source_name")), "left")
                .join(lookupAllergyCausativeAgentDf, sourceDataFrame.col("AllergyCausativeAgent")
                        .equalTo(lookupAllergyCausativeAgentDf.col("source_name")), "left")
                .join(lookupAllergicReactionDf, sourceDataFrame.col("AllergicReaction")
                        .equalTo(lookupAllergicReactionDf.col("source_name")), "left")
                .withColumn("ChronicIllness", when(lookupChronicIllnessDf.col("target_name").isNotNull(), lookupChronicIllnessDf.col("target_name"))
                        .otherwise(col("ChronicIllness")))
                .withColumn("AllergyCausativeAgent", when(lookupAllergyCausativeAgentDf.col("target_name").isNotNull(), lookupAllergyCausativeAgentDf.col("target_name"))
                        .otherwise(col("AllergyCausativeAgent")))
                .withColumn("AllergicReaction", when(lookupAllergicReactionDf.col("target_name").isNotNull(), lookupAllergicReactionDf.col("target_name"))
                        .otherwise(col("AllergicReaction")));
        logger.info("Loading target ct allergies data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_allergies");
        targetDataFrame.createOrReplaceTempView("target_allergies");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_allergies s LEFT ANTI JOIN target_allergies t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");


        long newVisitCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newVisitCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, VisitID, VisitDate, Emr, Project, ChronicIllness, ChronicOnsetDate, knownAllergies, AllergyCausativeAgent, AllergicReaction, AllergySeverity, AllergyOnsetDate, Skin, Eyes, ENT, Chest, CVS, Abdomen, CNS, Genitourinary, DateImported from new_records");
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
