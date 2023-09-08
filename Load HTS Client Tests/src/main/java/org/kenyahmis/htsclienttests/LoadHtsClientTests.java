package org.kenyahmis.htsclienttests;

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

public class LoadHtsClientTests {
    private static final Logger logger = LoggerFactory.getLogger(LoadHtsClientTests.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Client Tests");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsClientTests.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsClientTests.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load hts client tests query from file", e);
            return;
        }
        logger.info("Loading hts client tests");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.htscentral.url"))
                .option("driver", rtConfig.get("spark.htscentral.driver"))
                .option("user", rtConfig.get("spark.htscentral.user"))
                .option("password", rtConfig.get("spark.htscentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.htscentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        // load lookup tables
        Dataset<Row> patientSourceLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_patient_source")
                .option("query", "select source_name,target_name from dbo.lkp_patient_source")
                .load();

        Dataset<Row> htsStrategyLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_htsStrategy")
                .option("query", "select Source_htsStrategy, Target_htsStrategy from dbo.lkp_htsStrategy")
                .load();

        // Clean source data
        sourceDf = sourceDf.
                withColumn("ClientTestedAs", when(col("ClientTestedAs").isin("C: Couple (includes polygamous)", "Couple"), "Couple")
                        .when(col("ClientTestedAs").isin("I: Individual", "Individual"), "Individual")
                        .otherwise(null))
                .withColumn("TbScreening", when(col("TbScreening").isin("No Signs", "No TB", "No TB signs", "Yes"), "No Signs")
                        .when(col("TbScreening").isin("On TB Treatment", "INH", "TB Rx", "TBRx"), "On TB Treatment")
                        .when(col("TbScreening").isin("Presumed TB", "PrTB"), "Presumed TB")
                        .when(col("TbScreening").equalTo("TB Confirmed"), "TB Confirmed")
                        .otherwise("Not Done"))
                .withColumn("ClientSelfTested", when(col("ClientSelfTested").isin("1", "Yes"), "Yes")
                        .when(col("ClientSelfTested").isin("0", "No"), "No")
                        .when(col("ClientSelfTested").equalTo("NA"), "NA")
                        .otherwise(null))
                .withColumn("CoupleDiscordant", when(col("CoupleDiscordant").equalTo("Yes"), "Yes")
                        .when(col("CoupleDiscordant").equalTo("No"), "No")
                        .when(col("CoupleDiscordant").isin("NA", ""), null)
                        .otherwise(null))
                .withColumn("TestType", when(col("TestType").isin("Initial", "Initial Test"), "Initial Test")
                        .when(col("TestType").isin("Repeat", "Repeat Test"), "Repeat Test")
                        .when(col("TestType").equalTo("Retest"), "Retest")
                        .otherwise(null))
                .withColumn("Consent", when(col("Consent").equalTo("No"), "No")
                        .when(col("Consent").equalTo("Yes"), "Yes")
                        .when(col("Consent").isin("NULL", ""), null)
                        .otherwise(null))
                .withColumn("Setting", when(col("Setting").isin("Facility", "Tent"), "Facility")
                        .when(col("Setting").isin("Community", "Medical Camp"), "Community")
                        .otherwise(null))
                .withColumn("Approach", when(col("Approach").isin("CITC", "Client Initiated Testing (CITC)"), "Client Initiated Testing (CITC)")
                        .when(col("Approach").isin("PITC", "Provider Initiated Testing(PITC)"), "Provider Initiated Testing(PITC)")
                        .otherwise(null))
                .withColumn("MonthsSinceLastTest", when(col("MonthsSinceLastTest").gt(1540), null)
                .otherwise(col("MonthsSinceLastTest")));

        // Set values from look up tables
        sourceDf = sourceDf
                .join(patientSourceLookupDf, sourceDf.col("EntryPoint").equalTo(patientSourceLookupDf.col("source_name")), "left")
                .join(htsStrategyLookupDf, sourceDf.col("TestStrategy").equalTo(htsStrategyLookupDf.col("Source_htsStrategy")), "left")
                .withColumn("EntryPoint",
                        when(patientSourceLookupDf.col("target_name").isNotNull(), patientSourceLookupDf.col("target_name"))
                                .otherwise(col("EntryPoint")))
                .withColumn("TestStrategy",
                        when(htsStrategyLookupDf.col("Target_htsStrategy").isNotNull(), htsStrategyLookupDf.col("Target_htsStrategy"))
                                .otherwise(col("TestStrategy")));

        logger.info("Loading target hts client tests");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_ClientTests")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_client_tests");
        sourceDf.createOrReplaceTempView("source_client_tests");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_client_tests s LEFT ANTI JOIN " +
                "target_client_tests t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode AND s.EncounterId <=>t.EncounterId");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk")
                .cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: {}", newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String columnList = "FacilityName,SiteCode,PatientPk,Emr,Project,EncounterId,TestDate,EverTestedForHiv," +
                "MonthsSinceLastTest,ClientTestedAs,EntryPoint,TestStrategy,TestResult1,TestResult2,FinalTestResult," +
                "PatientGivenResult,TbScreening,ClientSelfTested,CoupleDiscordant,TestType,Consent,Setting,Approach," +
                "HtsRiskCategory,HtsRiskScore,OtherReferredServices,ReferredForServices,ReferredServices";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));
        newRecordsJoinDf.printSchema();

        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_ClientTests")
                .mode(SaveMode.Append)
                .save();
    }
}
