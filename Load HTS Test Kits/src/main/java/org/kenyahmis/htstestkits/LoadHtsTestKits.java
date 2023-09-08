package org.kenyahmis.htstestkits;

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

public class LoadHtsTestKits {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadHtsTestKits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Test Kits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsTestKits.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsTestKits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load hts test kits query from file");
        }
        LOGGER.info("Loading hts test kits");
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

        // Clean source data TODO: Sort out lot numbers
//        final String regexOne = "^(0[1-9]|[12][0-9]|3[01])\\/(0[1-9]|1[0-2])\\/[0-9]{4}\\s(0[1-9]|1[0-2]):[0-5][0-9]:[0-5][0-9]\\s(AM|PM)$";
//        final String regexTwo = "^(0[1-9]|[12][0-9]|3[01])\\/(0[1-9]|1[0-2])\\/[0-9]{4}\\s(?:[01][0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$";
        sourceDf = sourceDf
                .withColumn("TestResult2", when(col("TestResult2").equalTo("N/A"), null)
                        .otherwise(col("TestResult2")))
                .withColumn("TestKitName2", when(col("TestKitName2").equalTo(""), null)
                        .otherwise(col("TestKitName2")))
                .withColumn("TestKitName1", when(col("TestKitName1").equalTo(""), null)
                        .otherwise(col("TestKitName1")));
//                .withColumn("TestKitExpiry1",
//                        when(col("TestKitExpiry1").rlike(regexOne), to_date(col("TestKitExpiry1"), "dd/MM/yyyy hh:mm:ss a"))
//                                .when(col("TestKitExpiry1").rlike(regexTwo), to_date(col("TestKitExpiry1"), "dd/MM/yyyy HH:mm:ss"))
//                                .otherwise(col("TestKitExpiry1")))
//                .withColumn("TestKitExpiry2",
//                        when(col("TestKitExpiry2").rlike(regexOne), to_date(col("TestKitExpiry2"), "dd/MM/yyyy hh:mm:ss a"))
//                                .when(col("TestKitExpiry2").rlike(regexTwo), to_date(col("TestKitExpiry2"), "dd/MM/yyyy HH:mm:ss"))
//                                .otherwise(col("TestKitExpiry2")));


        LOGGER.info("Loading target hts test kits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_TestKits")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_hts_test_kits");
        sourceDf.createOrReplaceTempView("source_hts_test_kits");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_hts_test_kits s LEFT ANTI JOIN " +
                "target_hts_test_kits t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        LOGGER.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,EncounterId,TestKitName1," +
                "TestKitLotNumber1,TestKitExpiry1,TestResult1,TestKitName2,TestKitLotNumber2,TestKitExpiry2,TestResult2";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_TestKits")
                .mode(SaveMode.Append)
                .save();

    }

}
