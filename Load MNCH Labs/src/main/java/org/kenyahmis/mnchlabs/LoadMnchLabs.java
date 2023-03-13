package org.kenyahmis.mnchlabs;

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

public class LoadMnchLabs {
    private static final Logger logger = LoggerFactory.getLogger(LoadMnchLabs.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Labs");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMnchLabs.sql";
        String sourceQuery;
        InputStream inputStream = LoadMnchLabs.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch labs from file");
        }
        logger.info("Loading source mnch labs");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target mnch labs");
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
        targetDf.createOrReplaceTempView("target_mnch_labs");
        sourceDf.createOrReplaceTempView("source_mnch_labs");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_labs s LEFT ANTI JOIN " +
                "target_mnch_labs t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND " +
                "t.VisitID <=> s.VisitID AND t.TestName <=> s.TestName AND t.TestResult <=> s.TestResult");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PatientMnchIDHash", upper(sha2(col("PatientMNCH_ID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        newRecordsJoinDf = session.sql("select PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status,StatusDate," +
                "DateExtracted,PatientMNCH_ID,FacilityName,SatelliteName,VisitID,OrderedbyDate,ReportedbyDate,TestName," +
                "TestResult,LabReason,Date_Last_Modified," +
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
