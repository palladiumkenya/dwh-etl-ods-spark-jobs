package org.kenyahmis.partnertracing;

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

public class LoadHtsPartnerTracing {
    private static final Logger logger = LoggerFactory.getLogger(LoadHtsPartnerTracing.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Partner Tracing");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsPartnerTracing.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsPartnerTracing.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load hts partner tracing query from file");
        }
        logger.info("Loading hts partner tracing");
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

        //Clean source data
        sourceDf = sourceDf.withColumn("TraceOutcome", when(col("TraceOutcome").isin("null", "NULL"), null)
                .otherwise(col("TraceOutcome")));

        logger.info("Loading target hts partner tracing");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_PartnerTracings")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_partner_tracing");
        sourceDf.createOrReplaceTempView("source_partner_tracing");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_partner_tracing s LEFT ANTI JOIN " +
                "target_partner_tracing t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND " +
                "s.HtsNumber <=> t.HtsNumber");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        String columnList = "FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,TraceType,TraceDate,TraceOutcome," +
                "BookingDate";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_PartnerTracings")
                .mode(SaveMode.Append)
                .save();
    }
}
