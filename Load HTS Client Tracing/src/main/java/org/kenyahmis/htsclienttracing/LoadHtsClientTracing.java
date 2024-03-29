package org.kenyahmis.htsclienttracing;

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
import static org.apache.spark.sql.functions.col;

public class LoadHtsClientTracing {
    private static final Logger logger = LoggerFactory.getLogger(LoadHtsClientTracing.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Client Tracing");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsClientTracing.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsClientTracing.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load hts client tracing query from file", e);
            return;
        }
        logger.info("Loading hts client tracing");
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


        logger.info("Loading target hts client tracing");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_ClientTracing")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_client_tracing");
        sourceDf.createOrReplaceTempView("source_client_tracing");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_client_tracing s LEFT ANTI JOIN " +
                "target_client_tracing t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode" +
                " AND s.HtsNumber <=> t.HtsNumber AND s.TracingOutcome <=> t.TracingOutcome");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String columnList = "FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,TracingType,TracingDate,TracingOutcome,RecordUUID";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_ClientTracing")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("HtsNumber", "HtsNumberHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.HTS_ClientTracing", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
