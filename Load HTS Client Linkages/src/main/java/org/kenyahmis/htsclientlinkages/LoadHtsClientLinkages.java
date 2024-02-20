package org.kenyahmis.htsclientlinkages;

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

public class LoadHtsClientLinkages {
    private static final Logger LOGGER = LoggerFactory.getLogger(LoadHtsClientLinkages.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Client Linkages");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsClientLinkages.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsClientLinkages.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            LOGGER.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            LOGGER.error("Failed to load hts client linkages query from file", e);
            return;
        }
        LOGGER.info("Loading hts client linkages");
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

        // Clean source data
        sourceDf = sourceDf
                .withColumn("ReferralDate", when(col("ReferralDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1)))), null)
                        .otherwise(col("ReferralDate")))
                .withColumn("DateEnrolled", when(col("DateEnrolled").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1)))), null)
                        .otherwise(col("DateEnrolled")))
                .withColumn("DatePrefferedToBeEnrolled", when(col("DatePrefferedToBeEnrolled").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1)))), null)
                        .otherwise(col("DatePrefferedToBeEnrolled")));

        LOGGER.info("Loading target hts client linkages");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_ClientLinkages")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_client_linkages");
        sourceDf.createOrReplaceTempView("source_client_linkages");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_client_linkages s LEFT ANTI JOIN " +
                "target_client_linkages t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        LOGGER.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String columnList = "FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,EnrolledFacilityName,ReferralDate," +
                "DateEnrolled,DatePrefferedToBeEnrolled,FacilityReferredTo,HandedOverTo,HandedOverToCadre,ReportedCCCNumber";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));
        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_ClientLinkages")
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
            dbUtils.hashPIIColumns("dbo.HTS_ClientLinkages", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
