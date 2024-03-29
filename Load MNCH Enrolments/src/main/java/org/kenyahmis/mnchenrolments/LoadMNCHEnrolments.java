package org.kenyahmis.mnchenrolments;

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

public class LoadMNCHEnrolments {
    private static final Logger logger = LoggerFactory.getLogger(LoadMNCHEnrolments.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Enrolments");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMNCHEnrolments.sql";
        String sourceQuery;
        InputStream inputStream = LoadMNCHEnrolments.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch enrolments from file");
        }
        logger.info("Loading source mnch enrolments");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.mnchcentral.url"))
                .option("driver", rtConfig.get("spark.mnchcentral.driver"))
                .option("user", rtConfig.get("spark.mnchcentral.user"))
                .option("password", rtConfig.get("spark.mnchcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.mnchcentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target mnch enrolments");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.MNCH_Enrolments")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_mnch_enrolments");
        sourceDf.createOrReplaceTempView("source_mnch_enrolments");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_enrolments s LEFT ANTI JOIN " +
                "target_mnch_enrolments t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientMnchIDHash", upper(sha2(col("PatientMnchID").cast(DataTypes.StringType), 256)));
        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        final String columnList = "PatientMnchID,PatientPk,SiteCode,FacilityName,EMR,Project,DateExtracted,ServiceType," +
                "EnrollmentDateAtMnch,MnchNumber,FirstVisitAnc,Parity,Gravidae,LMP,EDDFromLMP,HIVStatusBeforeANC," +
                "HIVTestDate,PartnerHIVStatus,PartnerHIVTestDate,BloodGroup,StatusAtMnch," +
                "Date_Last_Modified,RecordUUID";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.MNCH_Enrolments")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientMnchID", "PatientMnchIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.MNCH_Enrolments", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
