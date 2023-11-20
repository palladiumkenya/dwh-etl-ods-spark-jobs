package org.kenyahmis.htspartnernotificationservices;

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

public class LoadPartnerNotificationServices {
    private static final Logger logger = LoggerFactory.getLogger(LoadPartnerNotificationServices.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Partner Notification Services");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPartnerNotificationServices.sql";
        String sourceQuery;
        InputStream inputStream = LoadPartnerNotificationServices.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load hts partner notification services query from file", e);
            return;
        }
        logger.info("Loading hts partner notification services");
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
        sourceDf = sourceDf
                .withColumn("FacilityLinkedTo", when(col("FacilityLinkedTo").equalTo(""), null)
                        .otherwise(col("FacilityLinkedTo")))
                .withColumn("PnsApproach", when(col("PnsApproach").equalTo("Pr: Provider Referral").or(col("PnsApproach").equalTo("D: Dual Referral")), "Provider Referral")
                        .when(col("PnsApproach").equalTo("Cr: Passive Referral"), "Passive Referral")
                        .otherwise(col("PnsApproach")))
                .withColumn("LinkedToCare", when(col("LinkedToCare").equalTo("Y"), "Yes")
                        .when(col("LinkedToCare").equalTo("N"), "No")
                        .otherwise(col("LinkedToCare")))
                .withColumn("PnsConsent", when(col("PnsConsent").equalTo("0"), "No")
                        .otherwise(col("PnsConsent")))
                .withColumn("ScreenedForIpv", when(col("ScreenedForIpv").equalTo("N/A"), null)
                        .otherwise(col("ScreenedForIpv")))
                .withColumn("CccNumber", when(col("CccNumber").equalTo(""), null)
                        .otherwise(col("CccNumber")))
                .withColumn("Age", when(col("Age").lt(0).or(col("Age").gt(100)), null)
                        .otherwise(col("Age")));

        logger.info("Loading target hts partner notification services");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_PartnerNotificationServices")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_partner_notification_services");
        sourceDf.createOrReplaceTempView("source_partner_notification_services");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_partner_notification_services s LEFT ANTI JOIN " +
                "target_partner_notification_services t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND " +
                "s.PartnerPatientPk <=> t.PartnerPatientPk");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));
        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String sourceColumnList = "ID,FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,PartnerPatientPk," +
                "KnowledgeOfHivStatus,PartnerPersonID,CccNumber,IpvScreeningOutcome,ScreenedForIpv,PnsConsent," +
                "RelationsipToIndexClient,LinkedToCare,MaritalStatus,PnsApproach,FacilityLinkedTo,Gender," +
                "CurrentlyLivingWithIndexClient,Age,DateElicited,Dob,LinkDateLinkedToCare";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", sourceColumnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_PartnerNotificationServices")
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
            dbUtils.hashPIIColumns("dbo.HTS_PartnerNotificationServices", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
