package org.kenyahmis.htspartnernotificationservices;

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
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());


        logger.info("Loading target hts partner notification services");
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
        targetDf.createOrReplaceTempView("target_partner_notification_services");
        sourceDf.createOrReplaceTempView("source_partner_notification_services");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_partner_notification_services s LEFT ANTI JOIN " +
                "target_partner_notification_services t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND " +
                "s.PartnerPatientPk <=> t.PartnerPatientPk");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        newRecordsJoinDf = session.sql("select ID,FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project," +
                "PartnerPatientPk,KnowledgeOfHivStatus,PartnerPersonID,CccNumber,IpvScreeningOutcome,ScreenedForIpv," +
                "PnsConsent,RelationsipToIndexClient,LinkedToCare,MaritalStatus,PnsApproach,FacilityLinkedTo,Gender," +
                "CurrentlyLivingWithIndexClient,Age,DateElicited,Dob,LinkDateLinkedToCare,PatientPKHash,HtsNumberHash" +
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
