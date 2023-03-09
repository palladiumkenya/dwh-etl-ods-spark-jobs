package org.kenyahmis.mnchcwcenrolments;

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

public class LoadMnchCwcEnrolments {
    private static final Logger logger = LoggerFactory.getLogger(LoadMnchCwcEnrolments.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Cwc Enrolments");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMnchCwcEnrolments.sql";
        String sourceQuery;
        InputStream inputStream = LoadMnchCwcEnrolments.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load mnch cwc enrolments from file");
        }
        logger.info("Loading source mnch cwc enrolments");
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

        logger.info("Loading target mnch cwc enrolments");
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
        targetDf.createOrReplaceTempView("target_mnch_cwc_enrolments");
        sourceDf.createOrReplaceTempView("source_mnch_cwc_enrolments");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_mnch_cwc_enrolments s LEFT ANTI JOIN " +
                "target_mnch_cwc_enrolments t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("MothersCCCNoHash", upper(sha2(col("MothersCCCNo").cast(DataTypes.StringType), 256)))
                .withColumn("MothersPkvHash", upper(sha2(col("MothersPkv").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");


        newRecordsJoinDf = session.sql("select PatientIDCWC,HEIID,PatientPk,SiteCode,EMR,FacilityName,Project," +
                "DateExtracted,PKV,MothersPkv,RegistrationAtCWC,RegistrationAtHEI,VisitID,Gestation,BirthWeight," +
                "BirthLength,BirthOrder,BirthType,PlaceOfDelivery,ModeOfDelivery,SpecialNeeds,SpecialCare,HEI," +
                "MotherAlive,MothersCCCNo,TransferIn,TransferInDate,TransferredFrom,HEIDate,NVP,BreastFeeding," +
                "ReferredFrom,ARTMother,ARTRegimenMother,ARTStartDateMother,Date_Created,Date_Last_Modified," +
                "PatientPKHash,MothersPkvHash,MothersCCCNoHash" +
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
