package org.kenyahmis.preppatient;

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

public class LoadPrepPatient {
    private static final Logger logger = LoggerFactory.getLogger(LoadPrepPatient.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PrEP Patient");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPrepPatient.sql";
        String sourceQuery;
        InputStream inputStream = LoadPrepPatient.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load prep patient from file");
        }
        logger.info("Loading source prep patient");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.prepcentral.url"))
                .option("driver", rtConfig.get("spark.prepcentral.driver"))
                .option("user", rtConfig.get("spark.prepcentral.user"))
                .option("password", rtConfig.get("spark.prepcentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.prepcentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target prep patient");
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
        targetDf.createOrReplaceTempView("target_prep_patient");
        sourceDf.createOrReplaceTempView("source_prep_patient");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_prep_patient s LEFT ANTI JOIN " +
                "target_prep_patient t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("PrepNumberHash", upper(sha2(col("PrepNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select ID,RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId," +
                "Status,StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber,PrepEnrollmentDate,Sex," +
                "DateofBirth,CountyofBirth,County,SubCounty,Location,LandMark,Ward,ClientType,ReferralPoint," +
                "MaritalStatus,Inschool,PopulationType,KeyPopulationType,Refferedfrom,TransferIn,TransferInDate," +
                "TransferFromFacility,DatefirstinitiatedinPrepCare,DateStartedPrEPattransferringfacility," +
                "ClientPreviouslyonPrep,PrevPrepReg,DateLastUsedPrev,Date_Created" +
                ",Date_Last_Modified," +
                "PatientPKHash,PrepNumberHash" +
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