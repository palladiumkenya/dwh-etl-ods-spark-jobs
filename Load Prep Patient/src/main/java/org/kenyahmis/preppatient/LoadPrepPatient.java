package org.kenyahmis.preppatient;

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

        sourceDf = sourceDf
                .withColumn("DateLastUsedPrev", when(col("DateLastUsedPrev").equalTo(""), null)
                        .otherwise(col("DateLastUsedPrev")))
                .withColumn("PrevPrepReg", when(col("PrevPrepReg").equalTo(""), null)
                        .otherwise(col("PrevPrepReg")))
                .withColumn("ClientPreviouslyonPrep", when(col("ClientPreviouslyonPrep").equalTo(""), null)
                        .otherwise(col("ClientPreviouslyonPrep")))
                .withColumn("DateStartedPrEPattransferringfacility", when(col("DateStartedPrEPattransferringfacility").equalTo(""), null)
                        .otherwise(col("DateStartedPrEPattransferringfacility")))
                .withColumn("TransferFromFacility", when(col("TransferFromFacility").equalTo(""), null)
                        .otherwise(col("TransferFromFacility")))
                .withColumn("TransferInDate", when(col("TransferInDate").equalTo(""), null)
                        .otherwise(col("TransferInDate")))
                .withColumn("Refferedfrom", when(col("Refferedfrom").equalTo(""), null)
                        .otherwise(col("Refferedfrom")))
                .withColumn("PopulationType", when(col("PopulationType").equalTo(""), null)
                        .otherwise(col("PopulationType")))
                .withColumn("ReferralPoint", when(col("ReferralPoint").equalTo(""), null)
                        .otherwise(col("ReferralPoint")))
                .withColumn("ClientType", when(col("ClientType").equalTo(""), null)
                        .otherwise(col("ClientType")))
                .withColumn("Ward", when(col("Ward").equalTo(""), null)
                        .otherwise(col("Ward")))
                .withColumn("LandMark", when(col("LandMark").equalTo(""), null)
                        .otherwise(col("LandMark")))
                .withColumn("SubCounty", when(col("SubCounty").equalTo(""), null)
                        .otherwise(col("SubCounty")))
                .withColumn("CountyofBirth", when(col("CountyofBirth").equalTo(""), null)
                        .otherwise(col("CountyofBirth")))
                .withColumn("Sex", when(col("Sex").equalTo(""), null)
                        .otherwise(col("Sex")))
//                .withColumn("Voided", when(col("Voided").isNull(), lit(false))
//                        .otherwise(col("Voided")))
                .withColumn("KeyPopulationType", when(col("KeyPopulationType").equalTo("160579"), "FSW")
                        .when(col("KeyPopulationType").equalTo("160578"), "MSM")
                        .when(col("KeyPopulationType").equalTo("165084"), "MSW")
                        .when(col("KeyPopulationType").equalTo("105"), "PWID")
                        .otherwise(col("KeyPopulationType")))
                .withColumn("Inschool", when(col("Inschool").equalTo("1"), "Yes")
                        .when(col("Inschool").equalTo("2"), "No")
                        .otherwise(col("Inschool")))
                .withColumn("MaritalStatus", when(col("MaritalStatus").equalTo("Married"), "Married Monogamous")
                        .when(col("MaritalStatus").equalTo("Never married"), "Single")
                        .when(col("MaritalStatus").equalTo("Living with partner"), "Cohabiting")
                        .when(col("MaritalStatus").equalTo("Polygamous"), "Married Polygamous")
                        .when(col("MaritalStatus").equalTo("OTHER NON-CODED"), "Unknown")
                        .when(col("MaritalStatus").equalTo("Separated"), "Divorced")
                        .otherwise(col("MaritalStatus")))
                .withColumn("County", when(col("County").isin("THARAKA - NITHI", "Tharaka-Nithi"), "Tharaka Nithi")
                        .when(col("County").isin("North Alego", "West Sakwa", "Ugunja", "North Ugenya", "Ugenya West", "Ukwala", "West Alego"), "Siaya")
                        .when(col("County").isin("Kabuoch South/Pala", "Gwassi North", "Homa Bay Arunjo", "HOMABAY", "Kendu Bay Town", "Kwabwai", "Homa Bay East"), "Homa Bay")
                        .when(col("County").isin("Kamahuha", "Kambiti", "Nginda", "Muranga"), "Murang'a")
                        .when(col("County").equalTo("KIAMBU''"), "Kiambu")
                        .when(col("County").equalTo("Majoge"), "Kisii")
                        .when(col("County").equalTo("Nangina"), "Busia")
                        .when(col("County").equalTo("Shamata"), "Nyandarua")
                        .when(col("County").equalTo("Kagen"), "NOT DOCUMENTED")
                        .when(col("County").equalTo("..."), "NOT DOCUMENTED")
                        .when(col("County").equalTo(""), null)
                        .otherwise(col("County")));

        logger.info("Loading target prep patient");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.PrEP_Patient")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_prep_patient");
        sourceDf.createOrReplaceTempView("source_prep_patient");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_prep_patient s LEFT ANTI JOIN " +
                "target_prep_patient t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("PrepNumberHash", upper(sha2(col("PrepNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "ID,RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status," +
                "StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber,PrepEnrollmentDate,Sex," +
                "DateofBirth,CountyofBirth,County,SubCounty,Location,LandMark,Ward,ClientType,ReferralPoint," +
                "MaritalStatus,Inschool,PopulationType,KeyPopulationType,Refferedfrom,TransferIn,TransferInDate," +
                "TransferFromFacility,DatefirstinitiatedinPrepCare,DateStartedPrEPattransferringfacility," +
                "ClientPreviouslyonPrep,PrevPrepReg,DateLastUsedPrev,Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.PrEP_Patient")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PrepNumber", "PrepNumberHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.PrEP_Patient", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
