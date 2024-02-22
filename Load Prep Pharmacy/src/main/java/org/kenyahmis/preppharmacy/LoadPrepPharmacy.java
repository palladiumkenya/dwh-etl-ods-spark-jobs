package org.kenyahmis.preppharmacy;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
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

public class LoadPrepPharmacy {
    private static final Logger logger = LoggerFactory.getLogger(LoadPrepPharmacy.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load PrEP Pharmacy");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPrepPharmacy.sql";
        String sourceQuery;
        InputStream inputStream = LoadPrepPharmacy.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load prep pharmacy from file");
        }
        logger.info("Loading source prep pharmacy");
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
                .withColumn("Duration", when(col("Duration").gt(lit(12)), null)
                        .otherwise(col("Duration")))
                .withColumn("DispenseDate",
                        when(col("DispenseDate").equalTo("")
                                .or(col("DispenseDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))), null)
                                .otherwise(col("DispenseDate")))
                .withColumn("RegimenPrescribed", when(col("RegimenPrescribed").equalTo(""), null)
                        .otherwise(col("RegimenPrescribed")));

        logger.info("Loading target prep pharmacy");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.PrEP_Pharmacy")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_prep_pharmacy");
        sourceDf.createOrReplaceTempView("source_prep_pharmacy");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_prep_pharmacy s LEFT ANTI JOIN " +
                "target_prep_pharmacy t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode AND t.VisitID <=> s.VisitID");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("PrepNumberHash", upper(sha2(col("PrepNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "ID,RefId,Created,PatientPk,SiteCode,Emr,Project,Processed,QueueId,Status," +
                "StatusDate,DateExtracted,FacilityId,FacilityName,PrepNumber,HtsNumber," +
                "VisitID,RegimenPrescribed,DispenseDate,Duration,Date_Created,Date_Last_Modified,RecordUUID";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.PrEP_Pharmacy")
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
            dbUtils.hashPIIColumns("dbo.PrEP_Pharmacy", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
