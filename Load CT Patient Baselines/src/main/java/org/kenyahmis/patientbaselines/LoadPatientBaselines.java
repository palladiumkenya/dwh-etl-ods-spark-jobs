package org.kenyahmis.patientbaselines;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class LoadPatientBaselines {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientBaselines.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Patients Baselines");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPatientBaselines.sql";
        String sourcePatientsQuery;
        InputStream inputStream = LoadPatientBaselines.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            throw new RuntimeException(sourceQueryFileName + " not found");
        }
        try {
            sourcePatientsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load patient baselines query from file");
        }

        logger.info("Loading source patient baselines");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", sourcePatientsQuery)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();


        sourceDf = sourceDf
                .withColumn("bCD4", when(col("bCD4").lt(lit(0)), lit(999))
                        .otherwise(col("bCD4")))
                .withColumn("bWHODate", when(col("bWHODate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("bWHODate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("bWHODate")))
                .withColumn("bCD4Date", when(col("bCD4Date").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("bCD4Date").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("bCD4Date")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target patient baselines");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patient_baselines");
        targetDf.createOrReplaceTempView("target_patient_baselines");

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patient_baselines s LEFT ANTI JOIN" +
                " target_patient_baselines t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("SELECT PatientID,SiteCode,bCD4,bCD4Date,bWHO,bWHODate,eCD4," +
                "eCD4Date,eWHO,eWHODate,lastWHO,lastWHODate,lastCD4,lastCD4Date,m12CD4,m12CD4Date,m6CD4,m6CD4Date," +
                "PatientPK,Emr,Project FROM new_records");

        // Write to target table
        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.source.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
