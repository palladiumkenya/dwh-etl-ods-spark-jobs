package org.kenyahmis.patientbaselines;

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
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", sourcePatientsQuery)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
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
        sourceDf.printSchema();

        logger.info("Loading target patient baselines");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.CT_PatientBaselines")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.printSchema();

        sourceDf.createOrReplaceTempView("source_patient_baselines");
        targetDf.createOrReplaceTempView("target_patient_baselines");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patient_baselines s LEFT ANTI JOIN" +
                " target_patient_baselines t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,PatientPK,SiteCode,bCD4,bCD4Date,bWHO,bWHODate,eCD4,eCD4Date," +
                "eWHO,eWHODate,lastWHO,lastWHODate,lastCD4,lastCD4Date,m12CD4,m12CD4Date,m6CD4,m6CD4Date,Emr,Project," +
                "bWAB,bWABDate,eWAB,eWABDate,lastWAB,lastWABDate,Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_PatientBaselines")
                .mode(SaveMode.Append)
                .save();
        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_PatientBaselines", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
