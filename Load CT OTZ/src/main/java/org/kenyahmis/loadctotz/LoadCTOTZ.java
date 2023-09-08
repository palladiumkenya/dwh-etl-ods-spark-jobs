package org.kenyahmis.loadctotz;

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

public class LoadCTOTZ {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTOTZ.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT OTZ");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTOTZ.sql";
        String query;
        InputStream inputStream = LoadCTOTZ.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct otz query from file", e);
            return;
        }

        logger.info("Loading source ct otz data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDataFrame = sourceDataFrame
                .withColumn("OTZEnrollmentDate", when(col("OTZEnrollmentDate").lt(lit(Date.valueOf(LocalDate.of(2012, 1, 1))))
                        .or(col("OTZEnrollmentDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("OTZEnrollmentDate")))
                .withColumn("TransferInStatus", when(col("TransferInStatus").isin("Yes", "1"), "Yes")
                        .when(col("TransferInStatus").isin("No", "0"), "No")
                        .otherwise(col("TransferInStatus")))
                .withColumn("SupportGroupInvolvement", when(col("SupportGroupInvolvement").isin("Yes", "1"), "Yes")
                        .when(col("SupportGroupInvolvement").isin("No", "0"), "No")
                        .otherwise(col("SupportGroupInvolvement")));

        sourceDataFrame.persist(StorageLevel.DISK_ONLY());
        logger.info("Loading target ct otz data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Otz")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_otz");
        targetDataFrame.createOrReplaceTempView("target_otz");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_otz s LEFT ANTI JOIN target_otz t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,PatientPK,SiteCode,FacilityName,VisitID,VisitDate,Emr,Project," +
                "OTZEnrollmentDate,TransferInStatus,ModulesPreviouslyCovered,ModulesCompletedToday,SupportGroupInvolvement," +
                "Remarks,TransitionAttritionReason,OutcomeDate,Date_Created,Date_Last_Modified";

        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Otz")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_Otz", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
