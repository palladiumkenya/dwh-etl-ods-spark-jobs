package org.kenyahmis.loadctotz;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
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
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_otz");
        targetDataFrame.createOrReplaceTempView("target_otz");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_otz s LEFT ANTI JOIN target_otz t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select PatientID,PatientPK,SiteCode,FacilityName,VisitID,VisitDate,Emr,Project,OTZEnrollmentDate,TransferInStatus,ModulesPreviouslyCovered,ModulesCompletedToday,SupportGroupInvolvement,Remarks,TransitionAttritionReason,OutcomeDate,DateImported from new_records");
        newRecordsJoinDf
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
