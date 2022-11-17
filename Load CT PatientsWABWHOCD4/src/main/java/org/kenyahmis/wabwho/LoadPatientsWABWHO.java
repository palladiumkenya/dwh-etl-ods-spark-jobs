package org.kenyahmis.wabwho;

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
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

public class LoadPatientsWABWHO {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientsWABWHO.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Patients WAB WHO CD4");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPatientsWABWHOCD4.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientsWABWHO.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load patients WAB WHO CD4 query from file", e);
            return;
        }

        logger.info("Loading source patient WAB WHO CD4");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();


        sourceDf = sourceDf
                .withColumn("bCD4", when(col("bCD4").lt(lit(0)), lit(999)))
                .withColumn("bWHODate", when(col("bWHODate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("bWHODate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("bWHODate")))
                .withColumn("bCD4Date ", when(col("bCD4Date ").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("bCD4Date ").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("bCD4Date ")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target WAB WHO CD4");
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

        sourceDf.createOrReplaceTempView("source_patient_wab_who");
        targetDf.createOrReplaceTempView("target_patient_wab_who");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patient_wab_who t LEFT ANTI JOIN source_patient_wab_who s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");


        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT (PatientID,SiteCode,bCD4,bCD4Date,bWHO,bWHODate,eCD4," +
                "eCD4Date,eWHO,eWHODate,lastWHO,lastWHODate,lastCD4,lastCD4Date,m12CD4,m12CD4Date,m6CD4,m6CD4Date," +
                "PatientPK,Emr,Project FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT (PatientID,SiteCode,bCD4,bCD4Date,bWHO,bWHODate,eCD4," +
                "eCD4Date,eWHO,eWHODate,lastWHO,lastWHODate,lastCD4,lastCD4Date,m12CD4,m12CD4Date,m6CD4,m6CD4Date," +
                "PatientPK,Emr,Project FROM source_patient_wab_who");

        sourceMergeDf2.printSchema();
        unmatchedMergeDf1.printSchema();


        // Will "update" all rows matched, insert new rows and maintain any unmatched rows
        Dataset<Row> finalMergeDf = unmatchedMergeDf1.union(sourceMergeDf2);

        long mergedFinalCount = finalMergeDf.count();
        logger.info("Merged final count: " + mergedFinalCount);

        logger.info("Writing final dataframe to target table");
        // Write to target table
        finalMergeDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("truncate", "true")
                .mode(SaveMode.Overwrite)
                .save();
    }
}
