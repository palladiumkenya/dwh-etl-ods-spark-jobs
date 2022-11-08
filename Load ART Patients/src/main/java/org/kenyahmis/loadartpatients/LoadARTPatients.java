package org.kenyahmis.loadartpatients;

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

import static org.apache.spark.sql.functions.lit;

public class LoadARTPatients {
    private static final Logger logger = LoggerFactory.getLogger(LoadARTPatients.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load ART Patients");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceARTPatients.sql";
        final String targetQueryFileName = "LoadTargetARTPatients.sql";
        String sourceQuery;
        String targetQuery;
        InputStream inputStream = LoadARTPatients.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source ART patients query from file", e);
            return;
        }

        InputStream targetQueryInputStream = LoadARTPatients.class.getClassLoader().getResourceAsStream(targetQueryFileName);
        if (targetQueryInputStream == null) {
            logger.error(targetQueryFileName + " not found");
            return;
        }
        try {
            targetQuery = IOUtils.toString(targetQueryInputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load target ART patients query from file", e);
            return;
        }
        logger.info("Loading source ART Patients");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDf = sourceDf.withColumn("PreviousARTRegimen_Orig", lit(null))
                .withColumn("StartRegimen_Orig", lit(null))
                .withColumn("LastRegimen_Orig", lit(null))
                .withColumn("DateImported", lit(null).cast(DataTypes.DateType));

        sourceDf.printSchema();
        sourceDf.persist(StorageLevel.MEMORY_AND_DISK());

        logger.info("Loading target ART Patients");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", "(" + targetQuery + ") pvt")
                .load();

        targetDf.persist(StorageLevel.MEMORY_AND_DISK());
        targetDf.printSchema();

        // Find rows in target table unmatched in source table
        Dataset<Row> unmatchedDf = targetDf.except(sourceDf);

        // Will "update" all rows matched, insert new rows and maintain any unmatched rows
        Dataset<Row> finalMergeDf = sourceDf.unionAll(unmatchedDf);
        logger.info("Writing final dataframe to target table");
        // Write to target table
        finalMergeDf
                .repartition(10)
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
