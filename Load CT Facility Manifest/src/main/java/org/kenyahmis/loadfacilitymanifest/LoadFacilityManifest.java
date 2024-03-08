package org.kenyahmis.loadfacilitymanifest;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFacilityManifest {
    private static final Logger logger = LoggerFactory.getLogger(LoadFacilityManifest.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Facility Manifest");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadFacilityManifest.sql";
        String query;
        InputStream inputStream = LoadFacilityManifest.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct ovc query from file", e);
            return;
        }

        logger.info("Loading source ct facility manifest data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDataFrame.persist(StorageLevel.DISK_ONLY());

//        sourceDataFrame.printSchema();
        logger.info("Loading target ct facility manifest data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_FacilityManifest")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_manifest");
        targetDataFrame.createOrReplaceTempView("target_manifest");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_manifest s LEFT ANTI JOIN target_manifest t ON s.ID <=> t.ID");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,Voided,Processed,SiteCode,PatientCount,DateRecieved,Name,EmrName,EmrSetup,UploadMode,Start,End,Tag";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_FacilityManifest")
                .mode(SaveMode.Append)
                .save();
    }
}
