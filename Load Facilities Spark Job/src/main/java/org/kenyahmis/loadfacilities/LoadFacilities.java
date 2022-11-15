package org.kenyahmis.loadfacilities;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadFacilities {
    private static final Logger logger = LoggerFactory.getLogger(LoadFacilities.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load facilities");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadSites.sql";
        String query;
        InputStream inputStream = LoadFacilities.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }

        logger.info("Loading source facilities data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDataFrame.persist(StorageLevel.MEMORY_ONLY());
        logger.info("Loading target facilities data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_facilities");
        targetDataFrame.createOrReplaceTempView("target_facilities");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_facilities t LEFT ANTI JOIN source_facilities s ON s.MFL_Code <=> t.MFL_Code");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> mergeDf1 = session.sql("select MFL_Code, \"Facility Name\", County, SubCounty, Owner, Latitude, Longitude, SDP, EMR, \"EMR Status\", \"HTS Use\", \"HTS Deployment\", \"HTS Status\", \"IL Status\", BOOLEAN(\"Registration IE\"), BOOLEAN(\"Pharmacy IE\"), mlab, Ushauri, Nishauri, OVC, OTZ, PrEP, 3PM, AIR, KP, MCH, TB,\"Lab Manifest\" from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select MFL_Code, \"Facility Name\", County, SubCounty, Owner, Latitude, Longitude, SDP, EMR, \"EMR Status\", \"HTS Use\", \"HTS Deployment\", \"HTS Status\", \"IL Status\", BOOLEAN(\"Registration IE\"), BOOLEAN(\"Pharmacy IE\"), STRING(mlab), STRING(Ushauri), STRING(Nishauri), STRING(OVC), STRING(OTZ), STRING(PrEP), STRING(3PM), STRING(AIR), STRING(KP), STRING(MCH), STRING(TB), STRING(\"Lab Manifest\") from source_facilities");

        mergeDf2.printSchema();
        mergeDf1.printSchema();

        // Union all records together
        Dataset<Row> dfMergeFinal = mergeDf1.unionAll(mergeDf2);
        long mergedFinalCount = dfMergeFinal.count();
        logger.info("Merged final count: " + mergedFinalCount);

        dfMergeFinal
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
