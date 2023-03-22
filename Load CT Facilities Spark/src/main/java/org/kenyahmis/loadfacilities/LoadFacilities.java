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

        final String odsTable = "dbo.ALL_EMRSites";
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
                .option("url", rtConfig.get("spark.his.url"))
                .option("driver", rtConfig.get("spark.his.driver"))
                .option("user", rtConfig.get("spark.his.user"))
                .option("password", rtConfig.get("spark.his.password"))
                .option("query", query)
                .load();

        sourceDataFrame.persist(StorageLevel.MEMORY_ONLY());
        logger.info("Loading target facilities data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", odsTable)
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_facilities");
        targetDataFrame.createOrReplaceTempView("target_facilities");

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_facilities s LEFT ANTI JOIN target_facilities t ON s.MFL_Code <=> t.MFL_Code");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select MFL_Code,Facility_Name,County,SubCounty,Owner,Latitude," +
                "Longitude,SDP,SDP_Agency,Implementation,EMR,EMR_Status,HTS_Use,HTS_Deployment,HTS_Status," +
                "IL_Status,Registration_IE,Phamarmacy_IE,mlab,Ushauri,Nishauri,Appointment_Management_IE,OVC," +
                "OTZ,PrEP,`_3PM`,AIR,KP,MCH,TB,Lab_Manifest,Comments,Project from new_records");
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable",odsTable)
                .mode(SaveMode.Append)
                .save();
    }
}
