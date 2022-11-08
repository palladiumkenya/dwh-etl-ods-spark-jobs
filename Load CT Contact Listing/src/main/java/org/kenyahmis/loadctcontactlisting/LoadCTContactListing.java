package org.kenyahmis.loadctcontactlisting;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Arrays;

import static org.apache.spark.sql.functions.col;

public class LoadCTContactListing {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTContactListing.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Contact Listing");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();
        final int targetPartitions = 10;

        final String queryFileName = "LoadCTContactListing.sql";
        String query;
        InputStream inputStream = LoadCTContactListing.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct contact listing query from file", e);
            return;
        }

        logger.info("Loading source ct contact listing data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDataFrame.persist(StorageLevel.MEMORY_AND_DISK());
        sourceDataFrame.printSchema();
        logger.info("Loading target ct ipt data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_AND_DISK());

        // source comparison data frame
        Dataset<Row> sourceComparisonDf = sourceDataFrame.select(col("PatientID"), col("PatientPK"),
                col("SiteCode"), col("ContactPatientPK"));

        // target comparison data frame
        Dataset<Row> targetComparisonDf = targetDataFrame.select(col("PatientID"), col("PatientPK"),
                col("SiteCode"), col("ContactPatientPK"));

        // Records in target data frame and not in source data frame
        Dataset<Row> unmatchedFacilities = targetComparisonDf.except(sourceComparisonDf)
                .withColumnRenamed("PatientID", "UN_PatientID")
                .withColumnRenamed("PatientPK", "UN_PatientPK")
                .withColumnRenamed("SiteCode", "UN_SiteCode")
                .withColumnRenamed("ContactPatientPK", "UN_ContactPatientPK");

        Dataset<Row> finalUnmatchedDf = unmatchedFacilities.join(targetDataFrame,
                targetComparisonDf.col("PatientID").equalTo(unmatchedFacilities.col("UN_PatientID")).and(
                        targetComparisonDf.col("PatientPK").equalTo(unmatchedFacilities.col("UN_PatientPK"))
                ).and(
                        targetComparisonDf.col("SiteCode").equalTo(unmatchedFacilities.col("UN_SiteCode"))
                ).and(
                        targetComparisonDf.col("ContactPatientPK").equalTo(unmatchedFacilities.col("UN_ContactPatientPK"))
                ), "inner");

        finalUnmatchedDf.createOrReplaceTempView("final_unmatched");
        sourceDataFrame.createOrReplaceTempView("source_dataframe");

        String sourceColumns = Arrays.toString(sourceDataFrame.columns());
        logger.info("Source columns: " + sourceColumns);

        Dataset<Row> mergeDf1 = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, Emr, Project, PartnerPersonID, ContactAge, ContactSex, ContactMaritalStatus, RelationshipWithPatient, ScreenedForIpv, IpvScreening, IPVScreeningOutcome, CurrentlyLivingWithIndexClient, KnowledgeOfHivStatus, PnsApproach, DateImported, CKV, ContactPatientPK, DateCreated from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, Emr, Project, PartnerPersonID, ContactAge, ContactSex, ContactMaritalStatus, RelationshipWithPatient, ScreenedForIpv, IpvScreening, IPVScreeningOutcome, CurrentlyLivingWithIndexClient, KnowledgeOfHivStatus, PnsApproach, DateImported, CKV, ContactPatientPK, DateCreated from source_dataframe");

        // Union all records together
        Dataset<Row> dfMergeFinal = mergeDf1.unionAll(mergeDf2);
        dfMergeFinal.printSchema();
        dfMergeFinal
                .repartition(targetPartitions)
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
