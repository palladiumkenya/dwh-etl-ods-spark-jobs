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

import static org.apache.spark.sql.functions.*;

public class LoadCTContactListing {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTContactListing.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Contact Listing");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

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

        sourceDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame = sourceDataFrame
                .withColumn("ContactAge", when(col("ContactAge").lt(lit(0))
                        .or(col("ContactAge").gt(lit(120))), lit(999)))
                .withColumn("ContactSex", when(col("ContactSex").equalTo("U"), "Undefined")
                        .when(col("ContactSex").equalTo("M"), "Male")
                        .when(col("ContactSex").equalTo("F"), "Female")
                        .otherwise(col("ContactSex")))
                .withColumn("RelationshipWithPatient", when(col("RelationshipWithPatient").isin("Daughter", "Son"), "Child")
                        .when(col("RelationshipWithPatient").equalTo("Co-wife"), "Sexual Partner")
                        .when(col("RelationshipWithPatient").equalTo("Select"), "OTHER")
                        .when(col("RelationshipWithPatient").isin("undefined", "None"), "Undefined")
                        .when(col("RelationshipWithPatient").equalTo("Nice"), "Niece")
                        .otherwise(col("RelationshipWithPatient")))
                .withColumn("IPVScreeningOutcome", when(col("IPVScreeningOutcome").equalTo("0"), "False")
                        .when(col("IPVScreeningOutcome").equalTo("No"), "False")
                        .when(col("IPVScreeningOutcome").equalTo("Yes"), "True")
                        .when(col("IPVScreeningOutcome").isin("1065", "1066"), "OTHER")
                        .otherwise(col("IPVScreeningOutcome")))
                .withColumn("KnowledgeOfHivStatus", when(col("KnowledgeOfHivStatus").isin("Negative", "Yes", "Positive", "Exposed Infant", "Exposed", "664", "703"), "Yes")
                        .when(col("KnowledgeOfHivStatus").isin("No", "Unknown", "1067", "0"), "No")
                        .otherwise(col("KnowledgeOfHivStatus")));

        logger.info("Loading target ct contact listing data frame");
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

        targetDataFrame.createOrReplaceTempView("target_listing");
        sourceDataFrame.createOrReplaceTempView("source_listing");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_listing t LEFT ANTI JOIN source_listing s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.AdverseEventsUnique_ID <=> t.AdverseEventsUnique_ID");

        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");


        Dataset<Row> mergeDf1 = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, Emr, Project, PartnerPersonID, ContactAge, ContactSex, ContactMaritalStatus, RelationshipWithPatient, ScreenedForIpv, IpvScreening, IPVScreeningOutcome, CurrentlyLivingWithIndexClient, KnowledgeOfHivStatus, PnsApproach, DateImported, CKV, ContactPatientPK, DateCreated from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, Emr, Project, PartnerPersonID, ContactAge, ContactSex, ContactMaritalStatus, RelationshipWithPatient, ScreenedForIpv, IpvScreening, IPVScreeningOutcome, CurrentlyLivingWithIndexClient, KnowledgeOfHivStatus, PnsApproach, DateImported, CKV, ContactPatientPK, DateCreated from source_listing");

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
