package org.kenyahmis.loadctcontactlisting;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.kenyahmis.core.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Properties;

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
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame = sourceDataFrame
                .withColumn("ContactAge", when(col("ContactAge").lt(lit(0))
                        .or(col("ContactAge").gt(lit(120))), lit(999))
                        .otherwise(col("ContactAge")))
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
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_ContactListing")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        targetDataFrame.createOrReplaceTempView("target_listing");
        sourceDataFrame.createOrReplaceTempView("source_listing");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_listing s LEFT ANTI JOIN target_listing t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)))
//                .withColumn("ContactPatientPKHash", upper(sha2(col("ContactPatientPK").cast(DataTypes.StringType), 256)));

        long newVisitCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newVisitCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String columnList = "ID,PatientID,PatientPK,SiteCode,FacilityName,Emr,Project,PartnerPersonID,ContactAge," +
                "ContactSex,ContactMaritalStatus,RelationshipWithPatient,ScreenedForIpv,IpvScreening," +
                "IPVScreeningOutcome,CurrentlyLivingWithIndexClient,KnowledgeOfHivStatus," +
                "PnsApproach,ContactPatientPK,DateCreated,Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", columnList));

        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_ContactListing")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");
        hashColumns.put("ContactPatientPK", "ContactPatientPKHash");

        try {
            dbUtils.hashPIIColumns("CT_ContactListing", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
