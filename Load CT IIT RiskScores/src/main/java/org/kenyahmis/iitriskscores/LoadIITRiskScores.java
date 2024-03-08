package org.kenyahmis.iitriskscores;

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

public class LoadIITRiskScores {
    private static final Logger logger = LoggerFactory.getLogger(LoadIITRiskScores.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT IIT Risk Scores");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadIITRiskScores.sql";
        String query;
        InputStream inputStream = LoadIITRiskScores.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct iit risk scores query from file", e);
            return;
        }

        logger.info("Loading source ct iit risk scores data frame");
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

        logger.info("Loading target ct iit risk scores data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_IITRiskScores")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_iit");
        targetDataFrame.createOrReplaceTempView("target_iit");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_iit s LEFT ANTI JOIN target_iit t ON s.ID <=> t.ID");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "SiteCode,PatientID,PatientPK,Emr,Project,Voided,Processed,Id,FacilityName," +
                "SourceSysUUID,RiskScore,RiskFactors,RiskDescription,RiskEvaluationDate," +
                "Created,Date_Created,Date_Last_Modified, current_date() as LoadDate";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records",sourceColumnList));

        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_IITRiskScores")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_IITRiskScores", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
