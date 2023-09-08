package org.kenyahmis.loadctipt;

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
import static org.apache.spark.sql.functions.sha2;

public class LoadCTIPT {

    private static final Logger logger = LoggerFactory.getLogger(LoadCTIPT.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT IPT");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTIPT.sql";
        String query;
        InputStream inputStream = LoadCTIPT.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct ipt query from file", e);
            return;
        }

        logger.info("Loading source ct ipt data frame");
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

        // clean source data
        sourceDataFrame = sourceDataFrame
                .withColumn("TBScreening", when(col("TBScreening").equalTo("1"), "Screened")
                        .when(col("TBScreening").isin("TB Screening not done", "0"), "Not Screened")
                        .otherwise(col("TBScreening")))
                .withColumn("IndicationForIPT", when(col("IndicationForIPT").isin("Adherence Issues", "Poor adherence"), "Adherence Issues")
                        .when(col("IndicationForIPT").equalTo("Client Traced back a"), "Client Traced back")
                        .when(col("IndicationForIPT").isin("No more drug Interru", "Toxicity Resolved", "Other patient decisi", "Pregnancy", "Patient declined", "Other", "High CD4", "Education", "Client Discharged fr"), "OTHER")
                        .otherwise(col("IndicationForIPT")));

        logger.info("Loading target ct ipt data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Ipt")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_ipt");
        targetDataFrame.createOrReplaceTempView("target_ipt");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_ipt s LEFT ANTI JOIN target_ipt t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,PatientPK,SiteCode,FacilityName,VisitID,VisitDate,Emr,Project," +
                "OnTBDrugs,OnIPT,EverOnIPT,Cough,Fever,NoticeableWeightLoss,NightSweats,Lethargy,ICFActionTaken," +
                "TestResult,TBClinicalDiagnosis,ContactsInvited,EvaluatedForIPT,StartAntiTBs,TBRxStartDate,TBScreening," +
                "IPTClientWorkUp,StartIPT,IndicationForIPT,Date_Created,Date_Last_Modified";

        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Ipt")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_Ipt", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
