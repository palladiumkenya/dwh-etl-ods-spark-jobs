package org.kenyahmis.loadctipt;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
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
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_ipt");
        targetDataFrame.createOrReplaceTempView("target_ipt");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_ipt s LEFT ANTI JOIN target_ipt t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, VisitID, VisitDate, Emr, Project, OnTBDrugs, OnIPT, EverOnIPT, Cough, Fever, NoticeableWeightLoss, NightSweats, Lethargy, ICFActionTaken, TestResult, TBClinicalDiagnosis, ContactsInvited, EvaluatedForIPT, StartAntiTBs, TBRxStartDate, TBScreening, IPTClientWorkUp, StartIPT, IndicationForIPT, DateImported from new_records");
        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
