package org.kenyahmis.loadctgbvscreening;

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

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class LoadCTGBVScreening {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTGBVScreening.class);
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT GBV Screening");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTGBVScreening.sql";
        String query;
        InputStream inputStream = LoadCTGBVScreening.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load CT gbv query from file", e);
            return;
        }

        logger.info("Loading source ct gbv data frame");
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
        logger.info("Loading target ct gbv data frame");
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

        sourceDataFrame.createOrReplaceTempView("source_gbv_screening");
        targetDataFrame.createOrReplaceTempView("target_gbv_screening");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_gbv_screening s LEFT ANTI JOIN target_gbv_screening t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND t.SiteCode = s.SiteCode AND s.VisitID <=> t.VisitID");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newRecordCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select PatientID, PatientPK, SiteCode, FacilityName, VisitID, VisitDate," +
                " Emr, Project, IPV, PhysicalIPV, EmotionalIPV, SexualIPV, IPVRelationship, DateImported," +
                "PatientPKHash,PatientIDHash from new_records");
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
