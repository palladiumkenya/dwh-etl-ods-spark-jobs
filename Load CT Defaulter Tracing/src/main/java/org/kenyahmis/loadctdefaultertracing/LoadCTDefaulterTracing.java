package org.kenyahmis.loadctdefaultertracing;

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

public class LoadCTDefaulterTracing {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTDefaulterTracing.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Defaulter Tracing");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTDefaulterTracing.sql";
        String query;
        InputStream inputStream = LoadCTDefaulterTracing.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct defaulter tracing query from file", e);
            return;
        }

        logger.info("Loading source ct defaulter tracing data frame");
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
        logger.info("Loading target ct defaulter tracing data frame");

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

        sourceDataFrame.createOrReplaceTempView("source_defaulter_tracing");
        targetDataFrame.createOrReplaceTempView("target_defaulter_tracing");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_defaulter_tracing t LEFT ANTI JOIN source_defaulter_tracing s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> mergeDf1 = session.sql("select PatientPK, PatientID, Emr, Project, SiteCode, FacilityName, VisitID, VisitDate, EncounterId, TracingType, TracingOutcome, AttemptNumber, IsFinalTrace, TrueStatus, CauseOfDeath, Comments, BookingDate, CKV, DateImported from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select PatientPK, PatientID, Emr, Project, SiteCode, FacilityName, VisitID, VisitDate, EncounterId, TracingType, TracingOutcome, AttemptNumber, IsFinalTrace, TrueStatus, CauseOfDeath, Comments, BookingDate, CKV, DateImported from source_defaulter_tracing");

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
