package org.kenyahmis.loadctpatientlabs;

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

import static org.apache.spark.sql.functions.lit;

public class LoadPatientLabs {
    private static final Logger logger = LoggerFactory.getLogger(LoadPatientLabs.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Patient Labs");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceCTPatientLabs.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientLabs.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source CT patient labs query from file", e);
            return;
        }

        logger.info("Loading source CT patient labs");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();


        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target CT patient labs");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_patient_labs");
        targetDf.createOrReplaceTempView("target_patient_labs");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patient_labs t LEFT ANTI JOIN source_patient_labs s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,PatientPK,SiteCode,VisitId,OrderedByDate,ReportedByDate," +
                "       TestName,EnrollmentTest,TestResult,Emr,Project,DateImported,Reason," +
                "       Created,CKV,DateSampleTaken,SampleType from final_unmatched");
        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,PatientPK,SiteCode,VisitId,OrderedByDate,ReportedByDate," +
                "       TestName,EnrollmentTest,TestResult,Emr,Project,DateImported,Reason," +
                "       Created,CKV,DateSampleTaken,SampleType from source_patient_labs");

        sourceMergeDf2.printSchema();
        unmatchedMergeDf1.printSchema();

        // Will "update" all rows matched, insert new rows and maintain any unmatched rows
        Dataset<Row> finalMergeDf = unmatchedMergeDf1.union(sourceMergeDf2);
        long mergedFinalCount = finalMergeDf.count();
        logger.info("Merged final count: " + mergedFinalCount);
        logger.info("Writing final dataframe to target table");
        // Write to target table
        finalMergeDf
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
