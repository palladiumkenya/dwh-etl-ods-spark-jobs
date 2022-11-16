package org.kenyahmis.loadctcovid;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

public class LoadCTCovid {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTCovid.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Covid");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String queryFileName = "LoadCTCovid.sql";
        String query;
        InputStream inputStream = LoadCTCovid.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load ct covid query from file", e);
            return;
        }

        logger.info("Loading source ct covid data frame");
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
        logger.info("Loading target ct covid data frame");
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

        sourceDataFrame.createOrReplaceTempView("source_covid");
        sourceDataFrame.createOrReplaceTempView("target_covid");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_covid t LEFT ANTI JOIN source_covid s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");


        Dataset<Row> mergeDf1 = session.sql("select PatientPK, PatientID, Emr, Project, SiteCode, FacilityName, VisitID, Covid19AssessmentDate, ReceivedCOVID19Vaccine, DateGivenFirstDose, FirstDoseVaccineAdministered, DateGivenSecondDose, SecondDoseVaccineAdministered, VaccinationStatus, VaccineVerification, BoosterGiven, BoosterDose, BoosterDoseDate, EverCOVID19Positive, COVID19TestDate, PatientStatus, AdmissionStatus, AdmissionUnit, MissedAppointmentDueToCOVID19, COVID19PositiveSinceLasVisit, COVID19TestDateSinceLastVisit, PatientStatusSinceLastVisit, AdmissionStatusSinceLastVisit, AdmissionStartDate, AdmissionEndDate, AdmissionUnitSinceLastVisit, SupplementalOxygenReceived, PatientVentilated, TracingFinalOutcome, CauseOfDeath, CKV, DateImported, BoosterDoseVerified, Sequence, COVID19TestResult from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select PatientPK, PatientID, Emr, Project, SiteCode, FacilityName, VisitID, Covid19AssessmentDate, ReceivedCOVID19Vaccine, DateGivenFirstDose, FirstDoseVaccineAdministered, DateGivenSecondDose, SecondDoseVaccineAdministered, VaccinationStatus, VaccineVerification, BoosterGiven, BoosterDose, BoosterDoseDate, EverCOVID19Positive, COVID19TestDate, PatientStatus, AdmissionStatus, AdmissionUnit, MissedAppointmentDueToCOVID19, COVID19PositiveSinceLasVisit, COVID19TestDateSinceLastVisit, PatientStatusSinceLastVisit, AdmissionStatusSinceLastVisit, AdmissionStartDate, AdmissionEndDate, AdmissionUnitSinceLastVisit, SupplementalOxygenReceived, PatientVentilated, TracingFinalOutcome, CauseOfDeath, CKV, DateImported, BoosterDoseVerified, Sequence, COVID19TestResult from source_covid");

        mergeDf2.printSchema();
        mergeDf1.printSchema();
        // Union all records together
        Dataset<Row> dfMergeFinal = mergeDf1.union(mergeDf2);
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
