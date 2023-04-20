package org.kenyahmis.loadctcovid;

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
import java.sql.Date;
import java.time.LocalDate;

import static org.apache.spark.sql.functions.*;

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

        sourceDataFrame = sourceDataFrame
                .withColumn("Covid19AssessmentDate", when(col("Covid19AssessmentDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("Covid19AssessmentDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("Covid19AssessmentDate")))
                .withColumn("DateGivenFirstDose", when(col("DateGivenFirstDose").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("DateGivenFirstDose").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DateGivenFirstDose")))
                .withColumn("DateGivenSecondDose", when(col("DateGivenSecondDose").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("DateGivenSecondDose").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DateGivenSecondDose")))
                .withColumn("VaccinationStatus", when(col("VaccinationStatus").equalTo("Fully - Details not Available"), "Fully Vaccinated")
                        .when(col("VaccinationStatus").equalTo("Partial"), "Partially Vaccinated")
                        .when(col("VaccinationStatus").equalTo("Partial - Details not Available"), "Partially Vaccinated")
                        .otherwise(col("VaccinationStatus")));

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
        targetDataFrame.createOrReplaceTempView("target_covid");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_covid s LEFT ANTI JOIN target_covid t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newVisitCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newVisitCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        newRecordsJoinDf = session.sql("select PatientPK, PatientID, Emr, Project, SiteCode, FacilityName," +
                " VisitID, Covid19AssessmentDate, ReceivedCOVID19Vaccine, DateGivenFirstDose, FirstDoseVaccineAdministered," +
                " DateGivenSecondDose, SecondDoseVaccineAdministered, VaccinationStatus, VaccineVerification, BoosterGiven," +
                " BoosterDose, BoosterDoseDate, EverCOVID19Positive, COVID19TestDate, PatientStatus, AdmissionStatus," +
                " AdmissionUnit, MissedAppointmentDueToCOVID19, COVID19PositiveSinceLasVisit," +
                " COVID19TestDateSinceLastVisit, PatientStatusSinceLastVisit, AdmissionStatusSinceLastVisit," +
                " AdmissionStartDate, AdmissionEndDate, AdmissionUnitSinceLastVisit, SupplementalOxygenReceived," +
                " PatientVentilated, TracingFinalOutcome, CauseOfDeath, BoosterDoseVerified," +
                " Sequence, COVID19TestResult,PatientPKHash,PatientIDHash from new_records");


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
