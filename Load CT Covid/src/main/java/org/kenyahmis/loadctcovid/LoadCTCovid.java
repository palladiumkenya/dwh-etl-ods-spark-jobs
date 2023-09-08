package org.kenyahmis.loadctcovid;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.kenyahmis.core.DatabaseUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Properties;

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
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("query", query)
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
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
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Covid")
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.DISK_ONLY());

        sourceDataFrame.createOrReplaceTempView("source_covid");
        targetDataFrame.createOrReplaceTempView("target_covid");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_covid s LEFT ANTI JOIN target_covid t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.VisitID <=> t.VisitID");

//        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        long newVisitCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newVisitCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientPK,PatientID,Emr,Project,SiteCode,FacilityName,VisitID,Covid19AssessmentDate," +
                "ReceivedCOVID19Vaccine,DateGivenFirstDose,FirstDoseVaccineAdministered,DateGivenSecondDose," +
                "SecondDoseVaccineAdministered,VaccinationStatus,VaccineVerification,BoosterGiven,BoosterDose," +
                "BoosterDoseDate,EverCOVID19Positive,COVID19TestDate,PatientStatus,AdmissionStatus,AdmissionUnit," +
                "MissedAppointmentDueToCOVID19,COVID19PositiveSinceLasVisit,COVID19TestDateSinceLastVisit," +
                "PatientStatusSinceLastVisit,AdmissionStatusSinceLastVisit,AdmissionStartDate,AdmissionEndDate," +
                "AdmissionUnitSinceLastVisit,SupplementalOxygenReceived,PatientVentilated,TracingFinalOutcome," +
                "CauseOfDeath,BoosterDoseVerified,Sequence,COVID19TestResult,Date_Created,Date_Last_Modified";
        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        newRecordsJoinDf
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Covid")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("CT_Covid", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
