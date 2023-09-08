package org.kenyahmis.loadctpatients;

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
import static org.apache.spark.sql.functions.col;

public class LoadCTPatients {
    private static final Logger logger = LoggerFactory.getLogger(LoadCTPatients.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Patients");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceCTPatients.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadCTPatients.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load source CT patients query from file", e);
            return;
        }

        logger.info("Loading source CT patients");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        sourceDf.persist(StorageLevel.DISK_ONLY());

        // load lookup tables
        Dataset<Row> maritalStatusLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_MaritalStatus")
                .option("query", "select Source_MaritalStatus, Target_MaritalStatus from dbo.lkp_MaritalStatus")
                .load();

        Dataset<Row> educationLevelLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.Lkp_EducationLevel")
                .option("query", "select SourceEducationLevel,TargetEducationLevel from dbo.Lkp_EducationLevel")
                .load();

        Dataset<Row> regimenMapLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_RegimenLineMap")
                .option("query", "select Source_Regimen,Target_Regimen from dbo.lkp_RegimenLineMap")
                .load();

        Dataset<Row> patientSourceLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_patient_source")
                .option("query", "select source_name,target_name from dbo.lkp_patient_source")
                .load();

//        Dataset<Row> partnerOfferingOVCLookupDf = session.read()
//                .format("jdbc")
//                .option("url", rtConfig.get("spark.ods.url"))
//                .option("driver", rtConfig.get("spark.ods.driver"))
//                .option("user", rtConfig.get("spark.ods.user"))
//                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_PartnerOfferingOVCServices")
//                .load();

        sourceDf = sourceDf
                .withColumn("DOB", when(col("DOB").lt(lit(Date.valueOf(LocalDate.of(1910, 1, 1))))
                        .or(col("DOB").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DOB")))
                .withColumn("RegistrationDate", when(col("RegistrationDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("RegistrationDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("RegistrationDate")))
                .withColumn("RegistrationAtCCC", when(col("RegistrationAtCCC").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("RegistrationAtCCC").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("RegistrationAtCCC")))
                .withColumn("RegistrationAtPMTCT", when(col("RegistrationAtPMTCT").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("RegistrationAtPMTCT").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("RegistrationAtPMTCT")))
                .withColumn("RegistrationAtTBClinic", when(col("RegistrationAtTBClinic").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("RegistrationAtTBClinic").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("RegistrationAtTBClinic")))
                .withColumn("PreviousARTStartDate", when(col("PreviousARTStartDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("PreviousARTStartDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("PreviousARTStartDate")))
                .withColumn("PreviousARTStartDate", when(col("PreviousARTStartDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("PreviousARTStartDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("PreviousARTStartDate")))
                .withColumn("LastVisit", when(col("LastVisit").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("LastVisit").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("LastVisit")))
                .withColumn("DateConfirmedHIVPositive", when(col("DateConfirmedHIVPositive").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("DateConfirmedHIVPositive").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("DateConfirmedHIVPositive")))
                .withColumn("TransferInDate", when(col("TransferInDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("TransferInDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("TransferInDate")));

        // Set values from look up tables
        sourceDf = sourceDf
                .join(maritalStatusLookupDf, sourceDf.col("MaritalStatus").equalTo(maritalStatusLookupDf.col("Source_MaritalStatus")), "left")
                .join(educationLevelLookupDf, sourceDf.col("EducationLevel").equalTo(educationLevelLookupDf.col("SourceEducationLevel")), "left")
                .join(regimenMapLookupDf, sourceDf.col("PreviousARTExposure").equalTo(regimenMapLookupDf.col("Source_Regimen")), "left")
                .join(patientSourceLookupDf, sourceDf.col("PatientSource").equalTo(patientSourceLookupDf.col("source_name")), "left")
//                .join(partnerOfferingOVCLookupDf, sourceDf.col("PartnerOfferingOVCServices").equalTo(partnerOfferingOVCLookupDf.col("Source_PartnerOfferingOVCServices")), "left")
                .withColumn("MaritalStatus", when(maritalStatusLookupDf.col("Target_MaritalStatus").isNotNull(), maritalStatusLookupDf.col("Target_MaritalStatus"))
                        .otherwise(col("MaritalStatus")))
                .withColumn("EducationLevel", when(educationLevelLookupDf.col("TargetEducationLevel").isNotNull(), educationLevelLookupDf.col("TargetEducationLevel"))
                        .otherwise(col("EducationLevel")))
                .withColumn("PreviousARTExposure", when(regimenMapLookupDf.col("Target_Regimen").isNotNull(), regimenMapLookupDf.col("Target_Regimen"))
                        .otherwise(col("PreviousARTExposure")))
                .withColumn("PatientSource", when(patientSourceLookupDf.col("target_name").isNotNull(), patientSourceLookupDf.col("target_name"))
                        .otherwise(col("PatientSource")));
//                .withColumn("PartnerOfferingOVCServices", when(partnerOfferingOVCLookupDf.col("Target_PartnerOfferingOVCServices").isNotNull(), partnerOfferingOVCLookupDf.col("Target_PartnerOfferingOVCServices"))
//                        .otherwise(col("PartnerOfferingOVCServices")));

        logger.info("Loading target CT patients");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Patient")
                .load();
        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patients");
        targetDf.createOrReplaceTempView("target_patients");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_patients s LEFT ANTI JOIN target_patients t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
//                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)))
//                .withColumn("NupiHash", upper(sha2(col("NUPI").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        final String sourceColumnList = "ID,PatientID,PatientPK,SiteCode,FacilityName,Gender,DOB,RegistrationDate," +
                "RegistrationAtCCC,RegistrationAtPMTCT,RegistrationAtTBClinic,PatientSource,Region,District," +
                "Village,ContactRelation,LastVisit,MaritalStatus,EducationLevel,DateConfirmedHIVPositive," +
                "PreviousARTExposure,PreviousARTStartDate,Emr,Project,Orphan,Inschool,PatientType,PopulationType," +
                "KeyPopulationType,PatientResidentCounty,PatientResidentSubCounty,PatientResidentLocation," +
                "PatientResidentSubLocation,PatientResidentWard,PatientResidentVillage,TransferInDate,Occupation," +
                "NUPI,Pkv,Date_Created,Date_Last_Modified";

        newRecordsJoinDf = session.sql(String.format("SELECT %s FROM new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_Patient")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");
        hashColumns.put("NUPI", "NupiHash");

        try {
            dbUtils.hashPIIColumns("dbo.CT_Patient", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
