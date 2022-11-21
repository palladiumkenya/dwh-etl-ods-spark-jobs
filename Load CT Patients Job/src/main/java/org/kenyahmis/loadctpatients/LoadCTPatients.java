package org.kenyahmis.loadctpatients;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceVisitsQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDf.persist(StorageLevel.DISK_ONLY());

        // load lookup tables
        Dataset<Row> maritalStatusLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.maritalStatus"))
                .load();

        Dataset<Row> educationLevelLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.educationLevel"))
                .load();

        Dataset<Row> regimenMapLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.regimenMap"))
                .load();

        Dataset<Row> patientSourceLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.patientSource"))
                .load();

        Dataset<Row> partnerOfferingOVCLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.partnerOfferingOvc"))
                .load();

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
                .join(partnerOfferingOVCLookupDf, sourceDf.col("PartnerOfferingOVCServices").equalTo(partnerOfferingOVCLookupDf.col("Source_PartnerOfferingOVCServices")), "left")
                .withColumn("MaritalStatus", when(maritalStatusLookupDf.col("Target_MaritalStatus").isNotNull(), maritalStatusLookupDf.col("Target_MaritalStatus"))
                        .otherwise(col("MaritalStatus")))
                .withColumn("EducationLevel", when(educationLevelLookupDf.col("TargetEducationLevel").isNotNull(), educationLevelLookupDf.col("TargetEducationLevel"))
                        .otherwise(col("EducationLevel")))
                .withColumn("PreviousARTExposure", when(regimenMapLookupDf.col("Target_Regimen").isNotNull(), regimenMapLookupDf.col("Target_Regimen"))
                        .otherwise(col("PreviousARTExposure")))
                .withColumn("PatientSource", when(patientSourceLookupDf.col("target_name").isNotNull(), patientSourceLookupDf.col("target_name"))
                        .otherwise(col("PatientSource")))
                .withColumn("PartnerOfferingOVCServices", when(partnerOfferingOVCLookupDf.col("Target_PartnerOfferingOVCServices").isNotNull(), partnerOfferingOVCLookupDf.col("Target_PartnerOfferingOVCServices"))
                        .otherwise(col("PartnerOfferingOVCServices")));

        logger.info("Loading target CT patients");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();
        targetDf.persist(StorageLevel.DISK_ONLY());

        sourceDf.createOrReplaceTempView("source_patients");
        targetDf.createOrReplaceTempView("target_patients");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_patients t LEFT ANTI JOIN source_patients s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.Id <=> t.Id");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT Id,PatientID,PatientPK,SiteCode,FacilityName,Gender," +
                "DOB,RegistrationDate,RegistrationAtCCC,RegistrationAtPMTCT,RegistrationAtTBClinic,PatientSource,Region," +
                "District,Village,ContactRelation,LastVisit,MaritalStatus,EducationLevel,DateConfirmedHIVPositive," +
                "PreviousARTExposure,PreviousARTStartDate,Emr,Project,Orphan,Inschool,PatientType,PopulationType," +
                "KeyPopulationType,PatientResidentCounty,PatientResidentSubCounty,PatientResidentLocation," +
                "PatientResidentSubLocation,PatientResidentWard,PatientResidentVillage,TransferInDate,Occupation,NUPI,CKV" +
                " FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT Id,PatientID,PatientPK,SiteCode,FacilityName,Gender," +
                "DOB,RegistrationDate,RegistrationAtCCC,RegistrationAtPMTCT,RegistrationAtTBClinic,PatientSource,Region," +
                "District,Village,ContactRelation,LastVisit,MaritalStatus,EducationLevel,DateConfirmedHIVPositive," +
                "PreviousARTExposure,PreviousARTStartDate,Emr,Project,Orphan,Inschool,PatientType,PopulationType," +
                "KeyPopulationType,PatientResidentCounty,PatientResidentSubCounty,PatientResidentLocation," +
                "PatientResidentSubLocation,PatientResidentWard,PatientResidentVillage,TransferInDate,Occupation,NUPI,CKV" +
                " FROM source_patients");

        sourceMergeDf2.printSchema();
        unmatchedMergeDf1.printSchema();

        // Will "update" all rows matched, insert new rows and maintain any unmatched rows
        Dataset<Row> finalMergeDf = sourceMergeDf2.union(unmatchedMergeDf1);

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
