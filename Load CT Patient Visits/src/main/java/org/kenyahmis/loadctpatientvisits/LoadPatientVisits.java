package org.kenyahmis.loadctpatientvisits;

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

public class LoadPatientVisits {

    private static final Logger logger = LoggerFactory.getLogger(LoadPatientVisits.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load CT Patient Visits");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadPatientVisits.sql";
        String sourceVisitsQuery;
        InputStream inputStream = LoadPatientVisits.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceVisitsQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load visits query from file", e);
            return;
        }
        logger.info("Loading source Visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", sourceVisitsQuery)
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        // load lookup tables
        Dataset<Row> familyPlanningDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.familyPlanning"))
                .load();
        Dataset<Row> pwpDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.pwp"))
                .load();

        // Clean source data
        sourceDf = sourceDf.withColumn("OIDATE", when((col("OIDATE").lt(lit(Date.valueOf(LocalDate.of(2000, 1, 1))).cast(DataTypes.DateType)))
                .or(col("OIDATE").gt(lit(Date.valueOf(LocalDate.now())).cast(DataTypes.DateType))), lit(Date.valueOf(LocalDate.of(1900, 1, 1)))))
                .withColumn("Weight", when((col("Weight").lt(lit(0)))
                        .or(col("Weight").gt(lit(200))), lit(999).cast(DataTypes.StringType))
                        .when(col("Weight").equalTo(""), null)
                        .otherwise(col("Weight")))
                .withColumn("Height", when((col("Height").lt(lit(0)))
                        .or(col("Height").gt(lit(259))), lit(999).cast(DataTypes.StringType))
                        .when(col("Height").equalTo(""), null)
                        .otherwise(col("Height")))
                .withColumn("Pregnant", when(col("Pregnant").isin("True", "LIVE BIRTH"), "Yes")
                        .when(col("Pregnant").isin("No - Miscarriage (mc)", "No - Induced Abortion (ab)", "RECENTLY MISCARRIAGED"), "No")
                        .when(col("Pregnant").equalTo("UNKNOWN").or(col("Pregnant").equalTo("")), null)
                        .otherwise(col("Pregnant")))
                .withColumn("StabilityAssessment", when(col("StabilityAssessment").equalTo("Stable1"), "Stable")
                        .when(col("StabilityAssessment").equalTo("Not Stable"), "Unstable")
                        .when(col("StabilityAssessment").equalTo(""), null)
                        .otherwise(col("StabilityAssessment")))
                .withColumn("DifferentiatedCare", when(col("DifferentiatedCare").isin("Express Care", "Express", "Fast Track care", "Differentiated care model", "MmasRecommendation0"), "Fast Track")
                        .when(col("DifferentiatedCare").isin("Community ART Distribution_Point", "Individual Patient ART Distribution_community", "Community Based Dispensing", "Community ART distribution - HCW led", "Community_Based_Dispensing"), "Community ART Distribution HCW Led")
                        .when(col("DifferentiatedCare").isin("Community ART distribution � Peer led", "Community ART Distribution - Peer Led"), "Community ART Distribution peer led")
                        .when(col("DifferentiatedCare").isin("Facility ART Distribution Group", "FADG"), "Facility ART distribution Group")
                        .when(col("DifferentiatedCare").equalTo(""), null)
                        .otherwise(col("DifferentiatedCare")))
                .withColumn("VisitDate", when((col("VisitDate").lt(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("VisitDate").gt(Date.valueOf(LocalDate.now()))), lit(Date.valueOf(LocalDate.of(1900, 1, 1)))))
                .withColumn("NextAppointmentDate", when(col("NextAppointmentDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("NextAppointmentDate").gt(lit(Date.valueOf(LocalDate.now().plusYears(1))))), lit(Date.valueOf(LocalDate.of(1900, 1, 1)))));

        // Set values from look up tables
        sourceDf = sourceDf
                .join(familyPlanningDf, sourceDf.col("FamilyPlanningMethod").equalTo(familyPlanningDf.col("source_name")), "left")
                .join(pwpDf, sourceDf.col("PwP").equalTo(pwpDf.col("source_name")), "left")
                .withColumn("FamilyPlanningMethod", when(familyPlanningDf.col("target_name").isNotNull(), familyPlanningDf.col("target_name"))
                        .otherwise(col("FamilyPlanningMethod")))
                .withColumn("PwP", when(pwpDf.col("target_name").isNotNull(), pwpDf.col("target_name"))
                        .otherwise(col("PwP")));

        logger.info("Loading target visits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_visits");
        sourceDf.createOrReplaceTempView("source_visits");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_visits t LEFT ANTI JOIN source_visits s ON s.PatientID <=> t.PatientID" +
                " AND s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode AND s.VisitID <=> t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unMatchedMergeDf1 = session.sql("select PatientID, PatientPK, FacilityName, SiteCode, VisitId, VisitDate, Service, VisitType, WHOStage, WABStage, Pregnant, LMP, EDD, Height, Weight, BP, OI, OIDate, Adherence, AdherenceCategory, FamilyPlanningMethod, PwP, GestationAge, NextAppointmentDate, Emr, Project, CKV, DifferentiatedCare, StabilityAssessment, PopulationType, KeyPopulationType, VisitBy, Temp, PulseRate, RespiratoryRate, OxygenSaturation, Muac, NutritionalStatus, EverHadMenses, Breastfeeding, Menopausal, NoFPReason, ProphylaxisUsed, CTXAdherence, CurrentRegimen, HCWConcern, TCAReason, ClinicalNotes, GeneralExamination, SystemExamination, Skin, Eyes, ENT, Chest, CVS, Abdomen, CNS, Genitourinary from final_unmatched");
        Dataset<Row> sourceMergeDf2 = session.sql("select PatientID, PatientPK, FacilityName, SiteCode, VisitId, VisitDate, Service, VisitType, WHOStage, WABStage, Pregnant, LMP, EDD, Height, Weight, BP, OI, OIDate, Adherence, AdherenceCategory, FamilyPlanningMethod, PwP, GestationAge, NextAppointmentDate, Emr, Project, CKV, DifferentiatedCare, StabilityAssessment, PopulationType, KeyPopulationType, VisitBy, Temp, PulseRate, RespiratoryRate, OxygenSaturation, Muac, NutritionalStatus, EverHadMenses, Breastfeeding, Menopausal, NoFPReason, ProphylaxisUsed, CTXAdherence, CurrentRegimen, HCWConcern, TCAReason, ClinicalNotes, GeneralExamination, SystemExamination, Skin, Eyes, ENT, Chest, CVS, Abdomen, CNS, Genitourinary from source_visits");

        // Union all records together
        // TODO remove duplicates in final dataframe
        Dataset<Row> dfMergeFinal = unMatchedMergeDf1.union(sourceMergeDf2);

        // Write to target table
        long mergedFinalCount = dfMergeFinal.count();

        // final unmatched. Records in target not found at source
        long unmatchedMergeDf1Count = unMatchedMergeDf1.count();
        // source records count

        long sourceMergeDf2Count = sourceMergeDf2.count();
        logger.info("unmatchedMergeDf1Count (unmatched records): " + unmatchedMergeDf1Count);
        logger.info("sourceMergeDf2Count (source records): " + sourceMergeDf2Count);
        logger.info("Merged final count: " + mergedFinalCount);

        // TODO test out removeDuplicates() before Nov launch
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
