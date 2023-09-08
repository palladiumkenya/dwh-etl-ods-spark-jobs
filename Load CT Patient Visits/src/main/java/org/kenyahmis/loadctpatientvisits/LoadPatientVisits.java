package org.kenyahmis.loadctpatientvisits;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.kenyahmis.core.DatabaseUtils;
import org.kenyahmis.core.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.Properties;

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
        FileUtils<LoadPatientVisits> fileUtils = new FileUtils<>();
        try {
            sourceVisitsQuery = fileUtils.loadTextFromFile(LoadPatientVisits.class, sourceQueryFileName);
        } catch (IOException e) {
            e.printStackTrace();
            throw new RuntimeException("Failed to load visits query from file");
        }
        logger.info("Loading source Visits");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", String.format("( %s ) as source", sourceVisitsQuery))
                .option("partitionColumn", "SiteCode")
                .option("lowerBound", "10019")
                .option("upperBound", "28742")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();
        int initialSourcePartitions = sourceDf.rdd().getNumPartitions();
        logger.info("Initial source partitions is " + initialSourcePartitions);
        sourceDf.persist(StorageLevel.DISK_ONLY());

        // load lookup tables
        Dataset<Row> familyPlanningDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_family_planning_method")
                .option("query", "select source_name, target_name from dbo.lkp_family_planning_method")
                .load();
        Dataset<Row> pwpDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_pwp")
                .option("query", "select source_name, target_name from dbo.lkp_pwp")
                .load();

        // Clean source data
        sourceDf = sourceDf.withColumn("OIDATE", when((col("OIDATE").lt(lit(Date.valueOf(LocalDate.of(2000, 1, 1))).cast(DataTypes.DateType)))
                .or(col("OIDATE").gt(lit(Date.valueOf(LocalDate.now())).cast(DataTypes.DateType))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                .otherwise(col("OIDATE")))
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
                        .or(col("VisitDate").gt(Date.valueOf(LocalDate.now()))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("VisitDate")))
                .withColumn("NextAppointmentDate", when(col("NextAppointmentDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("NextAppointmentDate").gt(lit(Date.valueOf(LocalDate.now().plusYears(1))))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("NextAppointmentDate")));

        int secondarySourcePartitions = sourceDf.rdd().getNumPartitions();
        logger.info("Secondary source partitions is " + secondarySourcePartitions);

        //Set values from look up tables
        sourceDf = sourceDf
                .join(familyPlanningDf, sourceDf.col("FamilyPlanningMethod").equalTo(familyPlanningDf.col("source_name")), "left")
                .join(pwpDf, sourceDf.col("PwP").equalTo(pwpDf.col("source_name")), "left")
                .withColumn("FamilyPlanningMethod", when(familyPlanningDf.col("target_name").isNotNull(), familyPlanningDf.col("target_name"))
                        .otherwise(col("FamilyPlanningMethod")))
                .withColumn("PwP", when(pwpDf.col("target_name").isNotNull(), pwpDf.col("target_name"))
                        .otherwise(col("PwP")));

        int sourcePartitions = sourceDf.rdd().getNumPartitions();
        logger.info("Source partitions are: " + sourcePartitions);
        logger.info("Loading target visits");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.CT_PatientVisits")
                .load();

        int targetPartitions = targetDf.rdd().getNumPartitions();
        logger.info("Target partitions are: {}", targetPartitions);
        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_visits");
        sourceDf.createOrReplaceTempView("source_visits");
        sourceDf.printSchema();

        // Get unmatched records
        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_visits t LEFT ANTI JOIN source_visits s" +
                " ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode AND s.VisitID <=> t.VisitID");
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        unmatchedFromJoinDf.printSchema();


        final String columnList = "PatientID,FacilityName,SiteCode,PatientPK,VisitID,VisitDate,SERVICE,VisitType," +
                "WHOStage,WABStage,Pregnant,LMP,EDD,Height,Weight,BP,OI,OIDate,Adherence,AdherenceCategory," +
                "FamilyPlanningMethod,PwP,GestationAge,NextAppointmentDate,Emr,Project,DifferentiatedCare," +
                "StabilityAssessment,KeyPopulationType,PopulationType,VisitBy,Temp,PulseRate,RespiratoryRate," +
                "OxygenSaturation,Muac,NutritionalStatus,EverHadMenses,Breastfeeding,Menopausal,NoFPReason," +
                "ProphylaxisUsed,CTXAdherence,CurrentRegimen,HCWConcern,TCAReason,ClinicalNotes,ZScore," +
                "ZScoreAbsolute,RefillDate,PaedsDisclosure,Date_Created,Date_Last_Modified";

        unmatchedFromJoinDf = session.sql(String.format("select %s from final_unmatched", columnList));
        Dataset<Row> sourceMergeDf = session.sql(String.format("select %s from source_visits", columnList));

        Dataset<Row> dfMergeFinal = unmatchedFromJoinDf.union(sourceMergeDf);

        dfMergeFinal = dfMergeFinal.withColumn("PatientPKHash", upper(sha2(col("PatientPK").cast(DataTypes.StringType), 256)))
                .withColumn("PatientIDHash", upper(sha2(col("PatientID").cast(DataTypes.StringType), 256)));

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // backup target table
        try {
            dbUtils.renameTable("CT_PatientVisits", "CT_PatientVisits_bk");
            logger.info("Successfully backed up CT_PatientVisits");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }

        // recreate target tables
        String ddlFileName = "PatientVisitsDDL.sql";
        try {
            String query = fileUtils.loadTextFromFile(LoadPatientVisits.class, ddlFileName);
            dbUtils.runQuery(query);
        } catch (IOException | SQLException e) {
            e.printStackTrace();
            throw new RuntimeException(String.format("Failed to recreate %s table", ddlFileName));
        }

//        long mergedFinalCount = dfMergeFinal.count();
//        logger.info("Merged final count: " + mergedFinalCount);
        dfMergeFinal
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_PatientVisits")
                .mode(SaveMode.Append)
                .save();

        // Hash PII columns
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");
        try {
            dbUtils.hashPIIColumns("CT_PatientVisits", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }

        // delete backup
        try {
            dbUtils.dropTable("CT_PatientVisits_bk");
            logger.info("Successfully deleted CT_PatientVisits_bk");
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
