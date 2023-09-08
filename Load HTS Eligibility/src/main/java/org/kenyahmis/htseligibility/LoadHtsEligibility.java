package org.kenyahmis.htseligibility;

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

public class LoadHtsEligibility {
    private static final Logger logger = LoggerFactory.getLogger(LoadHtsEligibility.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Eligibility");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsEligibility.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsEligibility.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load hts eligibility query from file", e);
            return;
        }
        logger.info("Loading hts eligibility");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.htscentral.url"))
                .option("driver", rtConfig.get("spark.htscentral.driver"))
                .option("user", rtConfig.get("spark.htscentral.user"))
                .option("password", rtConfig.get("spark.htscentral.password"))
                .option("query", sourceQuery)
                .option("numpartitions", rtConfig.get("spark.htscentral.numpartitions"))
                .load();
        sourceDf.persist(StorageLevel.DISK_ONLY());

        //Clean source data
        sourceDf = sourceDf
                .withColumn("WeightLoss", when(col("WeightLoss").equalTo("0"), "No")
                        .when(col("WeightLoss").equalTo("1"), "Yes")
                        .otherwise(col("WeightLoss")))
                .withColumn("NightSweats", when(col("NightSweats").equalTo("0"), "No")
                        .when(col("NightSweats").equalTo("1"), "Yes")
                        .otherwise(col("NightSweats")))
                .withColumn("Pregnant", when(col("Pregnant").equalTo("0"), "No")
                        .when(col("Pregnant").equalTo("1"), "Yes")
                        .otherwise(col("Pregnant")))
                .withColumn("Cough", when(col("Cough").equalTo("0"), "No")
                        .when(col("Cough").equalTo("1"), "Yes")
                        .otherwise(col("Cough")))
                .withColumn("IsHealthWorker", when(col("IsHealthWorker").equalTo("0"), "No")
                        .when(col("IsHealthWorker").equalTo("1"), "Yes")
                        .otherwise(col("IsHealthWorker")))
                .withColumn("PatientType", when(col("PatientType").equalTo("HP:Hospital Patient"), "Hospital Patient")
                        .when(col("PatientType").equalTo("NP:Non-Hospital Patient"), "Non-Hospital Patient")
                        .otherwise(col("PatientType")))
//                .withColumn("TracingOutcome", when(col("TracingOutcome").equalTo("Contacted but not linked"), "Contacted and Not Linked")
//                        .otherwise(col("TracingOutcome")))
                .withColumn("TypeGBV", when(col("TypeGBV").equalTo(""), null)
                        .otherwise(col("TypeGBV")))
                .withColumn("ReceivedServices", when(col("ReceivedServices").equalTo(""), null)
                        .otherwise(col("ReceivedServices")))
                .withColumn("ResultOfHIVSelf", when(col("ResultOfHIVSelf").equalTo(""), null)
                        .otherwise(col("ResultOfHIVSelf")))
                .withColumn("ReasonsForIneligibility", when(col("ReasonsForIneligibility").equalTo(""), null)
                        .otherwise(col("ReasonsForIneligibility")))
                .withColumn("ChildReasonsForIneligibility", when(col("ChildReasonsForIneligibility").equalTo(""), null)
                        .otherwise(col("ChildReasonsForIneligibility")))
                .withColumn("PartnerHIVStatus", when(col("PartnerHIVStatus").equalTo(""), null)
                        .otherwise(col("PartnerHIVStatus")))
                .withColumn("RelationshipWithContact", when(col("RelationshipWithContact").equalTo(""), null)
                        .otherwise(col("RelationshipWithContact")))
                .withColumn("DateTestedProvider", when(col("DateTestedProvider").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1)))), null)
                        .otherwise(col("DateTestedProvider")))
                .withColumn("VisitDate", when(col("VisitDate").lt(lit(Date.valueOf(LocalDate.of(2019, 1, 1)))), null)
                        .otherwise(col("VisitDate")));

        logger.info("Loading target hts eligibility");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_EligibilityExtract")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_eligibility");
        sourceDf.createOrReplaceTempView("source_eligibility");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_eligibility s LEFT ANTI JOIN " +
                "target_eligibility t ON s.PatientPk <=> t.PatientPk AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
                .withColumn("HtsNumberHash", upper(sha2(col("HtsNumber").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String columnList = "ID,FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,Processed,QueueId,Status," +
                "StatusDate,EncounterId,VisitID,VisitDate,PopulationType,KeyPopulation,PriorityPopulation," +
                "Department,PatientType,IsHealthWorker,RelationshipWithContact,TestedHIVBefore,WhoPerformedTest," +
                "ResultOfHIV,DateTestedSelf,StartedOnART,CCCNumber,EverHadSex,SexuallyActive,NewPartner,PartnerHIVStatus," +
                "CoupleDiscordant,MultiplePartners,NumberOfPartners,AlcoholSex,MoneySex,CondomBurst,UnknownStatusPartner," +
                "KnownStatusPartner,Pregnant,BreastfeedingMother,ExperiencedGBV,ContactWithTBCase,Lethargy,EverOnPrep," +
                "CurrentlyOnPrep,EverOnPep,CurrentlyOnPep,EverHadSTI,CurrentlyHasSTI,EverHadTB,SharedNeedle," +
                "NeedleStickInjuries,TraditionalProcedures,ChildReasonsForIneligibility,EligibleForTest," +
                "ReasonsForIneligibility,SpecificReasonForIneligibility,Cough,DateTestedProvider,Fever,MothersStatus," +
                "NightSweats,ReferredForTesting,ResultOfHIVSelf,ScreenedTB,TBStatus,WeightLoss,AssessmentOutcome," +
                "ForcedSex,ReceivedServices,TypeGBV,Disability,DisabilityType,HTSStrategy,HTSEntryPoint,HIVRiskCategory," +
                "ReasonRefferredForTesting,ReasonNotReffered,HtsRiskScore";

        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_EligibilityExtract")
                .mode(SaveMode.Append)
                .save();
    }
}
