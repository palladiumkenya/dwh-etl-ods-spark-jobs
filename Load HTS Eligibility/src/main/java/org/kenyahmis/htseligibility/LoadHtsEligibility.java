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


        logger.info("Loading target hts eligibility");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", rtConfig.get("spark.ods.dbtable"))
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


        newRecordsJoinDf = session.sql("select ID,FacilityName,SiteCode,PatientPk,HtsNumber,Emr,Project,Processed," +
                "QueueId,Status,StatusDate,EncounterId,VisitID,VisitDate,PopulationType,KeyPopulation,PriorityPopulation," +
                "Department,PatientType,IsHealthWorker,RelationshipWithContact,TestedHIVBefore,WhoPerformedTest,ResultOfHIV," +
                "DateTestedSelf,StartedOnART,CCCNumber,EverHadSex,SexuallyActive,NewPartner,PartnerHIVStatus,CoupleDiscordant," +
                "MultiplePartners,NumberOfPartners,AlcoholSex,MoneySex,CondomBurst,UnknownStatusPartner,KnownStatusPartner," +
                "Pregnant,BreastfeedingMother,ExperiencedGBV,ContactWithTBCase,Lethargy,EverOnPrep,CurrentlyOnPrep,EverOnPep," +
                "CurrentlyOnPep,EverHadSTI,CurrentlyHasSTI,EverHadTB,SharedNeedle,NeedleStickInjuries,TraditionalProcedures," +
                "ChildReasonsForIneligibility,EligibleForTest,ReasonsForIneligibility,SpecificReasonForIneligibility,Cough," +
                "DateTestedProvider,Fever,MothersStatus,NightSweats,ReferredForTesting,ResultOfHIVSelf,ScreenedTB,TBStatus," +
                "WeightLoss,AssessmentOutcome,ForcedSex,ReceivedServices,TypeGBV,PatientPKHash,HtsNumberHash" +
                " from new_records");

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", rtConfig.get("spark.ods.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }
}
