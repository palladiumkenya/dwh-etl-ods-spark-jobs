package org.kenyahmis.drugalcoholscreening;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.when;

public class LoadDrugAlcoholScreening {
    private static final Logger logger = LoggerFactory.getLogger(LoadDrugAlcoholScreening.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Drug and Alcohol Screening");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadDrugAlcoholScreening.sql";
        String sourceQuery;
        InputStream inputStream = LoadDrugAlcoholScreening.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load Alcohol and drug screening query from file", e);
            return;
        }

        logger.info("Loading source Alcohol drug screening");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDf = sourceDf
                .withColumn("DrinkingAlcohol", when(col("DrinkingAlcohol").equalTo("No"), "Never")
                        .when(col("DrinkingAlcohol").equalTo("Yes"), "OTHER")
                        .otherwise(col("DrinkingAlcohol")))
                .withColumn("Smoking", when(col("Smoking").equalTo("No"), "Never")
                        .when(col("Smoking").equalTo("Yes"), "OTHER")
                        .otherwise(col("Smoking")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target Alcohol drug screening");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_alcohol_drug_screening");
        targetDf.createOrReplaceTempView("target_alcohol_drug_screening");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_alcohol_drug_screening t LEFT ANTI JOIN source_alcohol_drug_screening s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.DrugAlcoholScreeningUnique_ID <=> t.DrugAlcoholScreeningUnique_ID AND s.VisitID <=> t.VisitID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,PatientPK,SiteCode,FacilityName,VisitID," +
                "VisitDate,Emr,Project,DrinkingAlcohol,Smoking,DrugUse,DateImported,CKV,PatientUnique_ID,DrugAlcoholScreeningUnique_ID" +
                " FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,PatientPK,SiteCode,FacilityName,VisitID," +
                "VisitDate,Emr,Project,DrinkingAlcohol,Smoking,DrugUse,DateImported,CKV,PatientUnique_ID,DrugAlcoholScreeningUnique_ID" +
                " FROM source_alcohol_drug_screening");

        sourceMergeDf2.printSchema();
        unmatchedMergeDf1.printSchema();

        Dataset<Row> dfMergeFinal = unmatchedMergeDf1.union(sourceMergeDf2);

        long mergedFinalCount = dfMergeFinal.count();
        logger.info("Merged final count: " + mergedFinalCount);

        // Write to target table
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
