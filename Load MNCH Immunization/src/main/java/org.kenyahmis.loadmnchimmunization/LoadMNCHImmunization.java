package org.kenyahmis.loadmnchimmunization;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;


public class LoadMNCHImmunization {
    private static final Logger logger = LoggerFactory.getLogger(LoadMNCHImmunization.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load MNCH Immunization");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadMNCHImmunization.sql";
        String sourceQuery;
        InputStream inputStream = LoadMNCHImmunization.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load MNCH immunization query from file", e);
            return;
        }

        logger.info("Loading source MNCH Immunization");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.mnchcentral.url"))
                .option("driver", rtConfig.get("spark.mnchcentral.driver"))
                .option("user", rtConfig.get("spark.mnchcentral.user"))
                .option("password", rtConfig.get("spark.mnchcentral.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.mnchcentral.numpartitions"))
                .load();

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target MNCH Immunization");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.MNCH_Immunization")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_immunization");
        targetDf.createOrReplaceTempView("target_immunization");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_immunization s LEFT ANTI JOIN target_immunization t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK and s.PatientMnchID <=> t.PatientMnchID and s.ID = t.ID");

        long newCervicalScreeningCount = newRecordsJoinDf.count();
        logger.info("New mnch immunization count is {} ",newCervicalScreeningCount);

        final String sourceColumnList = "Id,RefId,PatientPk,SiteCode,Emr,Project,DateExtracted,FacilityId," +
                "FacilityName,PatientMnchID,BCG,OPVatBirth,OPV1,OPV2,OPV3,IPV,DPTHepBHIB1,DPTHepBHIB2,DPTHepBHIB3," +
                "PCV101,PCV102,PCV103,ROTA1,MeaslesReubella1,YellowFever,MeaslesReubella2,MeaslesAt6Months,ROTA2," +
                "DateOfNextVisit,BCGScarChecked,DateChecked,DateBCGrepeated,VitaminAAt6Months,VitaminAAt1Yr," +
                "VitaminAAt18Months,VitaminAAt2Years,VitaminAAt2To5Years,FullyImmunizedChild";

        newRecordsJoinDf.createOrReplaceTempView("new_records");
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", sourceColumnList));

        // Write to target table
        newRecordsJoinDf
                .repartition(50)
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.MNCH_Immunization")
                .mode(SaveMode.Append)
                .save();

    }
}
