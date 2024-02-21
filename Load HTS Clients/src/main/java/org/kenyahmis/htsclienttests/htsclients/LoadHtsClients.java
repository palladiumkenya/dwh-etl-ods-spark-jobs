package org.kenyahmis.htsclienttests.htsclients;

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

public class LoadHtsClients {

    private static final Logger logger = LoggerFactory.getLogger(LoadHtsClients.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load HTS Clients");

        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadHtsClients.sql";
        String sourceQuery;
        InputStream inputStream = LoadHtsClients.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load hts clients query from file", e);
            return;
        }
        logger.info("Loading hts clients");
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

        Dataset<Row> maritalStatusLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_MaritalStatus")
                .option("query", "select Source_MaritalStatus,Target_MaritalStatus from dbo.lkp_MaritalStatus")
                .load();

        Dataset<Row> htsDisabilityLookupDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_htsDisability")
                .option("query", "select Source_Disability,Target_Disability from dbo.lkp_htsDisability")
                .load();

        //Clean source data
        sourceDf = sourceDf
                .withColumn("Dob", when(col("Dob").lt(lit(Date.valueOf(LocalDate.of(1910, 1, 1))))
                        .or(col("Dob").gt(lit(Date.valueOf(LocalDate.now())))), null)
                        .otherwise(col("Dob")))
                .withColumn("Gender", when(col("Gender").equalTo("M"), "Male")
                        .when(col("Gender").equalTo("F"), "Female")
                        .otherwise(col("Gender")))
                .withColumn("PatientDisabled", when(col("PatientDisabled").equalTo("No"), "No")
                        .when(col("PatientDisabled").isNotNull().and(col("PatientDisabled").notEqual("No")), "Yes")
                        .otherwise(null));

        //Set values from look up tables
        sourceDf = sourceDf
                .join(htsDisabilityLookupDf, htsDisabilityLookupDf.col("Source_Disability").equalTo(sourceDf.col("DisabilityType")), "left")
                .join(maritalStatusLookupDf, maritalStatusLookupDf.col("Source_MaritalStatus").equalTo(sourceDf.col("MaritalStatus")), "left")
                .withColumn("MaritalStatus",
                        when(maritalStatusLookupDf.col("Target_MaritalStatus").isNotNull(), maritalStatusLookupDf.col("Target_MaritalStatus"))
                                .otherwise(col("MaritalStatus")))
                .withColumn("DisabilityType",
                        when(htsDisabilityLookupDf.col("Target_Disability").isNotNull(), htsDisabilityLookupDf.col("Target_Disability"))
                                .otherwise(col("DisabilityType")));

        logger.info("Loading target hts clients");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("numpartitions", rtConfig.get("spark.ods.numpartitions"))
                .option("dbtable", "dbo.HTS_clients")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        targetDf.createOrReplaceTempView("target_clients");
        sourceDf.createOrReplaceTempView("source_clients");

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_clients s LEFT ANTI JOIN " +
                "target_clients t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode");

        // Hash PII columns
//        newRecordsJoinDf = newRecordsJoinDf.withColumn("PatientPKHash", upper(sha2(col("PatientPk").cast(DataTypes.StringType), 256)))
//                .withColumn("NupiHash", upper(sha2(col("NUPI").cast(DataTypes.StringType), 256)));

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        String columnList = "HtsNumber,Emr,Project,PatientPk,SiteCode,FacilityName,Dob,Gender,MaritalStatus," +
                "KeyPopulationType,DisabilityType,PatientDisabled,County,SubCounty,Ward,NUPI," +
                "HtsRecencyId,Occupation,PriorityPopulationType,pkv,RecordUUID";
        newRecordsJoinDf = session.sql(String.format("select %s from new_records", columnList));

        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.ods.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.HTS_clients")
                .mode(SaveMode.Append)
                .save();

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("NUPI", "NupiHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("dbo.HTS_clients", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}

