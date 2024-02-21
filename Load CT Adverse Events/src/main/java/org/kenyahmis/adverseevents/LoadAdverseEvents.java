package org.kenyahmis.adverseevents;

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

public class LoadAdverseEvents {
    private static final Logger logger = LoggerFactory.getLogger(LoadAdverseEvents.class);

    public static void main(String[] args) {
        SparkConf conf = new SparkConf();
        conf.setAppName("Load Adverse Events");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

        final String sourceQueryFileName = "LoadSourceAdverseEvents.sql";
        String sourceQuery;
        InputStream inputStream = LoadAdverseEvents.class.getClassLoader().getResourceAsStream(sourceQueryFileName);
        if (inputStream == null) {
            logger.error(sourceQueryFileName + " not found");
            return;
        }
        try {
            sourceQuery = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load Adverse events query from file", e);
            return;
        }

        logger.info("Loading source Adverse Events");
        Dataset<Row> sourceDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.dwapicentral.url"))
                .option("driver", rtConfig.get("spark.dwapicentral.driver"))
                .option("user", rtConfig.get("spark.dwapicentral.user"))
                .option("password", rtConfig.get("spark.dwapicentral.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.dwapicentral.numpartitions"))
                .load();

        // load lookup tables
        Dataset<Row> lookupRegimenDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("query", "select source_name, target_name from dbo.lkp_regimen")
                .load();

        Dataset<Row> lookupAdverseEventsDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
//                .option("dbtable", "dbo.lkp_adverse_events")
                .option("query", "select source_name, target_name from dbo.lkp_adverse_events")
                .load();

        sourceDf = sourceDf
                .withColumn("AdverseEventStartDate", when(col("AdverseEventStartDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("AdverseEventStartDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("AdverseEventStartDate")))
                .withColumn("AdverseEventEndDate", when(col("AdverseEventEndDate").lt(lit(Date.valueOf(LocalDate.of(1980, 1, 1))))
                        .or(col("AdverseEventEndDate").gt(lit(Date.valueOf(LocalDate.now())))), lit(Date.valueOf(LocalDate.of(1900, 1, 1))))
                        .otherwise(col("AdverseEventEndDate")))
                .withColumn("Severity", when(col("Severity").isin("Mild", "Mild|Mild|Mild"), "Mild")
                        .when(col("Severity").isin("Moderate", "Moderate|Moderate", "Moderate|Moderate|Moderate"), "Moderate")
                        .when(col("Severity").isin("Severe", "Fatal", "Severe|Severe", "Severe|Severe|Severe"), "Severe")
                        .when(col("Severity").isin("Mild|Moderate", "Moderate|Mild", "Severe|Moderate", "Unknown|Moderate", "Moderate|Severe"), "Unknown")
                        .when(col("Severity").equalTo(""), null)
                        .otherwise(col("Severity")))
                .withColumn("AdverseEventActionTaken", when(col("AdverseEventActionTaken").isin("Medicine not changed", "CONTINUE REGIMEN", "CONTINUE REGIMEN|CONTINUE REGIMEN"), "Drug not Changed")
                        .when(col("AdverseEventActionTaken").equalTo("Dose reduced"), "Drug Reduced")
                        .when(col("AdverseEventActionTaken").equalTo("SUBSTITUTED DRUG"), "Drug Substituted")
                        .when(col("AdverseEventActionTaken").isin("Medicine causing AE substituted/withdrawn", "STOP", "STOP|STOP", "All drugs stopped", "STOP|STOP|STOP", "Other|STOP", "NONE|STOP"), "Drug Withdrawn")
                        .when(col("AdverseEventActionTaken").isin("Other", "NONE", "Select", "SUBSTITUTED DRUG|STOP", "Other|Other"), "OTHER")
                        .when(col("AdverseEventActionTaken").equalTo("SWITCHED REGIMEN"), "Regimen Switched")
                        .when(col("AdverseEventActionTaken").equalTo(""), null)
                        .otherwise(col("AdverseEventActionTaken")))
                .withColumn("AdverseEventCause", when(col("AdverseEventCause").isin("3TC/D4T", "3TC/TDF/NVP", "ABACAVIR", "abacavirwhen she was using", "ABC", "ABC+3TC", "abc/3tc/efv", "AF2B", "af2b- avonza", "ALL ARV", "ALUVIA", "art", "ARV", "arvs", "atanzanavir", "atavanavir", "ataz/rit", "atazanavir", "Atazanavir/Rironavir", "atazanavir/ritonavir", "ATV", "ATV/r", "ATVr", "AZT", "AZT+3TC+EFV", "AZT/3TC/NVP", "AZT/ATV", "AZT/KALETRA", "ctx/3tc/tdf/efv", "D4T", "D4T / 3TC / NVP", "D4T/3TC", "D4T/AZT", "DDI", "Dolotegravir", "doluteglavir", "dolutegravir", "DTG", "DTG Aurobindo", "dultegravir", "EFARIRENZ", "EFAVIRENCE", "Efavirens", "efavirenz", "efavirenze", "efavirez", "efervirence", "efervirenz", "efevurence", "EFV", "EFV 600MG", "EFV/NVP", "efv/rhze", "HAART", "KALETRA", "lopinanavir", "LOPINAVIR", "LPV", "LPV/r", "lpvr", "NVP", "NVP/ABC", "pep", "TDF", "tdf dtg", "TDF/3TC/", "tdf/3tc/dtg", "tdf/3tc/efv", "Tenoforvir", "tenofovir", "TLD", "TLE ", "TLE 400", "TRIMUNE", "ZIDOVUDINE", "EFV", "? NVP", "? TLD", "?ATV/r", "3TC", "3TC/3TC", "D4T", "EFAVIRENZ"), "ARV")
                        .when(col("AdverseEventCause").isin("ART/TB", "ARVS, CTX , IPT", "CTX OR EFV", "D4T/INH", "INH/NVP", "isoniazid and nevirapine", "isoniazid efavirenz", "NVP/CTX", "tdf dtg ctx 3tc", "inh, tdf,3tc,dtg, ctx"), "ARV + OTHER DRUGS")
                        .when(col("AdverseEventCause").isin("ANT TB", "ANTI TB", "anti TBs", "ANTI-TB", "Co-trimoxazole", "CONTRIMAZOLE", "cotrimoxasole", "cotrimoxazole", "cotrimoxazole 960mg", "Cotrimoxazole-", "CTX", "CTX /ANTI TB", "Dapson", "fluconazole", "IHN", "INH", "INH (IPT)", "INH/CTX", "IPT", "ipt in 2016", "ipt side effect ", "IRIS", "Isiniazid", "isiniazide", "isonaizid", "isoniaizid", "isoniasid", "isoniazid", "Isoniazid - November 2017", "isoniazide", "isoniazin", "isonizid", "Isonizide and Pyridoxine", "IZONIAZID", "IZONIAZIDE", "pyrazinamid", "pyrazinamide", "PYRIDOXINE", "RH", "RHE", "RHZE", "septin", "SEPTRIN", "septrine", "Streptomycin", "sulfa", "sulphonamides", "SULPHONOMIDES", "SULPHUR", "TB", "TB DRUGS", "tb meds", "2RHZ/4RH(children)", "2RHZE/10RH", "2RHZE/4RH", "2SRHZE/1RHZE/", "INH, SEPTRIN"), "NON-ARVS")
                        .when(col("AdverseEventCause").equalTo(""), null)
                        .otherwise(col("AdverseEventCause")))
                .withColumn("AdverseEventClinicalOutcome", when(col("AdverseEventClinicalOutcome").equalTo("Recovered/Resolved"), "Recovered")
                        .when(col("AdverseEventClinicalOutcome").equalTo("Recovering/Resolving"), "Recovering")
                        .when(col("AdverseEventClinicalOutcome").equalTo("Requires intervention to prevent permanent damage"), "OTHER")
                        .when(col("AdverseEventClinicalOutcome").equalTo(""), null)
                        .otherwise(col("AdverseEventClinicalOutcome")));

        // Set values from look up tables
        sourceDf = sourceDf
                .join(lookupRegimenDf, sourceDf.col("AdverseEventRegimen")
                        .equalTo(lookupRegimenDf.col("source_name")), "left")
                .join(lookupAdverseEventsDf, sourceDf.col("AdverseEvent")
                        .equalTo(lookupAdverseEventsDf.col("source_name")), "left")
                .withColumn("AdverseEventRegimen", when(lookupRegimenDf.col("target_name").isNotNull(), lookupRegimenDf.col("target_name"))
                        .otherwise(col("AdverseEventRegimen")))
                .withColumn("AdverseEvent", when(lookupAdverseEventsDf.col("target_name").isNotNull(), lookupAdverseEventsDf.col("target_name"))
                        .otherwise(col("AdverseEvent")));

        sourceDf.persist(StorageLevel.DISK_ONLY());

        logger.info("Loading target Adverse Events");
        Dataset<Row> targetDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.ods.url"))
                .option("driver", rtConfig.get("spark.ods.driver"))
                .option("user", rtConfig.get("spark.ods.user"))
                .option("password", rtConfig.get("spark.ods.password"))
                .option("dbtable", "dbo.CT_AdverseEvents")
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_events");
        targetDf.createOrReplaceTempView("target_events");

        Properties connectionProperties = new Properties();
        connectionProperties.setProperty("dbURL", rtConfig.get("spark.ods.url"));
        connectionProperties.setProperty("user", rtConfig.get("spark.ods.user"));
        connectionProperties.setProperty("pass", rtConfig.get("spark.ods.password"));
        DatabaseUtils dbUtils = new DatabaseUtils(connectionProperties);

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_events s LEFT ANTI JOIN target_events t ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK and cast(s.VisitDate as date) <=> t.VisitDate");

        long newAdverseEventsCount = newRecordsJoinDf.count();
        logger.info("New adverse events count is: " + newAdverseEventsCount);

        final String sourceColumnList = "PatientID,Patientpk,SiteCode,AdverseEvent,AdverseEventStartDate," +
                "AdverseEventEndDate,Severity,VisitDate,EMR,Project,AdverseEventCause,AdverseEventRegimen," +
                "AdverseEventActionTaken,AdverseEventClinicalOutcome,AdverseEventIsPregnant,Date_Created," +
                "Date_Last_Modified,recorduuid,voided,current_timestamp() as LoadDate";

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
                .option("dbtable", "dbo.CT_AdverseEvents")
                .mode(SaveMode.Append)
                .save();

        // Hash PII
        HashMap<String, String> hashColumns = new HashMap<>();
        hashColumns.put("PatientID", "PatientIDHash");
        hashColumns.put("PatientPK", "PatientPKHash");

        try {
            dbUtils.hashPIIColumns("CT_AdverseEvents", hashColumns);
        } catch (SQLException se) {
            se.printStackTrace();
            throw new RuntimeException();
        }
    }
}
