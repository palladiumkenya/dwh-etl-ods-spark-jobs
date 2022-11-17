package org.kenyahmis.adverseevents;

import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Date;
import java.time.LocalDate;

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
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("dbtable", "(" + sourceQuery + ") pv")
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        // load lookup tables
        Dataset<Row> lookupRegimenDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.regimen"))
                .load();

        Dataset<Row> lookupAdverseEventsDf = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.lookup.adverse"))
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
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .load();

        targetDf.persist(StorageLevel.DISK_ONLY());
        sourceDf.createOrReplaceTempView("source_events");
        targetDf.createOrReplaceTempView("target_events");

        Dataset<Row> unmatchedFromJoinDf = session.sql("SELECT t.* FROM target_events t LEFT ANTI JOIN source_events s ON s.SiteCode <=> t.SiteCode AND" +
                " s.PatientPK <=> t.PatientPK AND s.AdverseEventsUnique_ID <=> t.AdverseEventsUnique_ID");

        long unmatchedVisitCount = unmatchedFromJoinDf.count();
        logger.info("Unmatched count after target join is: " + unmatchedVisitCount);
        unmatchedFromJoinDf.createOrReplaceTempView("final_unmatched");

        Dataset<Row> unmatchedMergeDf1 = session.sql("SELECT PatientID,Patientpk, SiteCode,AdverseEvent,AdverseEventStartDate," +
                "AdverseEventEndDate,Severity,VisitDate,EMR,Project,AdverseEventCause,AdverseEventRegimen,AdverseEventActionTaken," +
                "AdverseEventClinicalOutcome,AdverseEventIsPregnant,CKV,PatientUnique_ID,AdverseEventsUnique_ID,DateImported" +
                " FROM final_unmatched");

        Dataset<Row> sourceMergeDf2 = session.sql("SELECT PatientID,Patientpk, SiteCode,AdverseEvent,AdverseEventStartDate," +
                "AdverseEventEndDate,Severity,VisitDate,EMR,Project,AdverseEventCause,AdverseEventRegimen,AdverseEventActionTaken," +
                "AdverseEventClinicalOutcome,AdverseEventIsPregnant,CKV,PatientUnique_ID,AdverseEventsUnique_ID,DateImported" +
                " FROM source_events");

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
