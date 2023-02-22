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
import java.sql.*;
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

        // Get new records
        Dataset<Row> newRecordsJoinDf = session.sql("SELECT s.* FROM source_visits s LEFT ANTI JOIN " +
                "target_visits t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode AND s.VisitID <=> t.VisitID ");

        long newRecordsCount = newRecordsJoinDf.count();
        logger.info("New record count is: " + newRecordsCount);
        newRecordsJoinDf.createOrReplaceTempView("new_records");

        // Get matched records
//        if (rtConfig.contains("spark.ods.update") && rtConfig.get("spark.ods.update").equals("yes")) {
//            LoadPatientVisits loadPatientVisits = new LoadPatientVisits();
//            loadPatientVisits.updateMatchedRecords(session, rtConfig);
//        }

        newRecordsJoinDf = session.sql("select PatientID,FacilityName,SiteCode,PatientPK,VisitID," +
                "VisitDate,SERVICE,VisitType,WHOStage,WABStage,Pregnant,LMP,EDD,Height,Weight,BP,OI,OIDate,Adherence," +
                "AdherenceCategory,FamilyPlanningMethod,PwP,GestationAge,NextAppointmentDate,Emr,Project," +
                "DifferentiatedCare,StabilityAssessment,KeyPopulationType,PopulationType,VisitBy,Temp,PulseRate," +
                "RespiratoryRate,OxygenSaturation,Muac,NutritionalStatus,EverHadMenses,Breastfeeding,Menopausal," +
                "NoFPReason,ProphylaxisUsed,CTXAdherence,CurrentRegimen,HCWConcern,TCAReason,ClinicalNotes," +
                "PatientUnique_ID,PatientVisitUnique_ID from new_records");

        // TODO test out removeDuplicates() before Nov launch
        newRecordsJoinDf
                .repartition(Integer.parseInt(rtConfig.get("spark.source.numpartitions")))
                .write()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .mode(SaveMode.Append)
                .save();
    }

    private void updateMatchedRecords(SparkSession session, RuntimeConfig rtConfig) {
        Dataset<Row> matchedRecordsDf = session.sql("SELECT s.* FROM source_visits s INNER JOIN " +
                "target_visits t ON s.PatientPK <=> t.PatientPK AND s.SiteCode <=> t.SiteCode AND s.VisitID <=> t.VisitID");

        matchedRecordsDf.printSchema();
        matchedRecordsDf.createOrReplaceTempView("matched_records");
        matchedRecordsDf = session.sql("select PatientID,FacilityName,SiteCode,PatientPK,VisitID," +
                "VisitDate,SERVICE,VisitType,WHOStage,WABStage,Pregnant,LMP,EDD,Height,Weight,BP,OI,OIDate,Adherence," +
                "AdherenceCategory,FamilyPlanningMethod,PwP,GestationAge,NextAppointmentDate,Emr,Project," +
                "DifferentiatedCare,StabilityAssessment,KeyPopulationType,PopulationType,VisitBy,Temp,PulseRate," +
                "RespiratoryRate,OxygenSaturation,Muac,NutritionalStatus,EverHadMenses,Breastfeeding,Menopausal," +
                "NoFPReason,ProphylaxisUsed,CTXAdherence,CurrentRegimen,HCWConcern,TCAReason,ClinicalNotes," +
                "PatientUnique_ID,PatientVisitUnique_ID from matched_records");
        String dbURL = rtConfig.get("spark.sink.url");
        String user = rtConfig.get("spark.sink.user");
        String pass = rtConfig.get("spark.sink.password");
        matchedRecordsDf
                .repartition(Integer.parseInt(rtConfig.get("spark.source.numpartitions")))
                .javaRDD().foreachPartition(part -> {
                    final Connection conn = DriverManager.getConnection(dbURL, user, pass);
                    try {
                        while (part.hasNext()) {
                            Row row = part.next();
                            String query = "UPDATE CT_PatientVisits set " +
                                    "SERVICE = ?,VisitType = ?,WHOStage = ?,WABStage = ?,Pregnant = ?,LMP = ?,EDD = ?,Height =?," +
                                    "Weight = ?,BP = ?,OI = ?,OIDate = ?,Adherence = ?,AdherenceCategory = ?, FamilyPlanningMethod = ?," +
                                    "PwP = ?,GestationAge = ?,NextAppointmentDate = ?,Emr = ?,Project = ?,CKV = ?," +
                                    "DifferentiatedCare = ?,StabilityAssessment = ?,KeyPopulationType = ?,PopulationType = ?," +
                                    "VisitBy = ?,Temp = ?,PulseRate = ?,RespiratoryRate = ?,OxygenSaturation = ?,Muac = ?," +
                                    "NutritionalStatus = ?,EverHadMenses = ?,Breastfeeding = ?,Menopausal = ?,NoFPReason = ?," +
                                    "ProphylaxisUsed = ?,CTXAdherence = ?,CurrentRegimen = ?,HCWConcern = ?, TCAReason = ?," +
                                    "ClinicalNotes = ?,PatientUnique_ID = ?,PatientVisitUnique_ID = ? " +
                                    "WHERE PatientPK = ? AND SiteCode = ? AND VisitID = ?";
                            PreparedStatement preparedStatement = conn.prepareStatement(query);
                            preparedStatement.setString(1, row.getAs("SERVICE"));
                            preparedStatement.setString(2, row.getAs("VisitType"));
                            if (row.getAs("WHOStage") != null) {
                                preparedStatement.setString(3, row.getAs("WHOStage").toString());
                            } else {
                                preparedStatement.setNull(3, Types.NULL);
                            }
                            preparedStatement.setString(4, row.getAs("WABStage"));
                            preparedStatement.setString(5, row.getAs("Pregnant"));
                            if (row.getAs("LMP") != null) {
                                preparedStatement.setString(6, row.getAs("LMP").toString());
                            } else {
                                preparedStatement.setNull(6, Types.NULL);
                            }
                            if (row.getAs("EDD") != null) {
                                preparedStatement.setString(7, row.getAs("EDD").toString());
                            } else {
                                preparedStatement.setNull(7, Types.NULL);
                            }

                            preparedStatement.setString(8, row.getAs("Height"));
                            preparedStatement.setString(9, row.getAs("Weight"));
                            preparedStatement.setString(10, row.getAs("BP"));
                            preparedStatement.setString(11, row.getAs("OI"));
                            if (row.getAs("OIDate") != null) {
                                preparedStatement.setDate(12, new Date(((Timestamp) row.getAs("OIDate")).getTime()));
                            } else {
                                preparedStatement.setNull(12, Types.NULL);
                            }

                            preparedStatement.setString(13, row.getAs("Adherence"));
                            preparedStatement.setString(14, row.getAs("AdherenceCategory"));
                            preparedStatement.setString(15, row.getAs("FamilyPlanningMethod"));
                            preparedStatement.setString(16, row.getAs("PwP"));
                            if (row.getAs("GestationAge") != null) {
                                preparedStatement.setString(17, row.getAs("GestationAge").toString());
                            } else {
                                preparedStatement.setNull(17, Types.NULL);
                            }

                            if (row.getAs("NextAppointmentDate") != null) {
                                preparedStatement.setDate(18, new Date(((Timestamp) row.getAs("NextAppointmentDate")).getTime()));
                            } else {
                                preparedStatement.setNull(18, Types.NULL);
                            }
                            preparedStatement.setString(19, row.getAs("Emr"));
                            preparedStatement.setString(20, row.getAs("Project"));
                            preparedStatement.setString(21, row.getAs("CKV"));
                            preparedStatement.setString(22, row.getAs("DifferentiatedCare"));
                            preparedStatement.setString(23, row.getAs("StabilityAssessment"));
                            preparedStatement.setString(24, row.getAs("KeyPopulationType"));
                            preparedStatement.setString(25, row.getAs("PopulationType"));
                            preparedStatement.setString(26, row.getAs("VisitBy"));
                            if (row.getAs("Temp") != null) {
                                preparedStatement.setBigDecimal(27, row.getAs("Temp"));
                            } else {
                                preparedStatement.setNull(27, Types.NULL);
                            }

                            if (row.getAs("PulseRate") != null) {
                                preparedStatement.setInt(28, row.getAs("PulseRate"));
                            } else {
                                preparedStatement.setNull(28, Types.NULL);
                            }

                            if (row.getAs("RespiratoryRate") != null) {
                                preparedStatement.setInt(29, row.getAs("RespiratoryRate"));
                            } else {
                                preparedStatement.setNull(29, Types.NULL);
                            }
                            if (row.getAs("OxygenSaturation") != null) {
                                preparedStatement.setBigDecimal(30, row.getAs("OxygenSaturation"));
                            } else {
                                preparedStatement.setNull(30, Types.NULL);
                            }
                            if (row.getAs("Muac") != null) {
                                preparedStatement.setInt(31, row.getAs("Muac"));
                            } else {
                                preparedStatement.setNull(31, Types.NULL);
                            }
                            preparedStatement.setString(32, row.getAs("NutritionalStatus"));
                            preparedStatement.setString(33, row.getAs("EverHadMenses"));
                            preparedStatement.setString(34, row.getAs("Breastfeeding"));
                            preparedStatement.setString(35, row.getAs("Menopausal"));
                            preparedStatement.setString(36, row.getAs("NoFPReason"));
                            preparedStatement.setString(37, row.getAs("ProphylaxisUsed"));
                            preparedStatement.setString(38, row.getAs("CTXAdherence"));
                            preparedStatement.setString(39, row.getAs("CurrentRegimen"));
                            preparedStatement.setString(40, row.getAs("HCWConcern"));
                            preparedStatement.setString(41, row.getAs("TCAReason"));
                            preparedStatement.setString(42, row.getAs("ClinicalNotes"));
                            preparedStatement.setString(43, row.getAs("PatientUnique_ID"));
                            preparedStatement.setString(44, row.getAs("PatientVisitUnique_ID"));
                            preparedStatement.setString(45, row.getAs("PatientPK").toString());
                            preparedStatement.setString(46, row.getAs("SiteCode").toString());
                            preparedStatement.setString(47, row.getAs("VisitID").toString());
                            int rows = preparedStatement.executeUpdate();
                            logger.info(rows + " affected in visit update");
                        }
                    } catch (SQLException ex) {
                        ex.printStackTrace();
                    } finally {
                        try {
                            if (conn != null && !conn.isClosed()) {
                                conn.close();
                            }
                        } catch (SQLException ex) {
                            logger.error("Failed to close sql connection");
                            ex.printStackTrace();
                        }
                    }
                }
        );
    }
}
