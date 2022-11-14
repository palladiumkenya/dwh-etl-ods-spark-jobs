package org.kenyahmis.loadfacilities;

import com.mysql.cj.jdbc.MysqlDataSource;
import org.apache.commons.io.IOUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.*;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

public class LoadFacilities {
    private static final Logger logger = LoggerFactory.getLogger(LoadFacilities.class);

    public static void main(String[] args) {

        SparkConf conf = new SparkConf();
        conf.setAppName("Load facilities");
        SparkSession session = SparkSession.builder()
                .config(conf)
                .getOrCreate();
        RuntimeConfig rtConfig = session.conf();

//        MysqlDataSource dataSource = new MysqlDataSource();
//        dataSource.setUser(rtConfig.get("spark.source.user"));
//        dataSource.setPassword(rtConfig.get("spark.source.password"));
//        dataSource.setServerName(rtConfig.get("spark.source.database-host"));
        final int minPartitionColumnValue;
        final int maxPartitionColumnValue;
        final int targetPartitions = 10;
//        try {
//            Connection conn = dataSource.getConnection();
//            Statement statement = conn.createStatement();
//            String query = "select min(mfl_code),max(mfl_code) from " +
//                    rtConfig.get("spark.source.database-name") + "." + rtConfig.get("spark.source.metadata-table");
//            ResultSet rs = statement.executeQuery(query);
//            if (!rs.next()) {
//                logger.error("Metadata table has no records");
//                return;
//            }
//            minPartitionColumnValue = rs.getInt(1);
//            maxPartitionColumnValue = rs.getInt(2);
//        } catch (Exception e) {
//            logger.error("Failed to get source table metadata", e);
//            return;
//        }

        final String queryFileName = "LoadSites.sql";
        String query;
        InputStream inputStream = LoadFacilities.class.getClassLoader().getResourceAsStream(queryFileName);
        if (inputStream == null) {
            logger.error(queryFileName + " not found");
            return;
        }
        try {
            query = IOUtils.toString(inputStream, Charset.defaultCharset());
        } catch (IOException e) {
            logger.error("Failed to load query from file", e);
            return;
        }

        logger.info("Loading source facilities data frame");
        Dataset<Row> sourceDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.source.url"))
                .option("driver", rtConfig.get("spark.source.driver"))
                .option("user", rtConfig.get("spark.source.user"))
                .option("password", rtConfig.get("spark.source.password"))
                .option("query", query)
//                .option("dbtable", "(" + query + ") af")
//                .option("partitioncolumn", "MFL_Code")
//                .option("lowerbound", minPartitionColumnValue)
//                .option("upperbound", maxPartitionColumnValue)
                // TODO Make this value dynamic based on upper/lower bounds
                .option("numpartitions", rtConfig.get("spark.source.numpartitions"))
                .load();

        sourceDataFrame.persist(StorageLevel.MEMORY_ONLY());
//        sourceDataFrame.printSchema();
        logger.info("Loading target facilities data frame");
        Dataset<Row> targetDataFrame = session.read()
                .format("jdbc")
                .option("url", rtConfig.get("spark.sink.url"))
                .option("driver", rtConfig.get("spark.sink.driver"))
                .option("user", rtConfig.get("spark.sink.user"))
                .option("password", rtConfig.get("spark.sink.password"))
                .option("dbtable", rtConfig.get("spark.sink.dbtable"))
                .option("numpartitions", rtConfig.get("spark.sink.numpartitions"))
                .load();
        targetDataFrame.persist(StorageLevel.MEMORY_ONLY());

        // source comparison data frame
        Dataset<Row> sourceComparisonDf = sourceDataFrame.select(col("MFL_Code"));

        // target comparison data frame
        Dataset<Row> targetComparisonDf = targetDataFrame.select(col("MFL_Code"));

        Dataset<Row> unmatchedFacilities = targetComparisonDf.except(sourceComparisonDf).withColumnRenamed("MFL_Code", "UN_MFL_Code");

        Dataset<Row> finalUnmatchedDf = unmatchedFacilities.join(targetDataFrame,
                (targetComparisonDf.col("MFL_Code").equalTo(unmatchedFacilities.col("UN_MFL_Code"))), "inner");
//        String finalUnmatchedColumns = Arrays.toString(finalUnmatchedDf.columns());
//        logger.info("Final Unmatched columns: " + finalUnmatchedColumns);

        finalUnmatchedDf.createOrReplaceTempView("final_unmatched");
        sourceDataFrame.createOrReplaceTempView("source_dataframe");

        Dataset<Row> mergeDf1 = session.sql("select MFL_Code, \"Facility Name\", County, SubCounty, Owner, Latitude, Longitude, SDP, EMR, \"EMR Status\", \"HTS Use\", \"HTS Deployment\", \"HTS Status\", \"IL Status\", BOOLEAN(\"Registration IE\"), BOOLEAN(\"Pharmacy IE\"), mlab, Ushauri, Nishauri, OVC, OTZ, PrEP, 3PM, AIR, KP, MCH, TB,\"Lab Manifest\" from final_unmatched");
        Dataset<Row> mergeDf2 = session.sql("select MFL_Code, \"Facility Name\", County, SubCounty, Owner, Latitude, Longitude, SDP, EMR, \"EMR Status\", \"HTS Use\", \"HTS Deployment\", \"HTS Status\", \"IL Status\", BOOLEAN(\"Registration IE\"), BOOLEAN(\"Pharmacy IE\"), STRING(mlab), STRING(Ushauri), STRING(Nishauri), STRING(OVC), STRING(OTZ), STRING(PrEP), STRING(3PM), STRING(AIR), STRING(KP), STRING(MCH), STRING(TB), STRING(\"Lab Manifest\") from source_dataframe");

        mergeDf1.printSchema();
        mergeDf2.printSchema();
        // Union all records together
        Dataset<Row> dfMergeFinal = mergeDf1.unionAll(mergeDf2);
        dfMergeFinal
                .repartition(targetPartitions)
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
