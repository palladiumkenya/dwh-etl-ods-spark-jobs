package org.kenyahmis.core;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;

public class DatabaseUtils {
    private Connection connection = null;
    private final Properties connectionProperties;


    public DatabaseUtils(Properties connectionProperties) {
        this.connectionProperties = connectionProperties;
    }

    private Connection getConnection() throws SQLException {
        if (connection == null || connection.isClosed()) {
            final String dbURL = connectionProperties.getProperty("dbURL");
            final String user = connectionProperties.getProperty("user");
            final String pass = connectionProperties.getProperty("pass");
            connection =  DriverManager.getConnection(dbURL, user, pass);
        }
        return connection;
    }

    public void runQuery(String query) throws SQLException {
        final Connection conn = getConnection();
        Statement statement = conn.createStatement();
        statement.execute(query);
        conn.close();
    }

    public void renameTable(String oldName, String newName) throws SQLException {
        final Connection conn = getConnection();
        String query = String.format("EXEC sp_rename %s, %s", oldName, newName);
        Statement statement = conn.createStatement();
        statement.execute(query);
        conn.close();
    }

    public void dropTable(final String tableName) throws SQLException {
        final Connection conn = getConnection();
        String query = String.format("DROP TABLE %s", tableName);
        Statement statement = conn.createStatement();
        statement.execute(query);
        conn.close();
    }

    public void hashPIIColumns(String tableName, HashMap<String, String> columns) throws SQLException {

        final Connection conn = getConnection();
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("UPDATE %s SET ", tableName));
        List<String> updates = new ArrayList<>();
        columns.keySet().forEach(key -> {
            String updated = String.format("%s = convert(nvarchar(64), hashbytes('SHA2_256', cast(\"%s\"  as nvarchar(36))), 2)", columns.get(key), key);
            updates.add(updated);
        });
        builder.append(String.join(",", updates));
        String query = builder.toString();
        Statement statement = conn.createStatement();
        statement.execute(query);
        conn.close();
    }
}
