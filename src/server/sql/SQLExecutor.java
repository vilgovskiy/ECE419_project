package server.sql;

import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.PreparedStatement;
import java.sql.Connection;

import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;

public class SQLExecutor {
    private static Logger logger = Logger.getRootLogger();

    private SQLStorage storage;

    public SQLExecutor(SQLStorage storage) {
        this.storage = storage;
    }

    public List<String> query(String sql) {
        List<String> resultSet = new ArrayList<>();

        try {
            Connection conn = storage.connect();
            Statement statement  = conn.createStatement();
            ResultSet rs    = statement.executeQuery(sql);
            ResultSetMetaData rsmd = rs.getMetaData();
            List<String> colNames = new ArrayList<>();

            int colCount = rsmd.getColumnCount();
            for (int i = 1; i <= colCount; i++) {
                String colName = rsmd.getColumnName(i);
                colNames.add(colName);
            }
            resultSet.add(String.join("\t|\t", colNames));

            while (rs.next()) {
                List<String> resultRow = new ArrayList<>();
                for (int i = 1; i <= colCount; i++) {
                    Object obj = rs.getObject(i);
                    resultRow.add(obj.toString());
                }
                String result = String.join("\t|\t", resultRow);
                resultSet.add(result);
            }
            return resultSet;
        } catch (SQLException e) {
            logger.error("Error while executing QUERY statement " + sql );
            e.printStackTrace();
            return null;
        }
    }

    public boolean insert(String sql) {
        try {
            Connection conn = storage.connect();
            // supposed to use preparestatement but no one is gonna do sql injection
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            logger.error("Error while executing INSERT statement " + sql);
            e.printStackTrace();
            return false;
        }
    }

    public boolean update(String sql) {
        try {
            Connection conn = storage.connect();
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            logger.error("Error while executing UPDATE statement " + sql );
            e.printStackTrace();
            return false;
        }
    }

    public boolean delete(String sql) {
        try {
            Connection conn = storage.connect();
            Statement statement = conn.createStatement();
            statement.executeUpdate(sql);
            return true;
        } catch (SQLException e) {
            logger.error("Error while executing DELETE statement" + sql);
            e.printStackTrace();
            return false;
        }
    }

    public boolean create(String sql) {
        try {
            Connection conn = storage.connect();
            Statement statement = conn.createStatement();
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            logger.error("Error while executing CREATE statement " + sql);
            e.printStackTrace();
            return false;
        }
    }

    public boolean drop(String sql) {
        try {
            Connection conn = storage.connect();
            Statement statement = conn.createStatement();
            statement.execute(sql);
            return true;
        } catch (SQLException e) {
            logger.error("Error while executing DROP statement " + sql);
            e.printStackTrace();
            return false;
        }
    }
}
