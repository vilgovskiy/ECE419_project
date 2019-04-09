package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;

import junit.framework.TestCase;
import server.sql.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class SQLStorageTest extends TestCase {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static Logger logger = Logger.getRootLogger();
    private SQLStorage storage;
    private SQLExecutor executor;
    private Connection conn;


    @Override
    public void setUp() {
        storage = new SQLStorage("test1");
        executor = new SQLExecutor(storage);
        conn = storage.connect();
        assert(conn != null);
    }

    @Override
    public void tearDown() throws SQLException {
        conn.close();
        storage.clearSQLStorage();
    }

    public void sqlCreate() {
        String sqlCreate = "CREATE TABLE IF NOT EXISTS warehouses ("
                + "	id integer PRIMARY KEY,"
                + "	name text NOT NULL,"
                + "	capacity integer"
                + ");";
        boolean result = executor.create(sqlCreate);
        assert(result);
    }

    @Test
    public void testSQLCreate() {
        boolean exists = false;
        String sqlCreate = "CREATE TABLE IF NOT EXISTS warehouses ("
                + "	id integer PRIMARY KEY,"
                + "	name text NOT NULL,"
                + "	capacity integer"
                + ");";
        logger.info("SQL Test executing command: " + sqlCreate);

        boolean result = executor.create(sqlCreate);
        assert(result);

        try {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getTables(null, null, "warehouses", null);
            while (rs.next()) {
                String tName = rs.getString("TABLE_NAME");
                if (tName != null && tName.equals("warehouses")) {
                    exists = true;
                    break;
                }
            }
            assert(exists);
        } catch (SQLException e) {
        }
    }

    @Test
    public void testSQLDrop() {
        // create table
        sqlCreate();

        //drop table
        String sqlDrop = "DROP TABLE IF EXISTS warehouses;";
        boolean result = executor.drop(sqlDrop);
        assert(result);

        boolean tableExists = false;
        try {
            DatabaseMetaData md = conn.getMetaData();
            ResultSet rs = md.getTables(null, null, "warehouses", null);
            while (rs.next()) {
                String tName = rs.getString("TABLE_NAME");
                if (tName != null && tName.equals("warehouses")) {
                    tableExists = true;
                    break;
                }
            }
            assert(!tableExists);
        } catch (SQLException e) {
        }
    }

    @Test
    public void testSQLInsert(){
        sqlCreate();

        for (int i = 1; i <= 5; i++) {
            String wInsert = "INSERT INTO warehouses(name,capacity) VALUES (\"warehouse" + i + "\", \"" + i * 100 + "\");";
            System.out.println(wInsert);
            boolean result = executor.insert(wInsert);
            assert (result);
        }
    }

    @Test
    public void testSQLQuery() {
        sqlCreate();

        // insert data
        for (int i = 1; i <= 5; i++) {
            String wInsert = "INSERT INTO warehouses(name,capacity) VALUES (\"warehouse" + i + "\", \"" + i * 100 + "\");";
            System.out.println(wInsert);
            boolean result = executor.insert(wInsert);
            assert (result);
        }

        String querySql = "SELECT name, capacity FROM warehouses;";
        List<String> rs = executor.query(querySql);
        assert(rs != null);
        assert(rs.get(0).equals("name"+"\t|\t"+"capacity"));
        for (int i = 1; i <= 5 ; i++) {
            String assertResult = "warehouse" + i + "\t|\t" + i*100;
            assert(rs.get(i).equals(assertResult));
        }
    }

    @Test
    public void testSQLUpdate() {
        sqlCreate();

        // insert data
        for (int i = 1; i <= 5; i++) {
            String wInsert = "INSERT INTO warehouses(name,capacity) VALUES (\"warehouse" + i + "\", \"" + i * 100 + "\");";
            System.out.println(wInsert);
            boolean result = executor.insert(wInsert);
            assert (result);
        }

        String updateSQL = "UPDATE warehouses SET name='newWarehouse2' WHERE name='warehouse2'";
        boolean result = executor.update(updateSQL);
        assert(result);
        String querySql = "SELECT name, capacity FROM warehouses;";
        List<String> rs = executor.query(querySql);
        assert(rs.get(2).equals("newWarehouse2" + "\t|\t" + 200));
    }

    @Test
    public void testSQLDelete() {
        sqlCreate();
        // insert data
        for (int i = 1; i <= 5; i++) {
            String wInsert = "INSERT INTO warehouses(name,capacity) VALUES (\"warehouse" + i + "\", \"" + i * 100 + "\");";
            System.out.println(wInsert);
            boolean result = executor.insert(wInsert);
            assert (result);
        }

        String deleteSQL = "DELETE FROM warehouses WHERE name='warehouse2'";
        boolean result = executor.delete(deleteSQL);
        assert(result);
        String querySql = "SELECT name, capacity FROM warehouses;";
        List<String> rs = executor.query(querySql);
        assert(!rs.contains("warehouse2" + "\t|\t" + 200));
    }


    @Test
    public void testSQLJoins() {
        sqlCreate();

        // insert data into warehouses
        for (int i = 1; i <= 5; i++) {
            String wInsert = "INSERT INTO warehouses(name,capacity) VALUES (\"warehouse" + i + "\", \"" + i * 100 + "\");";
            System.out.println(wInsert);
            boolean result = executor.insert(wInsert);
            assert (result);
        }

        // create amazon table
        String amznSQL = "CREATE TABLE IF NOT EXISTS amznWarehouses ("
                + "	id integer PRIMARY KEY,"
                + "	name text NOT NULL,"
                + "	capacity integer"
                + ");";
        boolean result = executor.create(amznSQL);
        assert(result);

        // insert data into amznWarehouses
        for (int i = 1; i <= 5; i++) {
            String amznInsert = "INSERT INTO amznWarehouses(name,capacity) VALUES (\"amzn" + i + "\", \"" + i*100 + "\");";
            System.out.println(amznInsert);
            result = executor.insert(amznInsert);
            assert(result);
        }

        // executing JOIN statement
        String querySql = "SELECT warehouses.name, amznWarehouses.name FROM warehouses LEFT JOIN amznWarehouses ON warehouses.capacity=amznWarehouses.capacity;";
        List<String> resultList = executor.query(querySql);
        for (int i = 1 ; i <=5; i++) {
            assert(resultList.contains("warehouse" + i + "\t|\t" + "amzn" + i));
        }
    }
}
