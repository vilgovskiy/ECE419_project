package testing;

import logger.LogSetup;
import org.apache.log4j.Level;
import org.junit.Test;

import junit.framework.TestCase;
import server.sql.*;

import java.io.IOException;
import java.sql.Connection;
import java.util.*;

public class SQLStorageTest extends TestCase {

    static {
        try {
            new LogSetup("logs/testing/test.log", Level.ALL);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    @Test
    public void testSQLStorage() {
        SQLStorage storage = new SQLStorage("server1");
        SQLExecutor executor = new SQLExecutor(storage);
        Map<String, String> data = new HashMap<>();

        for (int i = 0; i < 10; i ++ ) {
            String name = "name" + i;
            String capacity = Integer.toString(i);
            data.put(name, capacity);
        }

        Connection conn = storage.connect();
        assert(conn != null);

        String createSql = "CREATE TABLE IF NOT EXISTS warehouses ("
                + "	id integer PRIMARY KEY,"
                + "	name text NOT NULL,"
                + "	capacity integer"
                + ");";
        boolean result = executor.create(createSql);
        assert(result);


        for (Map.Entry<String, String> entry : data.entrySet()) {
            String sql = "INSERT INTO warehouses (name,capacity) VALUES (\"" + entry.getKey() + "\",\"" + entry.getValue()+"\")";
            System.out.println(sql);
            result = executor.insert(sql);
            assert(result);
        }

        String querySql = "SELECT id, name, capacity FROM warehouses";
        List<String> resultList = executor.query(querySql);
        assert(resultList != null);
        for (String rs : resultList) {
            System.out.println(rs);
        }
    }
}
