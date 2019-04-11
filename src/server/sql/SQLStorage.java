package server.sql;

import org.apache.log4j.Logger;
import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.SQLException;

import java.io.File;


public class SQLStorage {
    private static Logger logger = Logger.getRootLogger();
    private final static String extension = ".db";
    private String dataFilePath;
    private String dataFileName;

    public SQLStorage(String name) {
        dataFileName = name + extension;
        dataFilePath = new File(System.getProperty("user.dir"), dataFileName).toString();
        connect();
    }

    public Connection connect() {
        // SQLite connection string
        String url = "jdbc:sqlite:" + dataFilePath;
        logger.debug("SQL Data file path " + url);

        Connection conn;
        try {
            conn = DriverManager.getConnection(url);
        } catch (SQLException e) {
            logger.error("Error while creating JDBC connection to " + url);
            e.printStackTrace();
            return null;
        }
        return conn;
    }

    public void clearSQLStorage() {
        logger.debug("deleting storage data file " + dataFilePath + "...");
        File storageFile = new File(dataFilePath);
        if (storageFile.exists()) {
            storageFile.delete();
            logger.debug("storage data file " + dataFilePath + " deleted");
        }
        assert(!storageFile.exists());
    }
}
