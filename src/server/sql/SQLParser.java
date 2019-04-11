package server.sql;

import org.apache.log4j.Logger;
import shared.messages.KVMessage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SQLParser {
    private static Logger logger = Logger.getRootLogger();

    public static List<String> getTableFromSQLMsg(KVMessage msg) {
        List<String> statementArr = new ArrayList<>(Arrays.asList(msg.getKey().split("\\s+")));
        String actionType = statementArr.get(0);
        int tableIndex = -1;
        int joinTableIndex = -1;

        switch (actionType) {
            // query
            case "select":
            case "SELECT":
                if (msg.getKey().contains("JOIN") || msg.getKey().contains("join")) {
                    for (int i = 1 ; i < statementArr.size(); i++) {
                        if (statementArr.get(i).equals("JOIN") || statementArr.get(i).equals("join")) {
                            joinTableIndex = i+1;
                            break;
                        }
                    }
                }
                for (int i = 1; i < statementArr.size(); i++) {
                    if (statementArr.get(i).equals("FROM") || statementArr.get(i).equals("from")) {
                        tableIndex = i+1;
                        break;
                    }
                }
                break;
            case "insert":
            case "INSERT":
                for(int i = 1; i < statementArr.size(); i++) {
                    if (statementArr.get(i).equals("INTO") || statementArr.get(i).equals("into")) {
                        tableIndex = i+1;
                        break;
                    }
                }
                break;
            case "create":
            case "CREATE":
                if (msg.getKey().contains(" IF NOT EXISTS ") || msg.getKey().contains(" if not exists")) {
                    for(int i = 1; i < statementArr.size(); i++) {
                        if (statementArr.get(i).equals("EXISTS") || statementArr.get(i).equals("exists")) {
                            tableIndex = i+1;
                            break;
                        }
                    }
                } else {
                    for(int i = 1; i < statementArr.size(); i++) {
                        if (statementArr.get(i).equals("TABLE") || statementArr.get(i).equals("table")) {
                            tableIndex = i+1;
                            break;
                        }
                    }
                }
                break;

            case "update":
            case "UPDATE":
                tableIndex = 1;
                break;

            case "drop":
            case "DROP":
                if (msg.getKey().contains(" IF EXISTS ") || msg.getKey().contains(" if exists")) {
                    for(int i = 1; i < statementArr.size(); i++) {
                        if (statementArr.get(i).equals("EXISTS") || statementArr.get(i).equals("exists")) {
                            tableIndex = i+1;
                            break;
                        }
                    }
                } else {
                    for(int i = 1; i < statementArr.size(); i++) {
                        if (statementArr.get(i).equals("TABLE") || statementArr.get(i).equals("table")) {
                            tableIndex = i+1;
                            break;
                        }
                    }
                }
                break;

            case "delete":
            case "DELETE":
                for(int i = 1; i < statementArr.size(); i++) {
                    if (statementArr.get(i).equals("FROM") || statementArr.get(i).equals("from")) {
                        tableIndex = i+1;
                        break;
                    }
                }
                break;
        }
        logger.debug("SQL Statement table is " + statementArr.get(tableIndex));
        List<String> tableNames = new ArrayList<>();
        if (tableIndex != -1) tableNames.add(statementArr.get(tableIndex));
        if (joinTableIndex != -1) tableNames.add(statementArr.get(joinTableIndex));
        return tableNames;
    }
}
