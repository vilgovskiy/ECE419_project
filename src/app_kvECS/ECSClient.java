package app_kvECS;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import logger.LogSetup;

import java.io.BufferedReader;
import java.util.List;
import java.util.ArrayList;
import java.util.Arrays;
import java.io.InputStreamReader;
import java.io.IOException;


import ecs.*;

public class ECSClient implements Runnable {
    private Logger logger = Logger.getRootLogger();
    private static final String PROMPT = "> ";

    private BufferedReader stdin;
    private IECSClient ecsClient;
    private boolean stop = false;

    public ECSClient(String configFile) throws IOException {
        this.ecsClient = new ECS(configFile);
    }


    @Override
    public void run(){
        while(!stop) {
            stdin = new BufferedReader(new InputStreamReader(System.in));
            System.out.print(PROMPT);

            try {
                String cmdLine = stdin.readLine();
                this.handleCommand(cmdLine);
            } catch (IOException ioe){
                stop = true;
                printError("CLI does not respond - Application terminated ");
            }
        }
    }

    private void handleCommand(String cmdLine) {
        String[] token = cmdLine.split("\\s+");
        String cmd = token[0];

        try {
            boolean result = false;
            switch (cmd) {
                case "start":
                    result = ecsClient.start();
                    break;
                case "stop":
                    result = ecsClient.stop();
                    break;
                case "shutdown":
                    result = ecsClient.shutdown();
                    break;
                case "addNode":
                    if (token.length == 3) {
                        try {
                            IECSNode node = ecsClient.addNode(token[1], Integer.parseInt(token[2]));
                            if (node == null) { result = false; }
                            else result = true;
                        } catch(NumberFormatException nfe) {
                            printError("Cache Size has to be an integer!");
                            logger.info("Unable to parse cache size", nfe);
                        } catch (IllegalArgumentException iae) {
                          printError("Wrong value for Caching strategy!");
                          logger.info("Unable to parse caching strategy");
                        }
                    } else {
                        printError("Invalid number of parameters!");
                        result = false;
                    }
                    break;
                case "removeNodes":
                    List<String> nodeList = new ArrayList<>(Arrays.asList(token));
                    nodeList.remove(0);
                    result = ecsClient.removeNodes(nodeList);
                    break;
                case "ringInfo":
                    String ringInfo = this.ecsClient.getHashRingInfo();
                    result = true;
                    printStatus(ringInfo);
                    break;

                case "nodesInfo":
                    String nodesInfo = this.ecsClient.getAllNodesInfo();
                    result = true;
                    printStatus(nodesInfo);
                    break;
                case "help":
                    printHelp();
                    result = true;
                    break;
                case "quit":
                    this.stop = true;
                    result = true;
                    break;
            }
            if (result) {
                printStatus("Command executed successfully");
            } else {
                printStatus("Command failed to execute");
            }
        } catch(Exception e) {
            printError("Unknown Error has occurred");
            e.printStackTrace();
        }
    }

    private void printError(String error){
        System.out.println(PROMPT + "Error! " +  error);
    }

    private void printStatus(String status){
        System.out.println(status);
    }

    private void printHelp() {
        StringBuilder sb = new StringBuilder();
        sb.append(PROMPT).append("ECS CLIENT HELP (Usage):\n");
        sb.append(PROMPT);
        sb.append("::::::::::::::::::::::::::::::::\n");
        sb.append(PROMPT).append("start | stop | shutdown | addNode | removeNodes | quit");
        System.out.println(sb.toString());
    }


    public static void main(String[] args) {
        try {
            new LogSetup("logs/client.log", Level.INFO);
            ECSClient cli = new ECSClient(args[0]);
            cli.run();
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger!");
            e.printStackTrace();
            System.exit(1);
        }
    }
}
