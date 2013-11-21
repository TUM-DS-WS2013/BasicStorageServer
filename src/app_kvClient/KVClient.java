package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import common.messages.KVMessage;
import java.io.IOException;
import java.net.UnknownHostException;

public class KVClient {

    private KVCommInterface objKVStoreClient;
    private final Logger logger;

    public KVClient() throws IOException {
        objKVStoreClient = null;
        logger = LogSetup.getLogger();
    }

    /**
     * Main program method
     *
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        try {
            LogSetup.initialize("logs/client/client.log", Level.WARN);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }

        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String userInput;
        try {
            KVClient client = new KVClient();
            while (true) {
                System.out.print("EchoClient> ");
                userInput = in.readLine();
                String[] strInputTokens = InputParser.parseUserInput(userInput);
                if (strInputTokens == null) {
                    continue;
                }
                String outPut = client.ProcessMessages(strInputTokens);
                System.out.print("EchoClient> " + outPut + "\n");
                if (strInputTokens[0].equalsIgnoreCase("quit")) {
                    break;
                }
            }
        } catch (IOException exception) {
            System.out.println(exception.getMessage());
        }
    }

    /**
     * It processes the messages and routes the command to their respective
     * modules.
     *
     * @param inputMessage array of string with the commands and parameters.
     * @return information String
     */
    public String ProcessMessages(String[] inputMessage) {
        String output = new String();
        if (inputMessage.length < 1 || (objKVStoreClient == null && !inputMessage[0].equalsIgnoreCase("connect"))) {
            return output = "Error: Connection not established!";
        }
        try {
            if (inputMessage[0].compareToIgnoreCase("connect") == 0) {
                objKVStoreClient = new KVStore(inputMessage[1], Integer.parseInt(inputMessage[2]));
                objKVStoreClient.connect();
                output = "Connection Established!";
            } else if (inputMessage[0].compareToIgnoreCase("disconnect") == 0) {
                objKVStoreClient.disconnect();
                objKVStoreClient = null;
                output = "Connection terminated!";
            } else if (inputMessage[0].compareToIgnoreCase("logLevel") == 0) {
                output = LogSetup.setLogLevel(inputMessage[1]);
            } else if (inputMessage[0].compareToIgnoreCase("quit") == 0) {
                objKVStoreClient.disconnect();
                objKVStoreClient = null;
                output = "Application exit!";
            } else if (inputMessage[0].compareToIgnoreCase("put") == 0) {
                KVMessage message = objKVStoreClient.put(inputMessage[1], inputMessage.length > 2 ? inputMessage[2]: null);
                if (message.getStatus() == KVMessage.StatusType.DELETE_SUCCESS)
                    output = "Deletion Succeeded: Value was '" + message.getValue() + "'.";
                else if (message.getStatus() == KVMessage.StatusType.PUT_SUCCESS)
                    output = "Value stored successfully. Value is '" + message.getValue() + "'.";
                else if (message.getStatus() == KVMessage.StatusType.PUT_UPDATE)
                    output = "Value updated successfully. Updated value is '" + message.getValue() + "'.";
                else
                    output = "Error Occured: " + message.getValue();
            } else if (inputMessage[0].compareToIgnoreCase("get") == 0) {
                KVMessage message = objKVStoreClient.get(inputMessage[1]);
                if (message.getStatus() == KVMessage.StatusType.GET_SUCCESS)
                    output = "Stored value is " + message.getValue();
                else
                    output = "Error Occured: " + message.getValue();
            }

            logger.info(output);
        }catch (UnknownHostException hostException) {
            output = "Error! See the log file for details";
            logger.error(hostException.getMessage());
        } catch (IOException ioexception) {
            output = "Error! See the log file for details";
            logger.error(ioexception.getMessage());
        } catch (Exception exception) {
            output = "Error! See the log file for details";
            logger.error(exception.getMessage());
        }

        return output;
    }
}
