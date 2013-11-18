package app_kvClient;

import client.KVCommInterface;
import client.KVStore;
import logger.LogSetup;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import org.apache.log4j.Level;
import CustomExceptions.ServerConnectionException;
import common.messages.KVMessage;
import java.io.IOException;
import java.net.UnknownHostException;

public class KVClient {
    
    private KVCommInterface objKVStoreClient;
    private final LogSetup logger;
    
    public KVClient() throws IOException
    {
        objKVStoreClient = null;
        logger = new LogSetup("c:\\", Level.DEBUG);
    }
    
    /**
     * Main program method
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        String userInput;
        try {
            KVClient client = new KVClient();
            while (true) {
                System.out.print("EchoClient> ");
                userInput = in.readLine();
                String[] strInputTokens = InputParser.parseUserInput(userInput);
                if (strInputTokens == null)
                    continue;
                String outPut = client.ProcessMessages(strInputTokens);
                System.out.print("EchoClient> " + outPut + "\n");
                if (strInputTokens[0].equalsIgnoreCase("quit"))
                    break;
            }
        }
        catch(IOException exception) {
            System.out.println(exception.getMessage());
        }
    }
    
    /**
     * It processes the messages and routes the command to their respective modules.
     * @param inputMessage array of string with the commands and parameters.
     * @return information String 
     */
    public String ProcessMessages(String[] inputMessage)
    {
        String output = new String();
        if (inputMessage.length < 1 || objKVStoreClient == null)
            return output;
        if (!(inputMessage[0].compareToIgnoreCase("connect") == 0 && objKVStoreClient == null))
            return output;
        try
        {
            if (inputMessage[0].compareToIgnoreCase("connect") == 0)
            {
                objKVStoreClient = new KVStore(inputMessage[1], Integer.parseInt(inputMessage[2]));
                objKVStoreClient.connect();
                output = "Connection Established!";
            }
            else if (inputMessage[0].compareToIgnoreCase("disconnect") == 0)
            {
                objKVStoreClient.disconnect();
                output = "Connection terminated!";
            }
            else if (inputMessage[0].compareToIgnoreCase("send") == 0)
            {
                output = objKVStoreClient.SendRecvMessage(inputMessage[1]);
            }
            else if (inputMessage[0].compareToIgnoreCase("logLevel") == 0)
            {
                output = logger.setLogLevel(inputMessage[1]);
            }
            else if (inputMessage[0].compareToIgnoreCase("quit") == 0)
            {
                objKVStoreClient.disconnect();
                output = "Application exit!";
            }
            else if (inputMessage[0].compareToIgnoreCase("put") == 0)
            {
                KVMessage message = objKVStoreClient.put(inputMessage[1], inputMessage[2]);
                output = "---";
            }
            else if (inputMessage[0].compareToIgnoreCase("get") == 0)
            {
                KVMessage message = objKVStoreClient.get(inputMessage[1]);
                output = "---";
            }
            
            logger.info(output);
        }
        catch (ServerConnectionException serverNotConnectedEx)
        {
            output = serverNotConnectedEx.GetErrorMsg();
            logger.error(output);
        }
        catch (UnknownHostException hostException)
        {
            output = "Error! See the log file for details";
            logger.error(hostException.getMessage());
        }
        catch(IOException ioexception)
        {
            output = "Error! See the log file for details";
            logger.error(ioexception.getMessage());
        }
        catch(Exception exception)
        {
            output = "Error! See the log file for details";
            logger.error(exception.getMessage());
        }
        
        return output;
    }
}
