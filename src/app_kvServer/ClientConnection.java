package app_kvServer;

import common.messages.KVMessage;
import common.messages.KVMessage.StatusType;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.text.ParseException;
import logger.LogSetup;
import org.apache.log4j.Logger;

/**
 *
 * @author Danila KLimenko
 */
public class ClientConnection implements Runnable {
    private static final Logger logger = LogSetup.getLogger();
    
    private final Socket        client_socket;
    private final KVDataStorage data_storage;
    private boolean             online;
    private InputStream         input;
    private OutputStream        output;

    public ClientConnection(Socket clientSocket, KVDataStorage data_storage) {
        this.client_socket = clientSocket;
        this.data_storage = data_storage;
        this.online = true;
    }
    
    @Override
    public void run() {
        try {
            output = client_socket.getOutputStream();
            input = client_socket.getInputStream();

            // Send connection acknowledgement
            String conn_ack = "Connected to server '" + client_socket.getLocalAddress() +
                            "' on port '" + client_socket.getLocalPort() + "'. You are '" +
                            client_socket.getInetAddress() + "'.";
            new NetworkMessage(conn_ack.getBytes()).writeTo(output);

            while (this.online) {
                try {
                    // Receive client's query
                    NetworkMessage  netmsg = NetworkMessage.readFrom(input);
                    KVMessage       kvmsg, kvmsg_reply;
                    
                    try {
                        kvmsg = KVMessageRaw.unmarshal(netmsg.getData());
                        
                        // Process query
                        kvmsg_reply = this.parseKVMessage(kvmsg);
                        
                    } catch (ParseException e) {
                        String report = "Warning! Received KVMessage is invalid: " + e.getMessage();
                        logger.warn(report);
                        new NetworkMessage(report.getBytes()).writeTo(output);
                        continue;
                    }
                    
                    // Send reply
                    netmsg = new NetworkMessage(KVMessageRaw.marshal(kvmsg_reply));
                    netmsg.writeTo(output);
                    
                } catch (IOException e) {
                    logger.error("Error! Connection lost: " + e.getMessage());
                    this.online = false;
                }
            }

        } catch (IOException e) {
            logger.error("Error! Connection could not be established!", e);

        } finally {

            try {
                if (client_socket != null) {
                    input.close();
                    output.close();
                    client_socket.close();
                }
            } catch (IOException e) {
                logger.error("Error! Unable to tear down connection!", e);
            }
        }
    }
    
    private KVMessage parseKVMessage(KVMessage kvmsg) throws ParseException {
        StatusType  type = kvmsg.getStatus();
        String      key = kvmsg.getKey();
        String      value = kvmsg.getValue();
        
        StatusType  return_type = null;
        String      return_value = null;
        
        switch (type) {
            case PUT:
                if (value != null) { // Performing put operation
                    try {
                        return_value = this.data_storage.put(key, value);
                        return_type = (return_value == null) ?
                                        StatusType.PUT_SUCCESS : StatusType.PUT_UPDATE;
                        return_value = value; // Return the value form the client query
                        
                    } catch (IllegalArgumentException e) {
                        return_type = StatusType.PUT_ERROR;
                        return_value = e.getMessage();
                    }
                    
                } else { // Performing delete operation
                    return_value = this.data_storage.delete(key);
                    if (return_value == null) {
                        return_type = StatusType.DELETE_ERROR;
                        return_value = "Requested key is not found or invalid.";
                    } else {
                        return_type = StatusType.DELETE_SUCCESS;
                    }
                }
                break;
                
            case GET:
                return_value = this.data_storage.get(key);
                if (return_value == null) {
                    return_type = StatusType.GET_ERROR;
                    return_value = "Requested key is not found or invalid.";
                } else {
                    return_type = StatusType.GET_SUCCESS;
                }
                break;
                
            default:
                throw new ParseException("Message type '" + type + "' is not a valid request.", 0);
        }
        
        return new KVMessageRaw(return_type, key, return_value);
    }
}
