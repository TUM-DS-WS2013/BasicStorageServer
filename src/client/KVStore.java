package client;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import CustomExceptions.ServerConnectionException;

import common.messages.KVMessage;
import common.messages.KVMessageRaw;
import common.messages.NetworkMessage;
import java.text.ParseException;

public class KVStore implements KVCommInterface {

    private Socket objSocketClient;
    private InputStream objSocketInputStream;
    private OutputStream objSocketOutPutStream;
    private final String strServerAdress;
    private final int nServerPort;

    /**
     * Get the IP address of server.
     *
     * @return the IP address of server to whom socket is connected.
     */
    public String GetServerIP() {
        return strServerAdress;
    }

    /**
     * Get the port number of server.
     *
     * @return port number to whom the socket is connected.
     */
    public int GetServerPort() {
        return nServerPort;
    }

    public KVStore() {
        strServerAdress = "";
        nServerPort = -1;
        objSocketClient = null;
        objSocketInputStream = null;
        objSocketOutPutStream = null;
    }

    /**
     * Initialize KVStore with address and port of KVServer
     *
     * @param address the address of the KVServer
     * @param port the port of the KVServer
     */
    public KVStore(String address, int port) {
        strServerAdress = address;
        nServerPort = port;
    }

    @Override
    public void connect() throws Exception {

        if (objSocketClient == null && !strServerAdress.isEmpty() && nServerPort != -1) {
            objSocketClient = new Socket(strServerAdress, nServerPort);
            objSocketInputStream = objSocketClient.getInputStream();
            objSocketOutPutStream = objSocketClient.getOutputStream();
            RecieveMessage();
        }
    }

    @Override
    public void disconnect() {
        try {
            if (objSocketInputStream != null) {
                objSocketInputStream.close();
            }
            if (objSocketOutPutStream != null) {
                objSocketOutPutStream.close();
            }
            if (objSocketClient != null) {
                objSocketClient.close();
            }
        } catch (IOException ex) {
        }

        objSocketInputStream = null;
        objSocketOutPutStream = null;
        objSocketClient = null;
    }

    @Override
    public KVMessage put(String key, String value) throws Exception {
        KVMessage kvmsg = new KVMessageRaw(KVMessage.StatusType.PUT, key, value);
        
        return this.kvRequest(kvmsg);
    }

    @Override
    public KVMessage get(String key) throws Exception {
        KVMessage   kvmsg = new KVMessageRaw(KVMessage.StatusType.GET, key, null);
        
        return this.kvRequest(kvmsg);
    }
    
    public KVMessage kvRequest(KVMessage kv_out) throws IOException {
        NetworkMessage netmsg = new NetworkMessage(KVMessageRaw.marshal(kv_out));
        netmsg.writeTo(objSocketOutPutStream);
        
        KVMessage kv_in;
        netmsg = NetworkMessage.readFrom(objSocketInputStream);
        try {
            kv_in = KVMessageRaw.unmarshal(netmsg.getData());
            // TODO: remove the next line!
            System.out.println(kv_in.getStatus() + " " + kv_in.getKey() + " " + kv_in.getValue());
        } catch (ParseException e) {
            kv_in = null;
            String error_message = new String(netmsg.getData());
            // TODO: remove the next line!
            System.out.println(error_message);
            throw new IOException(error_message);
        }
        
        return kv_in;
    }

    /**
     * Send the message to the server to whom socket is connected.
     *
     * @param strMessage Message which has to be send to server.
     * @throws IOException
     * @throws ServerConnectionException
     */
    private void SendMessage(String strMessage) throws IOException, ServerConnectionException {
        if (objSocketOutPutStream != null) {
            byte[] msgBytes = strMessage.getBytes();
            objSocketOutPutStream.write(msgBytes);
            objSocketOutPutStream.write(0x0D);
            objSocketOutPutStream.flush();
        } else {
            throw new ServerConnectionException("Error! Not connected!");
        }
    }

    /**
     * Receive the message from the server to whom socket is connected.
     *
     * @return Read message from the server.
     * @throws IOException
     * @throws ServerConnectionException
     */
    private String RecieveMessage() throws IOException, ServerConnectionException {
        String strRecvMsg = new String();

        if (objSocketInputStream != null) {
            byte[] recieveMsgBytes;
            byte[] readBuf1 = new byte[1];
            int readBytes = objSocketInputStream.read(readBuf1);
            if (objSocketInputStream.available() > 0) {
                byte[] readBuf2 = new byte[objSocketInputStream.available()];
                readBytes = objSocketInputStream.read(readBuf2);
                recieveMsgBytes = new byte[readBuf2.length + readBuf1.length];
                System.arraycopy(readBuf1, 0, recieveMsgBytes, 0, 1);
                System.arraycopy(readBuf2, 0, recieveMsgBytes, 1, readBuf2.length);
            } else {
                recieveMsgBytes = new byte[1];
                recieveMsgBytes[0] = readBuf1[0];
            }

            if (readBytes != 0 && readBytes != -1) {
                strRecvMsg = new String(recieveMsgBytes, 0, recieveMsgBytes.length - 2);
            }
        } else {
            throw new ServerConnectionException("Error! Not connected!");
        }

        return strRecvMsg;
    }

    /**
     * Send and receive the message to/from the server respectively.
     *
     * @param strMessage Message which has to be send to server.
     * @return Read message from the server.
     * @throws IOException
     * @throws ServerConnectionException
     */
    @Override
    public String SendRecvMessage(String strMessage) throws IOException, ServerConnectionException {
        SendMessage(strMessage);
        return RecieveMessage();
    }

}
