package app_kvServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class KVServer implements Runnable {
    private static final Logger logger = LogSetup.getLogger();
    private final int           port;
    
    private final KVDataStorage data_storage;
    private final ServerSocket  server_socket;
    private volatile boolean    online;
    
    public KVServer(int port) throws IOException {
        this.port = port;
        this.online = false;
        
        logger.info("Initializing server ...");
        this.server_socket = new ServerSocket(this.port);
        
        this.data_storage = new KVDataStorage();
        
        logger.info("Server listening on port: " + this.server_socket.getLocalPort());
        this.online = true;
    }
    
    @Override
    public void run() {
        while (this.online) {
            try {
                Socket client = server_socket.accept();
                ClientConnection connection = new ClientConnection(client, data_storage);
                new Thread(connection).start();
                
                logger.info("New connection from " + client.getInetAddress().getHostName() +
                            " from port " + client.getPort() + ".");
            } catch (IOException e) {
                logger.error("Error! Unable to establish connection. \n", e);
            }
        }
        logger.info("Server stopped.");
    }
    
    public static void main(String[] args) {
        
        // Parse command line arguments
        boolean print_usage = false;
        Level   log_level = Level.WARN;
        int     port = -1;
        
        for (int i = 0; i < args.length; ++i) {
            String opt = args[i];
            
            if (opt.startsWith("-")) {
                if (opt.equalsIgnoreCase("-h")) {
                    print_usage = true;
                    break;
                } else if (opt.equalsIgnoreCase("-l")) {
                    if (++i < args.length && LogSetup.isValidLevel(args[i])) {
                        log_level = Level.toLevel(args[i]);
                    } else {
                        System.out.println("Error! Option -l requires a log level as an argument.");
                        print_usage = true;
                        break;
                    }
                }
            } else {
                port = Integer.parseInt(args[i]);
                break;
            }
        }
        if (port == -1) {
            System.out.println("Error! Port number is not provided.\n");
            print_usage = true;
        }
        if (print_usage) {
            System.out.println("Usage: KVServer [-l log_level] <port>");
            System.out.println("    -l log_level    - Set logging level (default: WARN).");
            System.out.println("    port            - Port number for listening for connections.");
            System.exit(1);
        }
        
        // Initialize logger
        try {
            LogSetup.initialize("logs/server/server.log", log_level);
        } catch (IOException e) {
            System.out.println("Error! Unable to initialize logger: " + e.getMessage());
            System.exit(1);
        }
        
        // Start server
        try {
            KVServer server = new KVServer(port);
            new Thread(server).start();
            
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
            while (server.online) {
                String user_query = in.readLine();
                
                if (user_query.equalsIgnoreCase("quit")) {
                    server.online = false;
                    System.exit(1);
                } else if (user_query.equalsIgnoreCase("dump")) {
                    System.out.println(server.data_storage.dump());
                }
            }
            
        } catch (IOException e) {
            logger.error("Error! Cannot start server: " + e.getMessage());
        }
    }
}
