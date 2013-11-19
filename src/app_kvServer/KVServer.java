package app_kvServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import logger.LogSetup;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;

/**
 * Main class for server application.
 * @author Danila Klimenko
 */
public class KVServer implements Runnable {
    private static final Logger logger = LogSetup.getLogger();
    private final int           port;
    
    private final KVDataStorage         data_storage;
    private final ServerSocket          server_socket;
    private final Set<ClientConnection> clients;
    private volatile boolean            online;
    
    /**
     * Constructor taking port number as its only argument
     * @param port Port number
     * @throws IOException Thrown if server socket cannot be created
     */
    public KVServer(int port) throws IOException {
        this.port = port;
        this.online = false;
        
        logger.info("Initializing server ...");
        this.server_socket = new ServerSocket(this.port);
        
        this.data_storage = new KVDataStorage();
        this.clients = new HashSet<ClientConnection>();
        
        logger.info("Server listening on port: " + this.server_socket.getLocalPort());
        this.online = true;
    }
    
    /**
     * Override for run() method from Runnable interface
     */
    @Override
    public void run() {
        while (this.online) {
            try {
                Socket client = server_socket.accept();
                ClientConnection connection = new ClientConnection(client, this);
                new Thread(connection).start();
                synchronized (this.clients) {
                    this.clients.add(connection);
                }
                
                logger.info("New connection from " + client.getInetAddress().getHostName() +
                            " from port " + client.getPort() + ".");
            } catch (IOException e) {
                if (this.online) {
                    logger.error("Error! Unable to establish connection: " + e.getMessage());
                }
            }
        }
        logger.info("Server stopped.");
    }
    
    /**
     * Returns the key-value storage used by the server
     * @return Key-value map
     */
    public KVDataStorage getDataStorage() {
        return this.data_storage;
    }
    
    /**
     * A callback function triggered by a client thread prior to its termination
     * @param client The client connection which is about to close
     */
    public void clientTerminated(ClientConnection client) {
        synchronized (this.clients) {
            this.clients.remove(client);
        }
    }
    
    /**
     * Shuts down the server and frees all corresponding resources. Also terminates
     * all client connection which are still active.
     */
    public void shutDown() {
        this.online = false;
        
        synchronized (this.clients) {
            for (ClientConnection client : this.clients) {
                client.closeConnection();
            }
            this.clients.clear();
        }
        
        if (!this.server_socket.isClosed()) {
            try {
                this.server_socket.close();
            } catch (IOException e) {
                logger.error("Error! Unable to close server socket: " + e.getMessage());
            }
        }
    }
    
    /**
     * The server's main() method.
     * @param args Array of command line arguments
     */
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
                } else if (opt.startsWith("-l")) {
                    String arg = (opt.length() > 2) ? opt.substring(2) :
                                    ((i + 1) < args.length) ? args[++i] : null;
                    
                    if (arg != null && LogSetup.isValidLevel(arg)) {
                        log_level = Level.toLevel(arg);
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
                    server.shutDown();
                } else if (user_query.equalsIgnoreCase("dump")) {
                    System.out.println(server.data_storage.dump());
                }
            }
            
        } catch (IOException e) {
            logger.error("Error! Cannot start server: " + e.getMessage());
        }
    }
}
