package app_kvServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.text.ParseException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
        Level   log_level = Level.WARN;
        Integer port = null;
        
        // Parse command line arguments
        try {
            ArgumentParser parser = new ArgumentParser("hl:", args);
            ArgumentParser.Option option;
            
            while ((option = parser.getNextArgument()) != null) {
                if (option.name == null) { // Positional argument go here
                    if (port == null) {
                        try {
                            port = Integer.parseInt(option.argument);
                        } catch (NumberFormatException e) {}
                        if (port == null || port < 0 || port > 65535) {
                            throw new ParseException("Invalid port number: " + option.argument + ".", 0);
                        }
                    } else {
                        throw new ParseException("Excess positional argument: " + option.argument + ".", 0);
                    }
                    
                } else if (option.name.equals("h")) {
                    printUsage();
                    System.exit(1);
                    
                } else if (option.name.equals("l")) {
                    if (LogSetup.isValidLevel(option.argument)) {
                        log_level = Level.toLevel(option.argument);
                    } else {
                        throw new ParseException("Invalid logging level: " + option.argument + ".", 0);
                    }
                }
            }
            
            if (port == null) {
                throw new ParseException("Port number is not provided.", 0);
            }
            
        } catch (ParseException e) {
            System.out.println("Error parsing command line arguments: " + e.getMessage());
            printUsage();
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
            
            BufferedReader  input_reader = new BufferedReader(new InputStreamReader(System.in));
            String          user_query;
            
            while (server.online) {
                System.out.print("> ");
                user_query = input_reader.readLine().trim();
                
                if (user_query.equalsIgnoreCase("quit")) {
                    server.shutDown();
                } else if (user_query.equalsIgnoreCase("dump")) {
                    System.out.println(server.data_storage.dump());
                } else if (user_query.startsWith("log")) {
                    String tokens[] = user_query.split("\\s+");
                    if (tokens.length == 2 && LogSetup.isValidLevel(tokens[1])) {
                        LogSetup.setLogLevel(tokens[1]);
                    } else {
                        System.out.println("Error! Bad command format: '" + user_query + "'.");
                    }
                } else if (user_query.equalsIgnoreCase("help")) {
                    System.out.println("ACCEPTABLE COMMANDS:\n"
                            + "    quit          - Stop server and quit application.\n"
                            + "    dump          - Print the data stored on the server.\n"
                            + "    log <level>   - Change the logging level to <level>.\n"
                            + "    help          - Print this help text.");
                } else if (!user_query.isEmpty()) {
                    System.out.println("Error! Invalid command: '" + user_query + "'. "
                            + "Type 'help' for list of supported commands.");
                }
            }
            
        } catch (IOException e) {
            logger.error("Error! Cannot start server: " + e.getMessage());
        }
    }
    
    /**
     * Print help text
     */
    private static void printUsage() {
        System.out.println(
                  "Usage: KVServer [-l log_level] <port>\n"
                + "    -l log_level    - Set logging level (default: WARN).\n"
                + "    <port>          - Port number for listening for connections."
        );
    }
    
    /**
     * Private class responsible for command line arguments parsing
     */
    private static class ArgumentParser {
        private final String                format;
        private final Map<String, Boolean>  optArgs;
        private final String[]              args;
        private final int                   count;
        private int                         offset;
        
        /**
         * Main constructor
         * @param format String describing acceptable options (a simplified version
         *              of POSIX "getopt()" format)
         * @param args Array of command line arguments
         * @throws ParseException Thrown if format has inconsistent syntax
         */
        public ArgumentParser(String format, String[] args) throws ParseException {
            this.format = format;
            this.optArgs = new HashMap<String, Boolean>();
            this.args = args;
            this.count = args.length;
            this.offset = 0;
            
            this.parseFormat();
        }
        
        /**
         * Parses the format string and generates a map of valid options
         * @throws ParseException Thrown if format has inconsistent syntax
         */
        private void parseFormat() throws ParseException {
            Pattern syntax = Pattern.compile("([a-zA-Z0-9][:]?)*");
            
            if (!syntax.matcher(this.format).matches()) {
                throw new ParseException("Illegal symbols in format string.", 0);
            }
            
            int i = 0;
            while (i < this.format.length()) {
                String  opt = this.format.substring(i, i + 1);
                Boolean hasArg = false;
                
                if ((++i < this.format.length()) && (this.format.charAt(i) == ':')) {
                    hasArg = true;
                    ++i;
                }
                
                this.optArgs.put(opt, hasArg);
            }
        }
        
        /**
         * Reenterable function which parses command line arguments and return
         *  next valid option.
         * @return An `ArgumentParser.Option` instance containing the option and
         *          its argument. Either name or argument of an option may be null
         *          (parameterless option and positional argument, respectively).
         * @throws ParseException Thrown if an invalid option is encountered or
         *          if an option misses an argument
         */
        public Option getNextArgument() throws ParseException {
            if (offset >= count) {
                return null;
            }
            
            String  opt = this.args[offset];
            String  optName;
            String  optArgument;
            
            Pattern syntax = Pattern.compile("\\-([a-zA-Z0-9])([\\S]*)");
            Matcher syntax_matcher = syntax.matcher(opt);
            
            if (syntax_matcher.matches()) {
                optName = syntax_matcher.group(1);
                optArgument = syntax_matcher.group(2);
                if (optArgument.length() == 0) {
                    optArgument = null;
                }
            } else {
                optName = null;
                optArgument = opt;
            }
            
            if (optName != null) {
                if (!this.optArgs.containsKey(optName)) {
                    throw new ParseException("Option '" + optName + "' is not supported.", 0);
                }
                if (this.optArgs.get(optName) && (optArgument == null)) {
                    if (++offset >= count) {
                        throw new ParseException("Option '" + optName + "' must have an argument.", 0);
                    }
                    optArgument = this.args[offset];
                }
            }
            
            ++offset;
            
            return new Option(optName, optArgument);
        }
        
        /**
         * Restarts argument parsing from the first one
         */
        public void reset() {
            this.offset = 0;
        }
        
        /**
         * A simple subclass for returning the option and its argument
         */
        private class Option {
            public final String name;
            public final String argument;
            
            public Option(String name, String argument) {
                this.name = name;
                this.argument = argument;
            }
        }
    }
}
