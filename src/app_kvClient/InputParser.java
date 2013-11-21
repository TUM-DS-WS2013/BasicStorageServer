package app_kvClient;

/**
 * Custom user input parser. Supports input verification, command syntax 
 * checking, command arguments parsing and help text management.
 * @author Danila Klimenko
 */
public final class InputParser {
    /**
     * Maximal allowed length of the input
     */
    private static final int            MAX_INPUT_LENGTH = 128 * 1024 - 1; // 128 KB
    /**
     * The array of valid commands signatures
     */
    private static final ValidCommand[] VALID_COMMANDS = {
        new ValidCommand("connect",
                "connect <server> <port>    - Connect to the echo server with "
                        + "network address <server> on port <port>.",
                2),
        new ValidCommand("disconnect",
                "disconnect                 - Disconnect from the echo server"),
        new ValidCommand("logLevel",
                "logLevel <level>           - Change logging level to <level>.",
                1),
        new ValidCommand("put",
                "put <key> <value>          - Send a 'put' request to the server "
                        + "with the provided 'key', 'value' pair.",
                1, 1, true),
        new ValidCommand("get",
                "get <key>                  - Send a 'get' request to the server "
                        + "with the provided 'key'.",
                1),
        new ValidCommand("help",
                "help [command]             - Print general help text or help "
                        + "for a specified [command].",
                0, 1),
        new ValidCommand("quit",
                "quit                       - Exit the application.")
    };
    
    /**
     * Prints help text for command line interface.
     * @param commandString Name of a command to print help for. May be null,
     *      in which case a general help text will be printed.
     */
    private static void printHelpMessage(String commandString) {
        ValidCommand    command = null;
        
        if (commandString != null) {
            command = findCommandByName(commandString);
        }
        
        if (command != null) {
            System.out.println(command.helpText);
        } else {
            if (commandString != null) {
                System.out.println("Error! Command \""+ commandString + 
                        "\" is not valid!");
            }
            System.out.println("    USAGE");
            for (ValidCommand validCommand : VALID_COMMANDS) {
                System.out.println(validCommand.helpText);
            }
        }
        
    }
    
    /**
     * Parses a string provided by user and validates it against a set of 
     * known commands and their signatures.
     * @param input The input provided by a user.
     * @return If the input is valid returns an array of "String" objects, 
     *      otherwise return null. The first element of the array contains
     *      the command name, the other contains the arguments. 
     */
    public static String[] parseUserInput(String input) {
        // Check is the input string itself is valid
        if ((input == null) || (input.length() == 0)) {
            return null;
        } else if (input.length() >= MAX_INPUT_LENGTH) {
            System.out.println("Error! Input size is too big (max is 128KB).");
            return null;
        }
        
        // Split the input into tokens
        input = input.trim();
        String          commandString = input.split("\\s+", 2)[0];
        ValidCommand    command = findCommandByName(commandString);
        
        // If command is not found print error message and return
        if (command == null) {
            System.out.println("Error! Command \"" + commandString + "\" is not"
                    + " recognized. Type \"help\" go get list of acceptable"
                    + " commands.");
            return null;
        }
        
        // Split the input into command and arguments
        String  tokens[] = input.split("\\s+", 1 + command.argCount
                                       + command.optArgCount);
        
        // If the command equals "help" then print help text and return nothing
        if (command.name.equals("help")) {
            printHelpMessage(tokens.length > 1 ? tokens[1] : null);
            return null;
        }
        
        // Check the number of arguments
        if (tokens.length < (1 + command.argCount)) {
            System.out.println("Error! Too few arguments for the \"" + 
                    commandString + "\" command.");
            return null;
        }
        
        // Check if the last argument has multiple words
        if (tokens[tokens.length - 1].matches("\\S+\\s+.*") && 
                !command.greedyLastArg) {
            System.out.println("Error! Too many arguments for the \"" + 
                    commandString + "\" command.");
            return null;
        }
        
        // An ugly workaround for put and get commands
        if (command.name.equals("put") || command.name.equals("get")) {
            if (!verifyKeyValue(tokens[1],
                                (tokens.length >= 3) ? tokens[2] : null)) {
                return null;
            }
        }
        
        return tokens;
    }
    
    /**
     * Checks whether the provided key and value are too long
     * @param key The key string
     * @param value The value string
     * @return True if key and value are of acceptable length
     */
    private static Boolean verifyKeyValue(String key, String value) {
        final int MAX_KEY_LENGTH = 20;
        final int MAX_VAL_LENGTH = 120 * 1024;
        
        if (key.length() > MAX_KEY_LENGTH) {
            System.out.println("Error! Key is too long.");
            return false;
        }
        if (value != null && value.length() > MAX_VAL_LENGTH) {
            System.out.println("Error! Value is too long.");
            return false;
        }
        
        return true;
    }
    
    /**
     * Tries to find the command signature given the command name
     * @param commandName Name of the command to find
     * @return Signature of a found command (ValidCommand object) or null.
     */
    private static ValidCommand findCommandByName(String commandName) {
        ValidCommand    command = null;
        
        for (ValidCommand validCommand : VALID_COMMANDS) {
            if (commandName.equalsIgnoreCase(validCommand.name)) {
                command = validCommand;
                break;
            }
        }
        
        return command;
    }
    
    /**
     * Private constructor restricts instantiation of the class
     */
    private InputParser() {}
    
    /**
     * Private class representing a valid command and its signature.
     */
    private static class ValidCommand {
        public String name;
        public String helpText;
        public int argCount;
        public int optArgCount;
        public boolean greedyLastArg;

        // Pack of constructors with reduced argument sets
        ValidCommand(String name, String helpText) {
            this(name, helpText, 0, 0, false);
        }
        
        ValidCommand(String name, String helpText, int argCount) {
            this(name, helpText, argCount, 0, false);
        }
        
        ValidCommand(String name, String helpText, int argCount, 
                int optArgCount) {
            this(name, helpText, argCount, optArgCount, false);
        }
        
        /**
         * The full constructor for the ValidCommand class.
         * Other constructors imitate construction with default parameters
         * @param name          Name of the command.
         * @param helpText      Description of the command for the help text.
         * @param argCount      Number of mandatory arguments
         * @param optArgCount   Number of optional arguments
         * @param greedyLastArg True if last arguments may contain more than one
         *                      word.
         */
        ValidCommand(String name, String helpText, int argCount, 
                int optArgCount, boolean greedyLastArg) {
            this.name = name;
            this.helpText = helpText;
            this.argCount = argCount;
            this.optArgCount = optArgCount;
            this.greedyLastArg = greedyLastArg;
        }
    }
}