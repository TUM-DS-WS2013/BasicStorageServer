package CustomExceptions;

/**
 * Custom exception describing problems with connection to server
 */
public class ServerConnectionException extends Exception{
  
    /**
     * Parameterless Constructor
     */
    public ServerConnectionException() {}

    /**
     * Constructor that accepts a message
     * @param message The message describing the exception
     */
    public ServerConnectionException(String message) {
        super(message);
    }

    /**
     * Get the text message from the exception
     * @return The string containing the description of exception
     */
    public String GetErrorMsg() {
        return super.getMessage();
    }
}
