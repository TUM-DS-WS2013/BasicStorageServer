package logger;

import java.io.IOException;

import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;

/**
 * Represents the initialization for the server logging with Log4J.
 */
public class LogSetup {

	public static final String UNKNOWN_LEVEL = "UnknownLevel";
	private Logger logger = Logger.getRootLogger();
	private String logdir;
	
	/**
	 * Initializes the logging for the echo server. Logs are appended to the 
	 * console output and written into a separated server log file at a given 
	 * destination.
	 * 
	 * @param logdir the destination (i.e. directory + filename) for the 
	 * 		persistent logging information.
	 * @throws IOException if the log destination could not be found.
	 */
	public LogSetup(String logdir, Level level) throws IOException {
		this.logdir = logdir;
		initialize(level);
	}

	private void initialize(Level level) throws IOException {
		PatternLayout layout = new PatternLayout( "%d{ISO8601} %-5p [%t] %c: %m%n" );
		FileAppender fileAppender = new FileAppender( layout, logdir, true );		
	    
	    ConsoleAppender consoleAppender = new ConsoleAppender(layout);
		logger.addAppender(consoleAppender);
		logger.addAppender(fileAppender);
		logger.setLevel(level);
	}
	
	public boolean isValidLevel(String levelString) {
		boolean valid = false;
		
		if(levelString.equals(Level.ALL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.DEBUG.toString())) {
			valid = true;
		} else if(levelString.equals(Level.INFO.toString())) {
			valid = true;
		} else if(levelString.equals(Level.WARN.toString())) {
			valid = true;
		} else if(levelString.equals(Level.ERROR.toString())) {
			valid = true;
		} else if(levelString.equals(Level.FATAL.toString())) {
			valid = true;
		} else if(levelString.equals(Level.OFF.toString())) {
			valid = true;
		}
		
		return valid;
	}
	
        //<editor-fold defaultstate="collapsed" desc="log4j.Logger call wrappers">
        public void debug(Object message) {
            logger.debug(message);
        }
        public void info(Object message) {
            logger.info(message);
        }
        public void warn(Object message) {
            logger.warn(message);
        }
        public void error(Object message) {
            logger.error(message);
        }
        public void fatal(Object message) {
            logger.fatal(message);
        }
    //</editor-fold>
    
        /**
        * Changes logging level
        * @param levelString New logging level name.
        * @return Info string
        */
       public String setLogLevel(String levelString) {
           String resultString = null;
           Level level = null;

           if (levelString.equalsIgnoreCase("all")) {
               level = Level.ALL;
           } else if (levelString.equalsIgnoreCase("debug")) {
               level = Level.DEBUG;
           } else if (levelString.equalsIgnoreCase("info")) {
               level = Level.INFO;
           } else if (levelString.equalsIgnoreCase("warn")) {
               level = Level.WARN;
           } else if (levelString.equalsIgnoreCase("error")) {
               level = Level.ERROR;
           } else if (levelString.equalsIgnoreCase("fatal")) {
               level = Level.FATAL;
           } else if (levelString.equalsIgnoreCase("off")) {
               level = Level.OFF;
           } else {
               resultString = "Error! Logging level \"" + levelString 
                       + "\" is n ot valid.";
           }

           if (level != null) {
               logger.setLevel(level);
               resultString = "Logging level changed to \"" + levelString + "\".";
           }

           return resultString;
       }

	public String getPossibleLogLevels() {
		return "ALL | DEBUG | INFO | WARN | ERROR | FATAL | OFF";
	}
}
