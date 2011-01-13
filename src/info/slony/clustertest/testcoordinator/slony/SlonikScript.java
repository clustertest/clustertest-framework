package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;
import info.slony.clustertest.testcoordinator.Event;
import info.slony.clustertest.testcoordinator.EventSource;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;


/**
 * 
 * A class for running scripts through Slonik as part of a test.
 * 
 * This class handles running slonik scripts through slonik.
 * 
 * It has support for the following:
 * 
 * \li Generating a preamble based on the configuration for the test run from the properties file.
 *     This preamble will consist of a set of slonik defines where we substitite values from the
 *     properties file into the defines config.
 * \li Executing a slonik script passed in.
 * 
 *  \li Passing the output of the slonik process to the coordinator as events.
 *    
 *
 */
public class SlonikScript extends ShellExecScript {
	
	private String name;
	private String preamble;
	private Properties properties;
	private static Logger log = Logger.getLogger(SlonikScript.class);
	private String script;
	
	
	
	
	
	/**
	 * Creates a SlonikScript instance
	 * @param name  THe name of the script.
	 * @param preamble  The script preamble.  This is chunk of text that will be passed to slonik at the start
	 *                  of the script.  Configuration substitution is performed on the preamble.
	 * @param coordinator  The test coordinator instance
	 * @param properties  The configuration properties that will be used for the preamble variable substitution.
	 */
	public SlonikScript(String name, String preamble, Coordinator coordinator, Properties properties) {
		super(coordinator,"slonik " +name);
		this.name=name;
		this.preamble=preamble;		
		this.properties = properties;
	}
	
	/**
	 * Returns the preamble string with parameter substitution from the properties file applied.
	 * 
	 * @return
	 * 
	 * @note This method could be private except that we want unit tests to be able to call it directly
	 */
	 String substitutePreamble() {
		//
		// Scan through the preamble text looking for 
		// $database.db1.host
		// $database.db1.username
		// 
		// etc...
		//
		// If any of those variables (minus the $) match a property name defined in the properties file we 
		// will perform a search + replace on it.
		StringBuilder worksheet=new StringBuilder(preamble);
		int idx = 0;
		while( (idx=worksheet.indexOf("$",idx)) > -1) {
			int end=-1;
			String terminalList[] = {" ", "\n", ";" , "'" , "\"" };
			for(int tcounter=0; tcounter < terminalList.length; tcounter++) {
				int end2 = worksheet.indexOf(terminalList[tcounter],idx);
				if(end2 >= 0 && (end2 < end || end==-1)) {
					end=end2;
				}
			}
			
			
			
			if(end==-1) {
				end=worksheet.length();
			}
			String candidate = worksheet.substring(idx+1,end);
			String replace = properties.getProperty(candidate);
			if(replace != null) {
				worksheet.replace(idx,end,replace);
			}
			idx++;
		}
		return worksheet.toString();
	}
	 
	 
	 /**
	  * Run the slonik script.
	  * 
	  * This involves launching the slonik process along with threads to monitor
	  * stdout and stderr.  The script + preamble will be writen to stdin. 
	  */
	public void run() {
		
		
		runSubProcess();
		
	}
	
	public void setScript(String script) {
		this.script=script;
	}
	
	protected void writeInput(Writer scriptWriter) throws IOException  {
		String parsedPreamble = substitutePreamble();
		log.info("Executing slonik script:\n" + parsedPreamble + script);
		//Send the script to the slonik instance.
		scriptWriter.write(parsedPreamble);
		scriptWriter.write(script);
		//Close the stream. Slonik only parses the script once 
		//it receives EOF.
		scriptWriter.close();
	}
	
	protected ShellExecScript.CommandOptions getExecutablePath() {
		String path[] =  new String[]{properties.getProperty("slonik.path")};
		if (path == null) {
			log.error("slonik.path is not set in the properties file");
			return null;
		}
		ShellExecScript.CommandOptions options = new ShellExecScript.CommandOptions();
		options.commandOptions = path;
		return options;
	}
	
	
	protected Logger getOutputLogger() {
		return log;
	}
}
