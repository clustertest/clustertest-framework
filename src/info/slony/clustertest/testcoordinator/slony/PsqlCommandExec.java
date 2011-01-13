package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.File;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;


/**
 * This class will invoke the psql command on a predefined SQL script.
 * 
 *
 */
public class PsqlCommandExec extends PgCommand  {

	private static Logger log = Logger.getLogger(PsqlCommandExec.class);
	
	private String logicalDbName;
	private String SQLScript;
	private Properties properties;
	private File SQLFile;
	
	public PsqlCommandExec(String logicalDbName, String SQLScript, Coordinator coordinator,
			Properties properties) {
		super(logicalDbName,coordinator,properties,"psql " + logicalDbName);
		this.properties = properties;
		this.SQLScript = SQLScript;
		this.logicalDbName = logicalDbName;
		
	}
	
	public PsqlCommandExec(String logicalDbName,File  SQLFile, Coordinator coordinator,
			Properties properties) {
		super(logicalDbName,coordinator,properties,"psql " + logicalDbName);
		this.properties = properties;
		this.SQLFile = SQLFile;
		this.SQLScript="";
		this.logicalDbName = logicalDbName;
		
	}

	
	protected ShellExecScript.CommandOptions getExecutablePath() {
		ArrayList<String> path = new ArrayList<String>();
		String tmp = properties.getProperty("database." + logicalDbName + ".pgsql.path");
		if(tmp==null) {
			//If no path is set in the config file assume that createdb is in the PATH
			path.add("psql");
			
		}
		else {
			path.add(tmp + "/psql");
		
		}
		
		
		if(SQLFile != null) {
			path.add("-f");
			path.add(SQLFile.getAbsolutePath());			
		}
		ShellExecScript.CommandOptions options = getPgCommandOptions(true);
		for(int idx = 0; idx < options.commandOptions.length; idx++) {
			path.add(options.commandOptions[idx]);
		}	
		options.commandOptions = path.toArray(new String[path.size()]);
		return options;
	}
	
	
	protected  void writeInput(Writer w) throws IOException {

		w.write(SQLScript);
		w.close();
	}
	
	protected OutputLineProcessor getStdErrorProcessor() {
		return new OutputLineProcessor() {
			public void processLine(String line) {
				if(line.startsWith("ERROR:")) {
					 //An error was detected.
				}
			}
			
		};
	}
	
	public void run() {
		runSubProcess();
	}
}
