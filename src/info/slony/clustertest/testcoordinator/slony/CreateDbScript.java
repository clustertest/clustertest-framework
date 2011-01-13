package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * The CreateDbScript will create a database based on the logical name passed in.
 * 
 * This class uses the createdb postgresql binary. 
 *
 */
public class CreateDbScript extends PgCommand {
	
	private String logicalDbName;
	private Properties properties;
	
	private static Logger log = Logger.getLogger(CreateDbScript.class);
	
	
	/**
	 * Creates an instance that will handle creating the database and
	 * seeding it.
	 * @param logicalDbName  The logical name of the database, this should map to 
	 *                       a set of entries in the properties object of the from
	 *                       database.$logicalDbName.dbname database.$logicalDbName.host etc...	
	 * @param coordinator  The coordinator instance for processing events
	 * @param properties  A properties hash.
	 */
	public CreateDbScript(String logicalDbName, Coordinator coordinator, Properties properties) {
		super(logicalDbName,coordinator,properties,"createdb " + logicalDbName);
		this.logicalDbName = logicalDbName;
		this.properties=properties;
		
		
	}
	
	protected ShellExecScript.CommandOptions getExecutablePath() {
		ArrayList<String> path = new ArrayList<String>();
		String tmp = properties.getProperty("database." + logicalDbName + ".pgsql.path");
		if(tmp==null) {
			//If no path is set in the config file assume that createdb is in the PATH			
			path.add("createdb");
		}
		else {
			path.add(tmp + "/createdb");			
		}
		
		
		ShellExecScript.CommandOptions options = getPgCommandOptions(true);
		for(int idx = 0; idx < options.commandOptions.length; idx++) {
			path.add(options.commandOptions[idx]);
		}
		options.commandOptions = path.toArray(new String[path.size()]);
		return options;
	}
	
	protected  void writeInput(Writer w) throws IOException {
		//Do nothing.
		//We don't pass anything to createdb on standard input.
	}
	
	public void run() {
		runSubProcess();
	}

	
	
}
