package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Properties;

/**
 * 
 * The PgDumpCommand class handles the invoking of the pg_dump command against a logical
 * database.
 * 
 * The current implementation of this command ONLY dumps the data (no schema).
 * 
 * 
 */
public class PgDumpCommand extends PgCommand {

	/**
	 * The output file to dump the data to.
	 */
	private String outputFile;
	/**
	 * The list of tables to dump.
	 */
	private String[] tableList;
	
	private boolean dataOnly;
	
	public PgDumpCommand(String logicalDb , Coordinator coordinator, Properties properties
			,String outputFile, String[]  tableList,boolean dataOnly) {
		
		super(logicalDb, coordinator,properties,"pg_dump");
		this.tableList =tableList;
		this.outputFile = outputFile;
		this.dataOnly=dataOnly;
		 
	}
	
	@Override
	protected CommandOptions getExecutablePath() {
		// TODO Auto-generated method stub
		ArrayList<String> arguments = new ArrayList<String>();
			
		String tmp = properties.getProperty("database." + logicalDbName + ".pgsql.path");
		if(tmp==null) {
			//If no path is set in the config file assume that createdb is in the PATH
			arguments.add("pg_dump");
			
		}
		else {
			arguments.add(tmp + "/pg_dump");
		
		}
		if(dataOnly) {
			arguments.add("--data-only");
		}
		
		if(tableList != null) {
			for(int idx=0; idx < tableList.length; idx++) {
				arguments.add("--table=" + tableList[idx] );
			}
		}
		arguments.add("--file=" + outputFile);
		arguments.add("--disable-triggers");
		CommandOptions commandOptions = getPgCommandOptions(true);
		for(int idx=0; idx < commandOptions.commandOptions.length; idx++) {
			arguments.add(commandOptions.commandOptions[idx]);			
		}
		commandOptions.commandOptions = arguments.toArray(new String[arguments.size()]);
		
		return commandOptions;
		
	}

	@Override
	protected void writeInput(Writer w) throws IOException {
		
		//We don't write to pg_dumps stdin.
		w.close();

	}
	public void run() {
		runSubProcess();
	}

}
