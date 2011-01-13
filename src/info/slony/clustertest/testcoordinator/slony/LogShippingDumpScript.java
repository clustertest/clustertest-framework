package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * The SlonDumpScript class is used to invoke the slony1_dump.sh script from the
 * slony tools directory.
 * 
 * This script makes a dump file that can be restored to a log shipping target node.
 *
 */
public class LogShippingDumpScript extends PgCommand {

	private static Logger log = Logger.getLogger(LogShippingDumpScript.class);
	private Properties properties;
	private String logicalDb;
	private File outputFile;
	private Writer outputWriter;
	public LogShippingDumpScript(Coordinator coordinator,Properties properties, String logicalDb,
			File outputFile) {
		super(logicalDb,coordinator,properties,"slony-dump");
		this.properties=properties;
		this.logicalDb=logicalDb;
		this.outputFile = outputFile;
	}
	
	@Override
	protected CommandOptions getExecutablePath() {
		String  dumpPath = properties.getProperty("slony_dump.path");
		if(dumpPath==null) {
			log.error("slony_dump.path is not defined in the properties file");
			throw new IllegalArgumentException("slony_dump.path is not found");
		}
		CommandOptions options  = new CommandOptions();
		String dbName = properties.getProperty("database." + logicalDb + ".dbname");
		String hostName = properties.getProperty("database."+logicalDb+".host");
		String port = properties.getProperty("database."+ logicalDb + ".port");
		String clustername = properties.getProperty("clustername");
		String user=properties.getProperty("database." + logicalDb + ".user.slony");
		ArrayList<String> environment = new ArrayList<String>();
		if(hostName != null) {
			environment.add("PGHOST=" + hostName);
		}
		if(port != null) {
			environment.add("PGPORT=" + port);
		}
		if(user != null) {
			environment.add("PGUSER=" + user);
		}
		String password = properties.getProperty("database." + logicalDb + ".password.slony");
		if(password != null) {
			try {
				File passFile = this.createPasswordFile(hostName, port, dbName, user, password);
				environment.add("PGPASSFILE=" + passFile.getAbsolutePath());
			}
			catch(IOException e) {
				log.error("error writing password file:",e);
			}
			
		}
		options.commandOptions = new String[]{dumpPath,dbName,
		                                    clustername};
		options.environment =environment.toArray(new String[environment.size()]);
		
		return options;
	}

	@Override
	protected void writeInput(Writer w) throws IOException {
		// TODO Auto-generated method stub
		w.close();
	}
	public OutputLineProcessor getStdoutProcessor() {
		
		try {
			final FileWriter writer = new FileWriter(this.outputFile);
			outputWriter = writer;
			return new OutputLineProcessor() {
				public void processLine(String line) {
					try {
						writer.write(line);
						writer.write("\n");
						writer.flush();
					}
					catch(IOException e) {
						log.error("error writing file",e);
					}
				}
			};
		}
		catch(IOException e) {
			log.error("error creating output file",e);
			return null;
		}
		
		
		
		
		
	}
	
	public void run() {
		runSubProcess();
	}
	protected void onProcessFinished() {
		if(outputWriter != null) {
			try {
				log.info("closing output file");
				outputWriter.close();
			}
			catch(IOException e) {
				log.error("error closing output file");
			}
		}
	}

}
