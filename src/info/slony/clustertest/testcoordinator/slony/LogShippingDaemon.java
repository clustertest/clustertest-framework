package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * The LogShippingDaemon class will launch an instance of the slony_logshipping 
 * prorgram that processes shipped logs from teh archive directory and applies
 * them to a slave log-shipping database.
 *
 */
public class LogShippingDaemon extends ShellExecScript {
	
	private static Logger log = Logger.getLogger(LogShippingDaemon.class);
	private String logicalDb;
	private Properties properties;
	private File spoolDirectory;

	
	public LogShippingDaemon(Coordinator coordinator,Properties properties,
			String logicalDb, File spoolDirectory ) {
		super(coordinator,"slony_logshipping " + logicalDb);
		this.properties=properties;
		this.logicalDb = logicalDb;
		this.spoolDirectory=spoolDirectory;
	}
	
	@Override
	protected CommandOptions getExecutablePath() {
		// TODO Auto-generated method stub
		CommandOptions options = new CommandOptions();
		
		String logShipPath = properties.getProperty("log_shipping.path");
		
		
		try {
			File configFile = File.createTempFile("logshipping",".conf");
			configFile.deleteOnExit();
			FileWriter writer = new FileWriter(configFile);
		
		
			writer.write("logfile='/tmp/logshipping.out';\n");
			writer.write("cluster name='" + properties.getProperty("clustername")+"';\n");
			writer.write("destination database='dbname=" );
			String dbName = properties.getProperty("database." + logicalDb + ".dbname");
			writer.write(dbName);
			String host = properties.getProperty("database." + logicalDb + ".host");
			if(host != null) {
				writer.write(" host=");
				writer.write(host);
			}
			String user = properties.getProperty("database." + logicalDb + ".user.slony");
			if(user != null) {
				writer.write(" user=");
				writer.write(user);
			}
			String password = properties.getProperty("database." + logicalDb + ".user.slony");
			if(password != null) {
				writer.write(" password=");
				writer.write(password);
			}
			String port = properties.getProperty("database." + logicalDb + ".port");
			if(port != null) {
				writer.write(" port=");
				writer.write(port);
			}
			writer.write("';\n");
			writer.write("archive dir='" + spoolDirectory.getAbsolutePath() + "';\n");
			writer.close();
			
			options.commandOptions = new String[]{ logShipPath,
					"-f", "-s","3",configFile.getAbsolutePath()};
			
			
			return options;
		}
		catch(IOException e) {
			log.error("error writing archive file");
			return null;
		}
		
		
	}

	@Override
	protected void writeInput(Writer w) throws IOException {
		w.close();

	}
	
	public void run() {
		runSubProcess();
	}

}
