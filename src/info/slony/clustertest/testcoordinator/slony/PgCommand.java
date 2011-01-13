package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

public abstract class PgCommand extends ShellExecScript {
	
	protected Properties properties;
	protected String logicalDbName;
	
	private static Logger log = Logger.getLogger(PgCommand.class);

	
	
	
	PgCommand(String logicalDbName,Coordinator coordinator,Properties properties,String label) {
		super(coordinator,label);
		this.properties=properties;
		this.logicalDbName = logicalDbName;
	}
	
	
	/**
	 * Gets a string with command line options to launch a pgsql executable (ie psql,createdb etc..)
	 * with options based on the values of logicalDbName and the properteis hash for this object.
	 *  
	 * @param environmentAdditions An array list that will have environment entries 
	 * @return
	 */
	protected ShellExecScript.CommandOptions getPgCommandOptions(boolean useSlonyUser)  {
		
		CommandOptions result = new CommandOptions();
		
		ArrayList<String> path = new ArrayList<String>();
		
		String user =null;
		if(useSlonyUser) {
			user=properties.getProperty("database." + logicalDbName + ".user.slony");
		}
		else {
			user=properties.getProperty("database." + logicalDbName + ".user");
		}
		if(user != null) {
			path.add("--username=" + user);
		}		
		
		String host = properties.getProperty("database." + logicalDbName + ".host");
		if(host != null) {
			path.add("--host=" + host);
		}
		
		String port = properties.getProperty("database." + logicalDbName + ".port");
		if(port != null) {
			path.add("--port=" + port);
		}
		
		String dbName = properties.getProperty("database." + logicalDbName + ".dbname");
		if(dbName == null) {
			//This is an error.
			//How can we create a database with no name?
			throw new IllegalArgumentException("database." + logicalDbName + ".dbname is not defined" );
		}
		path.add(dbName);
		
		String password =null;
		if(useSlonyUser) {
			password = properties.getProperty("database." + logicalDbName + ".password.slony");
		}
		else {
			password = properties.getProperty("database." + logicalDbName + ".password");
		}
		if(password != null) {
			//Write out a .pgpass file
			try {
				File passFile = createPasswordFile(host,port,dbName,user,password);
				log.debug("using pgpass file:" + passFile.getAbsolutePath());
				result.environment=new String[]{"PGPASSFILE=" + passFile.getAbsolutePath()};
			}
			catch(IOException e) {
				log.error("error writing pgpass file",e);
				//Do not return a path string since the string we return
				//won't be valid.
				//Instead 
				return null;
			}
			
		}
		else
		{
			log.info("password for " + logicalDbName + " is null");
		}
		
		result.commandOptions = path.toArray(new String[path.size()]);
		//log.debug("returning path " + pathString);
		return result;
	}
	
	/**
	 * Generates a PG Password file suitable for passing the password to command line
	 * programs.
	 * @param host The host (if any) that the program will connect to
	 * @param port  The port (if any) that the program will connect to 
	 * @param dbName  The name of the database
	 * @param user The user the password is fore
	 * @param password The password
	 * @return  A File object pointing at the password file created.
	 * @throws IOException
	 */
	protected File createPasswordFile(String host,String port,String dbName,
				String user,String password) throws IOException {
		File passFile = File.createTempFile("pgpass.slony." + dbName, "");
		FileWriter writer = new FileWriter(passFile);
		if(host != null) {
			writer.write(host);
		}
		else {
			writer.write("*");
		}
		
		if(port != null) {
			writer.write(":" + port);
		}
		else {
			writer.write(":*");
		}
		writer.write(":" + dbName);
		if(user != null) {
			writer.write(":" + user);
		}
		else {
			writer.write(":*");
		}
		if(password != null) {
			writer.write(":" + password);
		}
		writer.write("\n");

		if(host != null) {
			writer.write(host);
		}
		else {
			writer.write("*");
		}
		
		if(port != null) {
			writer.write(":" + port);
		}
		else {
			writer.write(":*");
		}
		writer.write(":postgres");
		if(user != null) {
			writer.write(":" + user);
		}
		else {
			writer.write(":*");
		}
		if(password != null) {
			writer.write(":" + password);
		}
		writer.write("\n");

		writer.close();
		passFile.deleteOnExit();
		//pgpass had better be 0600
		passFile.setReadable(false, false);
		passFile.setReadable(true, true);
		passFile.setExecutable(false);			
		passFile.setWritable(false);
		return passFile;
	}
}
