package info.slony.clustertest.testcoordinator;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.log4j.Logger;

public class JDBCUtilities {

	
	private static Logger log = Logger.getLogger(JDBCUtilities.class);
	
	/**
	 * Gets (creates) a JDBC Connection class for the logical database name
	 * @param logicalName The logical database name used to find settings in the properties
	 *        map.
	 * @return A JDBC Connection instance that points to the database.
	 * @throws IllegalArgumentException
	 * @throws SQLException
	 */
	public static Connection getConnection(Driver jdbcDriver,Properties properties,String logicalName)
			throws IllegalArgumentException, SQLException {
		// Get the JDBC url
		StringBuilder builder = new StringBuilder("jdbc:postgresql://");
		String host = (String) properties.get("database." + logicalName
				+ ".host");
		if (host == null) {
			throw new IllegalArgumentException(logicalName
					+ " does not have a configuration value for the host");
		}
		builder.append(host);
		String port = (String) properties.get("database." + logicalName
				+ ".port");
		if (port != null) {
			builder.append(":" + port);
		}
		String dbName = (String) properties.get("database." + logicalName
				+ ".dbname");
		if (dbName == null) {
			// throw
			throw new IllegalArgumentException(logicalName
					+ " does not have a configuration value for the dbname");
		}
		builder.append("/" + dbName);
		Properties driverProperties = new Properties();
		String user = (String) properties.get("database." + logicalName
				+ ".user.slony");
		if (user != null) {
			driverProperties.put("user", user);
		}
		String password = (String) properties.get("database." + logicalName
				+ ".password.slony");
		if (password != null) {
			driverProperties.put("password", password);
		}
		String jdbcURL = builder.toString();
		log.debug("Creating JDBC Connection for URL:" + jdbcURL + " user:" + user );
		return jdbcDriver.connect(jdbcURL, driverProperties);
	}


}
