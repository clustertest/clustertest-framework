package info.slony.clustertest.testcoordinator;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * 
 * The CompareOperation class allows for the results of a query against a pair
 * of databases to be compared against each other.
 * 
 * Instances of this class can be used to compare two databases against each
 * other (ie a origin and a replica).
 * 
 * 
 * Compare operations run asynchrnously and report their results via the
 * Coordinator through the Event/ExecutionObserver interface.
 * 
 */
public class CompareOperation implements EventSource {

	private static Logger log = Logger.getLogger(CompareOperation.class);

	private Properties properties;

	private Connection jdbcConnection1;
	private Connection jdbcConnection2;

	private Statement jdbcStatement1;
	private Statement jdbcStatement2;
	private ResultSet jdbcResultSet1;
	private ResultSet jdbcResultSet2;

	private String query;
	private Driver jdbcDriver;

	public static String COMPARE_EQUALS = "COMPARE_EQUALS";
	public static String COMPARE_EXEC_FAILURE = "COMPARE_EXEC_FAILURE";
	public static String COMPARE_DIFFERENT = "COMPARE_DIFFERENT";

	private String resultCode;
	private String resultMessage;
	private String primaryKeyColumn;
	private ArrayList<String> differenceList = new ArrayList<String>();

	private Coordinator coordinator;
	private volatile boolean isFinished=false;

	public CompareOperation(Properties properties, Driver jdbcDriver,
			Coordinator coordinator, String primaryKeyColumn) {
		this.properties = properties;
		this.jdbcDriver = jdbcDriver;
		this.coordinator = coordinator;
		this.primaryKeyColumn=primaryKeyColumn;
	}

	/***
	 * Sets the logical name of the databases. 
	 * 
	 * Logical names need to map to entries in the properties file of the form
	 * 
	 * \li database.$logicalname.host
	 * \li database.$logicalname.dbname
	 * \li database.$logicalname.user
	 * \li database.$logicalname.password
	 * 
	 * @param logicalName1 The logical name of the first database.
	 * @param logicalName2 THe logical name of the second database.
	 * @throws IllegalArgumentException Thrown if no entry in the properties map match the logical name 
	 * @throws SQLException
	 */
	public void setDatabases(String logicalName1, String logicalName2)
			throws IllegalArgumentException, SQLException {

		jdbcConnection1 = JDBCUtilities.getConnection(jdbcDriver,properties,logicalName1);
		jdbcConnection2 = JDBCUtilities.getConnection(jdbcDriver,properties,logicalName2);
		jdbcConnection1.setAutoCommit(false);
		jdbcConnection2.setAutoCommit(false);

	}

	/**
	 * Sets the query that will be used for the comparision.
	 * 
	 * This query will be executed on both database systems and the ouptut 
	 * is what is compared by this class in determining if the two systems are
	 * the same.
	 * 
	 * It is important that these queries are ordered (ie have an ORDER BY clause 
	 * that covers all output columns).
	 * 
	 * @param query
	 */
	public void setQuery(String query) {
		this.query = query;
	}

	
	/**
	 * Runs the comparision operation.
	 * This method will return almost immediately, an EVENT_FINISHED message will be
	 * posted to the Coordinator when the comparision is actually complete.
	 */
	public void run() {
		final CompareOperation mThis = this;
		Thread t = new Thread(new Runnable() {
			public void run() {
				mThis.runInternal();
			}
		});

		// Now that
		t.start();
	}

		
	/**
	 * The internal run method for the comparision operation.
	 * This method is launched by the comparision thread to perform the 
	 * actual comparision.
	 * 
	 */
	private void runInternal() {

		final CompareOperation myThis = this;

		
		/**
		 * First we create threads for executing the query.
		 * We create a seperate thread for each database.
		 */
		Thread t1 = new Thread(new Runnable() {
			public void run() {
				try {
					log.debug("executing sql statement on first db:" + query);
					myThis.jdbcStatement1 = myThis.jdbcConnection1
							.createStatement();
					myThis.jdbcStatement1.setFetchSize(100);
					jdbcResultSet1 = myThis.jdbcStatement1.executeQuery(query);
					jdbcResultSet1.setFetchSize(100);
				} catch (SQLException e) {
					log.error("error executing SQL:" + query, e);
				}

			}

		});
		Thread t2 = new Thread(new Runnable() {
			public void run() {
				try {
					log.debug("executing sql statement on second db:" + query);
					myThis.jdbcStatement2 = myThis.jdbcConnection2
							.createStatement();
					myThis.jdbcStatement2.setFetchSize(100);
					jdbcResultSet2 = myThis.jdbcStatement2.executeQuery(query);
					jdbcResultSet2.setFetchSize(100);
				} catch (SQLException e) {
					log.error("error executing SQL:" + query, e);
				}
			}

		});

		t1.start();
		t2.start();

		try {
			t1.join();
			t2.join();
			
			//
			// At this point the two result sets should be available  
			// and visible to this thread (per the .join() above).
			log.debug("starting row by row comparision");
			
			if (jdbcResultSet1 == null || jdbcResultSet2 == null) {
				// Failure executing query.
				resultCode = COMPARE_EXEC_FAILURE;
				return;
			}
			
			//Assume the two sets are equal until we see otherwise.
			resultCode = COMPARE_EQUALS;
			// Compare the rows.
			boolean set1HasMore = jdbcResultSet1.next();
			boolean set2HasMore = jdbcResultSet2.next();
			
			int rowCount1=0;
			int rowCount2=0;
			while (set1HasMore || set2HasMore ) {
				
				if (!set1HasMore) {
					
					if (set2HasMore) {
						// Comparision failed.
						// Set 2 has more rows than set 1.
						resultCode = COMPARE_DIFFERENT;
						if(jdbcResultSet2.next()) {
							resultMessage = "set 2 has more rows  than set 1" ;
						}
						
						
						log.info(resultMessage);
					} else {
						//resultCode is left as it was from comparing the data.
						//ie if no differences in the data were found it should be COMPARE_EQUALS
						
					}
					break;
				}// set 1 is finished.
				else {
					if (!set2HasMore) {
						// set 2 is finished.
						// Comparision failed.
						// Set 1 has more rows than set 2.
						resultCode = COMPARE_DIFFERENT;
						//Check if set 1 has more rows.
						//If it has more rows then the sets are different sizes, but if
						//set 1 is also at the end then the data is just different.						
						if(jdbcResultSet1.next()) {
							resultMessage = "set 1 has more rows than set 2" ;
						}
						log.info(resultMessage);
						break;
					} else {
						if(!compareRow(jdbcResultSet1, jdbcResultSet2)) {
							String key1 = jdbcResultSet1.getString(primaryKeyColumn);
							String key2 = jdbcResultSet2.getString(primaryKeyColumn);
							if(key1.compareTo(key2) < 0) {
								set1HasMore=jdbcResultSet1.next();
							}
							else {
								set2HasMore=jdbcResultSet2.next();
							}
						}
						else {
							set1HasMore=jdbcResultSet1.next();
							set2HasMore=jdbcResultSet2.next();
						}
					}
				}				
			}//while

		} catch (InterruptedException e) {
			resultCode = COMPARE_EXEC_FAILURE;
			log.error("error waiting for queries to finish", e);
		} catch (SQLException e) {
			resultCode = COMPARE_EXEC_FAILURE;
			log.error("error comparing tables", e);
		} finally {
			if (jdbcResultSet1 != null) {
				try {
					jdbcResultSet1.close();
				} catch (SQLException e) {
					log.error("error closing JDBC resource", e);
				}
			}
			if (jdbcResultSet2 != null) {
				try {
					jdbcResultSet2.close();
				} catch (SQLException e) {
					log.error("error closing JDBC resource", e);
				}
			}
			if (jdbcStatement1 != null) {
				try {
					jdbcStatement1.close();
				} catch (SQLException e) {
					log.error("error closing JDBC resource", e);
				}
			}

			if (jdbcStatement2 != null) {
				try {
					jdbcStatement2.close();
				} catch (SQLException e) {
					log.error("error closing JDBC resource", e);
				}
			}
			if(jdbcConnection1 != null) {
				try {
					jdbcConnection1.close();
				}
				catch(SQLException e) {
					log.error("error closing JDBC connection");
				}
				
			}
			if(jdbcConnection2 != null) {
				try {
					jdbcConnection2.close();
				}
				catch(SQLException e) {
					log.error("error closing JDBC connection");
				}
				
			}
			
			
			//The last thing we do is post a notification that
			//this operation is finished.
			Event event = new Event();
			event.source = this;
			event.eventName = Coordinator.EVENT_FINISHED;
			coordinator.queueEvent(event);
		}// finally
		isFinished=true;

	}

	private boolean compareRow(ResultSet row1, ResultSet row2) throws SQLException {

		int columnCount = row1.getMetaData().getColumnCount();
		for (int idx = 1; idx <= columnCount; idx++) {
			byte bytes1[] = row1.getBytes(idx);
			byte bytes2[] = row2.getBytes(idx);
			if (!Arrays.equals(bytes1,bytes2)) {
				// fail.
				
				String msg = "<" + rowAsString(row1) + "\n" 
					+ ">"+ rowAsString(row2) ;
				
				differenceList.add(row1.getString(idx) + msg);
				resultCode =COMPARE_DIFFERENT;
				log.info("rows differ:\n" + msg);
				return false;
			}
		}
		return true;
	}

	
	private String rowAsString(ResultSet rs) throws SQLException {
		StringBuilder builder = new StringBuilder();
		ResultSetMetaData metaData = rs.getMetaData();
		if(metaData == null) {
			return "can't generate row";	
		}		
		int columnCount = metaData.getColumnCount();
		for(int idx=1; idx <= columnCount; idx++) {
			 if(idx > 1) {
				 builder.append(",");
			 }
			 
			 String value = rs.getString(idx);
			 if(value == null) {
				 builder.append("null");
			 }
			 else {
				 builder.append("'");			 
				 builder.append(value);
				 builder.append("'");
			 }
		}
		return builder.toString();
	}
	
	public List<String> getDifferenceList() {
		return differenceList;
	}

	public String getResultCode() {
		return resultCode;
	}
	
	public boolean isFinished() {
		return isFinished;
	}
	
	

}
