package info.slony.clustertest.testcoordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.log4j.Logger;


/**
 * 
 * The TestResult class is used to store and compute the results of a test run (ie a single test script).
 * 
 * The TestResult class allows for individual test checks to be marked as having passed or failed.
 * 
 * The TestResult class is responsible for storing these results to persistent storage.
 * 
 * 
 * @todo This class needs to put the results somewhere that we can automatically colate them.
 *       Currently it is more of a placeholder.
 */
public class TestResult {

	private volatile int passCount=0;
	private volatile int failCount=0;
	private String testName;
	private static Logger log = Logger.getLogger(TestResult.class);
	
	private FileWriter summaryFile;
	private FileWriter groupDetailFile;
	private String currentGroup;
	private String timeString;
	private String outputDirectory;
	private File resultsDirectory;
	public TestResult(String testName, String outputDirectory) throws IOException {
		this.testName=testName;
		this.outputDirectory = outputDirectory;
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
		timeString = dateFormat.format(new Date());
		resultsDirectory = new File(outputDirectory + File.separator + timeString);
		resultsDirectory.mkdirs();
		summaryFile = new FileWriter(new File(resultsDirectory,  "testResult." + testName + ".txt"));
		
	}
	
	public synchronized void newGroup(String s) throws IOException {
		if(currentGroup != null) {
			StringBuilder summary = new StringBuilder();
			summary.append(Integer.toString(passCount));
			summary.append(",");
			summary.append(failCount);
			summary.append(",");
			summary.append(currentGroup);
			summary.append("\n");
			summaryFile.write(summary.toString());
			summaryFile.flush();
			log.info(summary.toString());
		}
		log.info("Starting group " + s);
		currentGroup=s;
		passCount=0;
		failCount=0;
		if(s != null)  {
			if(groupDetailFile != null) {
				groupDetailFile.close();
			}
			groupDetailFile = new FileWriter(new File(resultsDirectory  , "testDetail." + s + ".txt" ));
		}
		
	}
	
	public void assertCheck(String description, Object expected, Object actual) throws IOException {
		if(expected.equals(actual)  ) {
			if(groupDetailFile != null) {
				groupDetailFile.write("pass," + description + "\n");
			}
			log.info(description + ":passes");
			passCount++;
		}
		else {
			log.info(description + ":fail:" + expected + "," + actual);
			if(groupDetailFile != null) {
				groupDetailFile.write("fail," + description + ":" + expected + "," + actual + "\n");
			}
			failCount++;
		}
	}
	
	/**
	 * Produce a report of the test results.
	 */
	public void testComplete() throws IOException {
		log.info("Test " + testName + " is complete. passes=" + passCount + " fails=" +failCount);
		newGroup(null);
	}
	
	public int getFailureCount() {
		return failCount;
	}
	public void close() throws IOException {
		summaryFile.close();
		if(groupDetailFile != null) {
			groupDetailFile.close();
			groupDetailFile =null;
		}
		
	}
}
