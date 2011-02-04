package info.slony.clustertest.testcoordinator;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Enumeration;

import org.apache.log4j.Logger;
import org.apache.log4j.Appender;
import org.apache.log4j.FileAppender;

/**
 * 
 * The TestResult class is used to store and compute the results of a test run (ie a single test script).
 * 
 * The TestResult class allows for individual test checks to be marked as having passed or failed.
 * 
 * The TestResult class is responsible for storing these results to persistent storage.
 * 
 * 
 * @todo This class needs to put the results somewhere that we can automatically collate them.
 *       Currently it is more of a placeholder.
 */
public class TestResult {

	private volatile int passCount=0;
	private volatile int failCount=0;
        private static int testnumber=0;    /* "Primary Key" for test */
	private String testName;
	private static Logger log = Logger.getLogger(TestResult.class);
	
	private FileWriter summaryFile;
	private FileWriter groupDetailFile;
	private String currentGroup;
	private String timeString;
	private String outputDirectory;
	private File resultsDirectory;
	private Appender groupAppender;

	public TestResult(String testName, String outputDirectory) throws IOException {
		this.testName=testName;
		this.outputDirectory = outputDirectory;
		DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd.HH:mm:ss");
		timeString = dateFormat.format(new Date());
		resultsDirectory = new File(outputDirectory + File.separator + timeString);
		resultsDirectory.mkdirs();
		summaryFile = new FileWriter(new File(resultsDirectory,  "testResult." + testName + ".txt"));
		
	}
	
	@SuppressWarnings("unchecked")
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
		File testGroupDir = null;
		if(s != null) {
			testGroupDir= new File(resultsDirectory,s);
			testGroupDir.mkdirs();
		}
		Logger rootLogger=Logger.getRootLogger();
		if( rootLogger != null) {
			if(groupAppender != null) {
				rootLogger.removeAppender(groupAppender);
			}
			if(testGroupDir != null) {
				Enumeration<Appender> appenderEnum = (Enumeration<Appender>)rootLogger.getAllAppenders();
				if(appenderEnum!=null && appenderEnum.hasMoreElements()) {			
					Appender defaultAppender=appenderEnum.nextElement();
					groupAppender=new FileAppender(defaultAppender.getLayout(),
												   testGroupDir+
												   File.separator + "testlog.log");
					rootLogger.addAppender(groupAppender);
				}
				
			}
		}
		log.info("Starting group " + s);
		currentGroup=s;
		passCount=0;
		failCount=0;
		if(s != null)  {
			if(groupDetailFile != null) {
				groupDetailFile.close();
			}
			groupDetailFile = new FileWriter(new File(testGroupDir  , 
													  "testDetail.txt" ));
		}
		
	}
	
	public void assertCheck(String description, Object expected, Object actual) throws IOException {
	        testnumber++;
		if(expected.equals(actual)  ) {
			if(groupDetailFile != null) {
				groupDetailFile.write("pass," + description + "," + testnumber + "\n");
			}
			log.info(description + ":passes");
			passCount++;
		}
		else {
			log.info(description + ":fail:" + expected + "," + actual);
			if(groupDetailFile != null) {
				groupDetailFile.write("fail," + description + ":" + expected + "," + actual + "," + testnumber + "\n");
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
