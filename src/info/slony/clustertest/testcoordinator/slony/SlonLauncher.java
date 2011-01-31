package info.slony.clustertest.testcoordinator.slony;

import info.slony.clustertest.testcoordinator.Coordinator;
import info.slony.clustertest.testcoordinator.Event;

import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Logger;

/**
 * 
 * The SlonLauncher will launch and monitor the output of a slon process.
 *
 */
public class SlonLauncher extends ShellExecScript {

	public static String EVENT_SET_SUBSCRIBED="EVENT_SET_SUBSCRIBED";
	public static String EVENT_STORE_SUBSCRIBE="EVENT_STORE_SUBSCRIBE";
	public static String EVENT_ABNORMAL_EXIT="EVENT_ABNORMAL_EXIT";
	public static String EVENT_COPY_FAILED="EVENT_COPY_FAILED";
	public static String EVENT_SLON_STARTED="EVENT_SLON_STARTED";
	
	private Properties properties;
	private static Logger log = Logger.getLogger(SlonLauncher.class);
	/**
	 * The logical database that this slon should connect to.
	 */
	private String logicalDatabase;
	
	/**
	 * The directory log shipping spool files should be placed in.
	 */
	private String logshippingDirectory=null;
	
	private class SlonOutputProcessor implements ShellExecScript.OutputLineProcessor {
		private Pattern copyDonePattern = Pattern.compile(".*CONFIG enableSubscription: sub_set=.*");
		private Pattern storeSubscribePattern= Pattern.compile(".*CONFIG storeSubscribe: sub_set=.*");
		private Pattern slonTerminated = Pattern.compile(".*CONFIG slon: child terminated [a-zA-Z0-9 ]*: (\\d*);.*");
		private Pattern copyFailed = Pattern.compile(".*ERROR .*prepareTableForCopy.*");
		private Pattern startingUp  = Pattern.compile(".*CONFIG main: configuration complete - starting threads");
		public void processLine(String line) {
			Matcher m1 = copyDonePattern.matcher(line);
			
			if(m1.matches()) {
				Event event = new Event();
				event.source = SlonLauncher.this;
				event.eventName = EVENT_SET_SUBSCRIBED;
				log.debug("subscription is complete:" + line);
				coordinator.queueEvent(event);
				return;
			}
			Matcher m2 = storeSubscribePattern.matcher(line);
			if (m2.matches()) {
				Event event = new Event();
				event.source = SlonLauncher.this;
				event.eventName = EVENT_STORE_SUBSCRIBE;
				log.debug("subscription step 1:" + line);
				coordinator.queueEvent(event);
				return;
			}
			Matcher m3 = slonTerminated.matcher(line);
			if(m3.matches()) {
				String code = m3.group(0);
				if(code != "0") {
					Event event = new Event();
					event.source = SlonLauncher.this;
					event.eventName = EVENT_ABNORMAL_EXIT;
					log.info("abnormal termination of slon" + line);
					coordinator.queueEvent(event);
					return;
				}
			}
			Matcher copyFailedMatcher = copyFailed.matcher(line);
			if(copyFailedMatcher.matches()) {
				Event event = new Event();
				event.source=SlonLauncher.this;
				event.eventName = EVENT_COPY_FAILED;
				log.info("copy failed:" + line);
				coordinator.queueEvent(event);
			}
			Matcher startingUpMatcher = startingUp.matcher(line);
			if(startingUpMatcher.matches()) {
				Event event = new Event();
				event.source=SlonLauncher.this;
				event.eventName = EVENT_SLON_STARTED;
				log.info("slon has started:" + line);
				coordinator.queueEvent(event);
			}
			
		}
	};
	
	public SlonLauncher(Coordinator coordinator,Properties properties, 
			String logicalDatabase, String logshippingDirectory) {
		super(coordinator,logicalDatabase);
		this.properties = properties;
		this.logicalDatabase = logicalDatabase;
		this.logshippingDirectory=logshippingDirectory;
	}
	
	@Override
	protected ShellExecScript.CommandOptions getExecutablePath() {
		String slonPath = properties.getProperty("slon.path");
		ArrayList<String> resultBuilder = new ArrayList<String>();
		
		if(slonPath == null) {
			log.error("slon.path is not defined");
			return null;
		}
		
		resultBuilder.add(slonPath);	
		
		if(logshippingDirectory != null) {
			resultBuilder.add("-a");
			resultBuilder.add(logshippingDirectory);
		}
		
		String logLevel = properties.getProperty("slon.loglevel");
		if (logLevel == null) {
		    // No log level defined here
		} else {
		    resultBuilder.add("-d" + logLevel);
		}
		
		String clusterName = properties.getProperty("clustername");
		resultBuilder.add(clusterName);
		
		String host = properties.getProperty("database." + logicalDatabase + ".host");
		String dbname = properties.getProperty("database." + logicalDatabase + ".dbname");
		String port = properties.getProperty("database." + logicalDatabase + ".port");
		String user = properties.getProperty("database." + logicalDatabase + ".user.slony");
		String password = properties.getProperty("database." + logicalDatabase + ".password.slony");
		
		StringBuilder connInfo = new StringBuilder();
		
		if(dbname != null) {
			connInfo.append("dbname=" + dbname);
			
		}
		if(host != null) {
			connInfo.append(" host=");
			connInfo.append(host);
		}
		
		if(user  != null) {
			connInfo.append(" user=");
			connInfo.append(user);
		}
		if(port != null) {
			connInfo.append(" port=");
			connInfo.append(port);
		}
		if(password != null) {
			connInfo.append(" password=");
			connInfo.append(password);
		}
		
		resultBuilder.add(connInfo.toString());
		ShellExecScript.CommandOptions options = new ShellExecScript.CommandOptions();
		options.commandOptions = resultBuilder.toArray(new String[resultBuilder.size()]);
		return options;
		
		
		
	}

	@Override
	protected void writeInput(Writer w) throws IOException {
		//We don't actually send any output to slon.

	}
	
	protected OutputLineProcessor getStdoutProcessor() {
		OutputLineProcessor processor = new SlonOutputProcessor();
		return processor;
	}
	
	public void run() {
		runSubProcess();
	}

	protected Logger getOutputLogger() {
		return log;
	}
}
