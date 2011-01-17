package info.slony.clustertest.testcoordinator;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.FileInputStream;
import java.io.Reader;
import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.EcmaError;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class TestCoordinator {

	private static Logger log = Logger.getLogger(TestCoordinator.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		  // Set up a simple configuration that logs on the console.
		try {

			Class.forName("org.postgresql.Driver");
			String propertiesFileName = args[0];
			TestCoordinator me = new TestCoordinator();
			FileReader reader = new FileReader(propertiesFileName);
			Properties props = new Properties();
			props.load(reader);
			String log4jConf = props.getProperty("log4j.conf");
			if(log4jConf != null) {
			    PropertyConfigurator.configure(log4jConf);
			}			 
			String outputDirectoryName = props.getProperty("output.directory");
			File outputDirectory = new File(outputDirectoryName);
			outputDirectory.mkdirs();
			TestResult results = new TestResult("test", outputDirectoryName);
			log.info("Launching a set of tests");
			for(int idx = 1; idx < args.length; idx++) {
				String testScript = args[idx];
				log.info("Starting test script " + testScript);		
				me.executeTestScript(args[idx],results, props);
			}
			results.close();
		}
		catch(IOException e) {
			log.error("error setting up test run",e);			
		}
		catch(ClassNotFoundException e) {
			log.error("can not load jdbc driver",e);
		}
		
	}
	
	protected void executeTestScript(String scriptFile, TestResult results,Properties properties) {
		
		Coordinator coordinator=null;
		try {
			Context ctx = Context.enter();			
			
			Scriptable scope = ctx.initStandardObjects();
		
			
			coordinator = new Coordinator(scope,ctx,properties);
			InputStream scriptStream = new FileInputStream(scriptFile);
			    //ClassLoader.getSystemResourceAsStream(scriptFile);
			if(scriptStream==null) {
				log.error("unable to find test script:" + scriptFile);
				return;
			}
			Reader scriptReader = new InputStreamReader(scriptStream); 
			Object wrappedOut = Context.javaToJS(coordinator, scope);
			ScriptableObject.putProperty(scope, "coordinator", wrappedOut);			
			wrappedOut = Context.javaToJS(results,scope);
			ScriptableObject.putProperty(scope,"results",wrappedOut);
			
			wrappedOut = Context.javaToJS(properties,scope);
			ScriptableObject.putProperty(scope,"properties",wrappedOut);
			
			
			ctx.evaluateReader(scope, scriptReader, scriptFile, 0, null);
			results.testComplete();
			
		}
		catch(IOException e) {
			log.error("error reading script",e);
			coordinator.abortTest(e.getMessage());
		}
		catch(EcmaError e) {
			log.error("error from script",e);
			e.printStackTrace();
			coordinator.abortTest(e.getMessage());
		}
		finally {
			if(coordinator != null) {
				coordinator.shutdown();
			}
		}
		
		
	}

}
