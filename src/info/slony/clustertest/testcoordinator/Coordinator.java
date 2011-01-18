package info.slony.clustertest.testcoordinator;

import info.slony.clustertest.testcoordinator.script.ClientScript;
import info.slony.clustertest.testcoordinator.script.ClientWorkerServer;
import info.slony.clustertest.testcoordinator.script.ExecutionObserver;
import info.slony.clustertest.testcoordinator.script.TimerHandle;
import info.slony.clustertest.testcoordinator.slony.CreateDbScript;
import info.slony.clustertest.testcoordinator.slony.DropDbScript;
import info.slony.clustertest.testcoordinator.slony.LogShippingDaemon;
import info.slony.clustertest.testcoordinator.slony.LogShippingDumpScript;
import info.slony.clustertest.testcoordinator.slony.PgDumpCommand;
import info.slony.clustertest.testcoordinator.slony.PsqlCommandExec;
import info.slony.clustertest.testcoordinator.slony.ShellExecScript;
import info.slony.clustertest.testcoordinator.slony.SlonLauncher;
import info.slony.clustertest.testcoordinator.slony.SlonikScript;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;

/**
 * The Coordinator class handles coordination of test coordinator scripts
 * 
 *  This class provides the ability for test scripts to:
 *    \li Start workflow tasks (ie test clients)
 *    \li Register an interest in events
 *   
 *   The class also provides the main event processing logic.
 * 
 *
 */
public class Coordinator {

	
	private static Logger log = Logger.getLogger(Coordinator.class);
	
	private Scriptable jsScope=null;
	
	private Context jsContext = null;
	
	public static String EVENT_FINISHED="EVENT_FINISHED";
	public static String EVENT_OUTPUT="EVENT_OUTPUT";
	public static String EVENT_TIMER="EVENT_TIMER";
	public static String EVENT_ERROR="EVENT_ERROR";
	
	private volatile boolean eventProcessingComplete=false;
	
	private List<Event> eventQueue=Collections.synchronizedList(new LinkedList<Event>());
	private Map<EventSource,Map<String,List<ExecutionObserver>>> eventListeners=Collections.synchronizedMap(new HashMap<EventSource,Map<String,List<ExecutionObserver>>>());
	private Properties properties = null;
	
	private Timer timer = new Timer();
	
	private Set<ClientScript> clientScripts=Collections.synchronizedSet(new HashSet<ClientScript>());
	private Set<ShellExecScript> shellProcesses = Collections.synchronizedSet(new HashSet<ShellExecScript>());
	private Set<Connection> jdbcConnections = new HashSet<Connection>();
	private Set<Statement> jdbcStatements = new HashSet<Statement>();
		
	private boolean testAborted=false;
	
	
	/**
	 * A map of locks used in the distributed locking mechanism.
	 * @note Code that accesses this map should be sure to enclose the approriate
	 * actions in a synchronized block that synchronizes on the lockMap.
	 */
	private Map<String,Lock> lockMap = new HashMap<String,Lock>();
	private ClientWorkerServer clientServer=null;
	
	public Coordinator(Scriptable scope, Context context, Properties props) throws IOException {
			jsScope = scope;
			jsContext = context;

			properties=props;
			if(properties.getProperty("client.server.port")!=null) {
				clientServer = new ClientWorkerServer(properties);
				clientServer.start();
			}
	}
	
	
	/**
	 * Includes a javascript file on the javascript scope.
	 * This method will compile the javascript in the javascript scope of this
	 * coordinator.  It will not explicitly execute the script.
	 *  
	 * @param fileName  The filename of the file to include/compile.  This should
	 * be available on the classpath.
	 * @throws IOException An IO Exception indicating that their was an error reading the file
	 */
	public void includeFile(String fileName) throws IOException {
	    InputStream stream = new FileInputStream(fileName);
		    // ClassLoader.getSystemResourceAsStream(fileName);
		if(stream == null) {
			throw new IOException("file not found:" + fileName);
		}
		Reader reader = new InputStreamReader(stream);
		jsContext.compileReader(reader, fileName, 0, null).exec(jsContext, jsScope);
		
		
	}
	
	/**
	 * Executes a client script in the background.
	 * 
	 * This method will return a ClientScript instances that represents a 
	 * client script execution (the actual execution might happen on a remote
	 * machine).
	 * 
	 * The script won't start executing until the .run() method is called on the
	 * result object.
	 *
	 */
	public ClientScript clientScript(String javaScript, String logicalDB) {
		
		ClientScript script = new ClientScript(javaScript,this, logicalDB,this.clientServer);
		script.setProperties(properties);
		clientScripts.add(script);
		return script;
	}
	
	
	
	
	/**
	 * Register an observer against the specified script for the event type.
	 *  
	 * @param script The ClientScript instance that events should be processed from
	 * @param eventType  The type of event that is of interest
	 * @param observer The observer.
	 */
	public void registerObserver(EventSource eventSource,String eventType,ExecutionObserver observer) {
		Map<String,List<ExecutionObserver>> mapForSource = eventListeners.get(eventSource);
		if(mapForSource == null) {
			mapForSource = Collections.synchronizedMap(new HashMap<String,List<ExecutionObserver>>());						
			eventListeners.put(eventSource,mapForSource);
		}
		if(mapForSource.containsKey(eventType)) {
			mapForSource.get(eventType).add(observer);
		}
		else {
			List<ExecutionObserver> list= new LinkedList<ExecutionObserver>();
			list.add(observer);
			mapForSource.put(eventType,list);
		}
	}
	
	/**
	 * Removes an observer from the observer list on a specified source/event type pair.
	 * 
	 * This function only removes the observer from the eventSource on the event type
	 * specified.  If the observer is registered against other event sources or
	 * event types it will remain registered against those.
	 *  
	 */
	public void removeObserver(EventSource eventSource, String eventType, ExecutionObserver observer) {
		Map<String,List<ExecutionObserver>> mapForSource = eventListeners.get(eventSource);
		if(mapForSource != null) {
			List<ExecutionObserver> list =mapForSource.get(eventType);
			if(list != null) {
				list.remove(observer);
			}
		}

	}
	
	/**
	 * Process events.
	 * 
	 * This method will process events until another thread calls the stopProcessing()
	 * method.  This thread will handle of the event processing (invoking the event observers)
	 * 
	 * 
	 */
	public void processEvents() throws TestAbortedException {
		
		//Grab an event from the queue
		Event event=null;		
		synchronized(this) {
			eventProcessingComplete=false;		
		}
		
		while(true) {
			synchronized(this) {
				if(testAborted) {
					//Abort
					throw new TestAbortedException();
				}
				if(!eventQueue.isEmpty()) {
					event = this.eventQueue.remove(0);
				}
				else {
					event=null;
					if(eventProcessingComplete) {
						break;
					}
				}
				if(event==null) {
					try {
						wait();
					}
					catch(InterruptedException e) {
						log.debug(e);
					}				
					continue;
				}
			}
			Map<String,List<ExecutionObserver>> listeners=this.eventListeners.get(event.source);
			if(listeners != null) {
				List<ExecutionObserver> observerList = listeners.get(event.eventName);
				if(observerList != null) {			
					//
					// We copy the observerList because event observers might modify the
					// event list (ie register/unregister observers).
					// We don't want concurrent modification exceptions
					// further we have decided to say that we will process all event observers
					// present when we start processing the event.
					LinkedList<ExecutionObserver> observerCopy =new LinkedList<ExecutionObserver>(observerList); 
					for(Iterator<ExecutionObserver> iter=observerCopy.listIterator(); iter.hasNext();) {
						iter.next().onEvent(event.source, event.eventName);
					}
				}//observerList!=null
			}//listeners!=null
		}
		
	}
	
	/**
	 * Adds an event to the queue.
	 * @param event
	 */
	public void queueEvent(Event event) {
		
		synchronized(this) {
			if(!eventProcessingComplete) {
				this.eventQueue.add(event);
				log.trace("event queue is " + Integer.toString(eventQueue.size()));
				notify();
			}
		}
	}
	
	
	/**
	 * This method signals the coordinator to tell it to stop processing events.
	 * 
	 * Any pending events in the event queue will be processed (by the event processing thread)
	 * which will then exit the processingEvents() method once the event queue is empty.
	 * 
	 * This method returns after the flag is set, it does not wait for event processing to finish.
	 * 
	 */
	public void stopProcessing() { 
		synchronized(this) {
			eventProcessingComplete=true;
			notify();
		}
		
	}
	
	public void clearListeners() {
		this.eventListeners.clear();
	}
	
	public String readFile(String fileName) throws IOException {
	    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
		String line;
		StringBuffer buffer = new StringBuffer();
		while( (line=reader.readLine())!=null) {
			buffer.append(line);
			buffer.append("\n");
		}
		return buffer.toString();
	}
	
	/**
	 * Creates a timer that will fire in seconds seconds.
	 * 
	 * When the timer task runs it will generate an event of type TIMER to for the
	 * observer.
	 * 
	 * @param seconds The number of seconds that the timer should fire in
	 * @param observer The observer that should receive notification of the event the timer
	 *        fires.
	 */
	public TimerHandle addTimerTask(String name,long seconds, ExecutionObserver observer) {
		TimerHandle handle = new TimerHandle(name,this);
		
		//Note: We currently are never removing the timer observer
		//even though it can only fire once.
		//It will stay in the event list like all event observer under the
		//obesrver list is cleared.  This should be fixed.
		registerObserver(handle,EVENT_TIMER,observer);
		timer.schedule(handle, seconds*1000);
		return handle;
		
	}
	
	
	public void scriptComplete(ClientScript script) {
		clientScripts.remove(script);
		
	}
	public void stopScripts() {
		// As we are stopping scripts the scripts might try to 
		// unregister themselves from the coordinator.
		// Since the lists can change we make sure to get a copy first
		//
		Set<ClientScript> clientScriptCopy = null;
		synchronized(clientScripts) {
			clientScriptCopy = new HashSet<ClientScript>(clientScripts);
		}
		for(Iterator<ClientScript> scripts = clientScriptCopy.iterator(); scripts.hasNext();) {
			ClientScript script = scripts.next();
			script.stop();
		}
	
		
		Set<ShellExecScript> shellProcessCopy = null;
		synchronized(shellProcesses) {
			shellProcessCopy =new HashSet<ShellExecScript>(shellProcesses);
		}
	
		for(Iterator<ShellExecScript> scripts = shellProcessCopy.iterator(); scripts.hasNext();) {
			ShellExecScript script = scripts.next();
			script.stop();		
		}
		
		//Also shutdown any active JDBC resources that the coordinator manages.
		for(Iterator<Statement> statements = jdbcStatements.iterator(); statements.hasNext();) {
			Statement stat = statements.next();
			try {
				stat.close();
			}
			catch(SQLException e) {
				log.info("exception cleaning up JDBC resources",e);
			}
		}
		for(Iterator<Connection> connections = jdbcConnections.iterator(); connections.hasNext();) {
			Connection con = connections.next();
			try {
				con.close();
			}
			catch(SQLException e) {
				log.info("exception cleaning up JDBC resources",e);
			}
		}
	}
	
	public SlonikScript createSlonik(String name, String preamble, String script) {
		SlonikScript slonik = new SlonikScript(name,preamble,this,properties);
		slonik.setScript(script);
		shellProcesses.add(slonik);
		return slonik;
	}
	
	
	/**
	 * Called when the test script is complete to cleanup any leftover resources.
	 */
	public void shutdown() {
		stopScripts();
		timer.cancel();
		if(clientServer != null) {
			clientServer.stop();
		}
	}
	
	public CompareOperation createCompareOperation(String logicalDb1, String logicalDb2, String query,String pkey)throws SQLException {
		
		
		Driver jdbcDriver = DriverManager.getDriver("jdbc:postgresql://");
		CompareOperation compareOp = new CompareOperation(properties,jdbcDriver,this,pkey);
		compareOp.setQuery(query);
		compareOp.setDatabases(logicalDb1,logicalDb2);
		return compareOp;
	}
	
	/**
	 * 
	 * Returns a new JDBC Connection object connected to the logical database.
	 */
	public Connection createJdbcConnection(String logicalDb) throws SQLException  {
		Driver jdbcDriver = DriverManager.getDriver("jdbc:postgresql://");
		return JDBCUtilities.getConnection(jdbcDriver, properties, logicalDb);
	}
	
	
	public SlonLauncher createSlonLauncher(String logicalDb) {
		SlonLauncher slon = new SlonLauncher(this,properties,logicalDb,null);
		shellProcesses.add(slon);
		return slon;
	}
	public SlonLauncher createSlonLauncher(String logicalDb, String logshippingDirectory) {
		SlonLauncher slon = new SlonLauncher(this,properties,logicalDb,logshippingDirectory);
		shellProcesses.add(slon);
		return slon;
	}
	
	public CreateDbScript createCreateDb(String logicalDbName) {
		CreateDbScript createdb = new CreateDbScript(logicalDbName,this,properties);
		shellProcesses.add(createdb);
		return createdb;
	}
	public PsqlCommandExec createPsqlCommand(String logicalDbName, String SQLScript) {
		PsqlCommandExec psql = new PsqlCommandExec(logicalDbName,SQLScript,this,properties);
		shellProcesses.add(psql);
		return psql;
	}
	
	public PsqlCommandExec createPsqlCommand(String logicalDbName, File SQLFile) {
		PsqlCommandExec psql = new PsqlCommandExec(logicalDbName,SQLFile,this,properties);
		shellProcesses.add(psql);
		return psql;
	}
	
	
	public DropDbScript createDropDbCommand(String logicalDbName) {
		DropDbScript psql = new DropDbScript(logicalDbName,this,properties);
		shellProcesses.add(psql);
		return psql;
	}
	
	public PgDumpCommand createPgDumpCommand(String logicalDbName, String outputFile,String[] tableList,boolean dataOnly) {
		PgDumpCommand pgdump = new PgDumpCommand(logicalDbName,this,properties,outputFile,tableList,dataOnly);
		shellProcesses.add(pgdump);
		return pgdump;
	}
	
	public LogShippingDumpScript createLogShippingDump(String logicalDbName, File outputFile) {
		
		LogShippingDumpScript dump  = new LogShippingDumpScript(this,this.properties,logicalDbName,outputFile);
		shellProcesses.add(dump);
		return dump;
	}
	
	public LogShippingDaemon createLogShippingDaemon(String logicalDbName, File spoolDirectory) {
		LogShippingDaemon logShipping = new LogShippingDaemon(this,this.properties,logicalDbName,spoolDirectory);
		shellProcesses.add(logShipping);
		return logShipping;
		
	}
	
	/**
	 * 
	 * Mark the lock identified by lockName as being obtained.
	 * 
	 * The coordinator maintains a list of locks that are shared
	 * between all of the clients of the test coordinator.
	 * 
	 * ClientScripts can remotely request and release these locks
	 * that can also be requested and released by the test coordinator threads.
	 * This allows for synchronization between scripts remote scripts.
	 * 
	 * @param lockName The name of the lock to obtain.  These names can be
	 * any string.
	 * 
	 */
	public void obtainLock(String lockName) {
		try {
			
			Lock lock=null;
			synchronized(lockMap) {
				lock = lockMap.get(lockName);
				if(lock==null) {
					lock = new ReentrantLock();
					lockMap.put(lockName,lock);
				}
			}//synchronized
			lock.lock();
		}
		finally {
			
		}
		
	}
	
	/**
	 *  Releases a lock obtained via obtainLock
	 * 
	 */
	public void releaseLock(String lockName) {
		synchronized(lockMap) {
			Lock lock = lockMap.get(lockName);
			if(lock != null) {
				lock.unlock();
			}
		}
	}
	
	
	/**
	 * Aborts the test.
	 * Sometimes an error in a test will occur that is severe enough that
	 * makes no sense to continue with the rest of the test.
	 * 
	 * This method helps to facilitate ending the test quickly.
	 * 
	 * In particular it will mark things such that
	 * \li no further events will be processed
	 * \li Any active client processes will be told to stop
	 * \li An Error will be thrown that should get propogated
	 * \li up past the top of the test case.
	 *  
	 * 
	 */
	public synchronized void abortTest(String message) {
		log.error("Aborting the test due to " + message);
		testAborted=true;
		eventProcessingComplete=true;
		stopScripts();
		timer.cancel();
		notifyAll();
		
		
	}
	
	/**
	 * Waits for eventSource to complete.
	 * This thread will continue to process events while waiting.
	 * 
	 * @returns true if eventSource has finished when this method exits
	 *          or false if the method is exiting for some other reason.
	 *          if stopProcessing() was called by an event handler then the
	 *          join() can exit even though eventSource is not finished.
	 */
	public boolean join(EventSource eventSource) throws TestAbortedException  {
		//
		// We want to wait on eventSource to finish.
		// We have to consider a number of cases
		// 1. eventSource might already be finished
		// 2. eventSource might not yet be finished but might
		//    finish after we check its finished state
		// 3. It will finish at some time in the future.
		//
		// We first register an EVENT_FINISHED listener
		// If the eventSource has not yet finished then
		// the listener execute when the source does finish.
		log.debug("waiting for a process to finish");
		ExecutionObserver observer = new ExecutionObserver() {
			public void onEvent(EventSource s,String eventName) {
				//The process has finished.				
				//Remove this listener
				Coordinator.this.removeObserver(s, EVENT_FINISHED, this);
				Coordinator.this.stopProcessing();
			}
		};
		
		registerObserver(eventSource,EVENT_FINISHED,observer);
		if(!eventSource.isFinished()) {
			processEvents();
		}
		removeObserver(eventSource,EVENT_FINISHED,observer);
		log.debug("join is finished");
		return eventSource.isFinished();
		
	}
	
	/**
	 * Logs a message to the test log.
	 * 
	 * This method is intended to allow test coordinator scripts (the javascript) to 
	 * write messages out (ie status, progress etc...) to the test output results.
	 * 
	 * @param message The message to log.
	 */
	public void log(String message) {
		//Right now we just send this to log4j at the info level, this might change.
		log.info(message);
	}
	
//	/**
//	 * Creates a JDBC Connection that is attached to the logical database specified.
//	 *  
//	 * @param logicalDB The logical database that the JDBC connection should be connected to.
//	 * @return A JDBC Statement object.
//	 */
//	public Connection createJDBCConnection(String logicalDB) throws SQLException {
//		String dbPort =  properties.getProperty("database." + logicalDB + ".port");
//		String dbName =  properties.getProperty("database." + logicalDB + ".dbname");
//		String dbHost =  properties.getProperty("database."+ logicalDB + ".host");
//		
//		String jdbcURL = "jdbc:postgresql://" + dbHost;
//		if(dbPort != null) {
//				jdbcURL += ":" + dbPort;
//		}
//		jdbcURL += "/"+ dbName;
//		String jdbcUser = properties.getProperty("database."+logicalDB + ".user");
//		String jdbcPassword = properties.getProperty("database."+logicalDB + ".password");
//		
//				
//		Driver driver = DriverManager.getDriver(jdbcURL);
//		Properties jdbcProperties = new Properties();
//		jdbcProperties.put("password",jdbcPassword);
//		jdbcProperties.put("user",jdbcUser);
//		Connection con = driver.connect(jdbcURL,jdbcProperties);
//		
//		jdbcConnections.add(con);
//		
//		return con;
//		
//	}
	
}
