/* ----
 * ClientGroup
 *
 * ----
 */
package info.slony.clustertest.client;

import java.io.PrintWriter;
import java.io.BufferedWriter;
import java.io.FileWriter;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;
import org.mozilla.javascript.Undefined;


public class ClientGroup extends ScriptableObject {
	
	private static Logger log = Logger.getLogger(ClientGroup.class);
	
	protected String			groupName;		// Name of this group
	private ClientEngine	engine;			// Beck reference to engine
	private int				numWorkers;		// Number of worker threads
	private String			className;		// JS Class to use for this group
	private String			transactionMix;	// Original form of transactions
	private String[]		transactions;	// List of transactions to run
	private String			dbUri;			// DB connection string
	private String			dbUser;			// DB username
	private String			dbPass;			// DB password
	private int				numTransWorker;	// # of transactions to run
	private long			runUntil;		// Time to run
	private long			minSleep;		// Nap time between transactions
	private long			maxSleep;		// ditto
	private long			reconnectSleep;	// Nap after errors
	private Boolean			isRunning;		// Group is active
	private PrintWriter		out;			// Output for debugging
	private ClientWorker[]	workers;
	private Thread[]		threads;
	private String			timingDir = null; // Directory where to place 
											  // the CSV files with the
											  // transaction timing data.
	private Boolean			timingAppend = false;

	public ClientGroup ()
			throws Exception {
		this("unnamed");
	}

	public ClientGroup (String name) 
			throws Exception {
		groupName		= name;
		engine			= ClientEngine.getLocalEngine();
		if (engine == null) {
			throw new Exception("Thread did not create a ClientEngine");
		}
		numWorkers		= 1;
		numTransWorker	= -1;
		runUntil		= -1;
		minSleep		= 0;
		maxSleep		= 0;
		reconnectSleep	= 1000;
		isRunning		= false;
		out				= engine.out;

		engine.registerGroup(this);
	}

	public static ClientGroup
	jsConstructor(Context cx, Object[] args, Function funObj, boolean isNew) 
			throws Exception {
		if (args.length == 0) {
			return new ClientGroup();
		}
		if (args.length == 1) {
			Object	name = args[0];
			if (name instanceof org.mozilla.javascript.Wrapper) {
				name = ((org.mozilla.javascript.Wrapper)name).unwrap();
			}
			if (!(name instanceof String)) {
				Context.reportRuntimeError("Bad argument " + name);
				return null;
			}
			return new ClientGroup((String)name);
		}
		Context.reportRuntimeError("unsupported number of arguments");
		return null;
	}

	@Override public String 
	getClassName() {
		return "ClientGroup";
	}

	/* ----
	 * name - readonly attribute groupName
	 * ----
	 */
	public String
	jsGet_name() {
		return groupName;
	}

	/* ----
	 * numWorkers - readonly while running
	 * ----
	 */
	public synchronized int
	jsGet_numWorkers() {
		return numWorkers;
	}

	public synchronized void
	jsFunction_setNumWorkers(int n) 
			throws Exception {
		if (isRunning) {
			throw new Exception("Number of workers cannot be changed " +
					"while group is running");
		}
		numWorkers = n;
	}

	/* ----
	 * className - readonly while running
	 * ----
	 */
	public synchronized String
	jsGet_className() {
		return className;
	}

	public synchronized void
	jsFunction_setClassName(String c) {
		if (isRunning) {
			Context.reportRuntimeError("className cannot be changed " +
					"while group is running");
			return;
		}
		className = c;
	}

	/* ----
	 * Timing directory functions
	 * ----
	 */
	public synchronized void
	jsFunction_setTimingDir(String path, Boolean append) {
		timingDir = path;
		timingAppend = append;
	}

	/* ----
	 * dbUri  - read only attribute (use setDb() to change)
	 * dbUser - read only attribute (use setDb() to change)
	 * dbPass - read only attribute (use setDb() to change)
	 * ----
	 */
	public synchronized String
	jsGet_dbUri() {
		return dbUri;
	}

	public synchronized String
	jsGet_dbUser() {
		return dbUser;
	}

	public synchronized String
	jsGet_dbPass() {
		return dbPass;
	}

	public synchronized String
	getDbUri() {
		return dbUri;
	}

	public synchronized String
	getDbUser() {
		return dbUser;
	}

	public synchronized String
	getDbPass() {
		return dbPass;
	}

	/* ----
	 * setDb() - change all three connection parameters at once
	 * ----
	 */
	public synchronized void 
	setDb(String uri, String user, String pass) {
		dbUri	= uri;
		dbUser	= user;
		dbPass	= pass;
	}

	public void
	jsFunction_setDb(String uri, String user, String pass) {
		setDb(uri, user, pass);
	}

	/* ----
	 * setTransactionMix()
	 *
	 *	Expand the given string into the transaction mix array
	 * ----
	 */
	public void setTransactionMix(String transString)
			throws Exception {
		if (running()) {
			throw new Exception("Cannot change transaction mix " +
					"while group is running");
		}

		/* ----
		 * Expand the comma separated transaction mix list
		 * ----
		 */
		String[] tmpList = transString.split(",");
		int n = 0;
		for (int i = 0; i < tmpList.length; i++) {
			String[] tmpTrans = tmpList[i].split("=");
			if (tmpTrans.length > 1) {
				n += Integer.parseInt(tmpTrans[1]);
			} else {
				n++;
			}
		}
		if (n == 0)
			throw new Exception ("transaction list cannot be empty");
		transactions = new String[n];
		n = 0;
		for (int i = 0; i < tmpList.length; i++) {
			int m;
			String[] tmpTrans = tmpList[i].split("=");
			if (tmpTrans.length > 1) {
				m = Integer.parseInt(tmpTrans[1]);
			} else {
				m = 1;
			}
			for (int j = 0; j < m; j++) {
				transactions[n++] = tmpTrans[0].trim();
			}
		}

		transactionMix = transString;
	}

	public void
	jsFunction_setTransactionMix(String mix) 
			throws Exception {
		setTransactionMix(mix);
	}

	public String
	jsGet_transactions() {
		return transactionMix;
	}

	/* ----
	 * launch()
	 *
	 *	Creates and starts the configured number of worker threads for
	 *	this group.
	 * ----
	 */
	public synchronized void
	launch (int nTransWorker, long mSeconds) 
			throws Exception {
		if (isRunning) {
			throw new Exception("group is already running");
		}

		if (className == null) {
			throw new Exception("no className set");
		}

		if (transactions == null || transactions.length == 0) {
			throw new Exception("no transaction mix set");
		}

		numTransWorker	= nTransWorker;
		if (mSeconds >= 0) {
			runUntil = System.currentTimeMillis() + mSeconds;
		} else {
			runUntil = -1;
		}

		workers = new ClientWorker[numWorkers];
		threads = new Thread[numWorkers];
		for (int i = 0; i < numWorkers; i++) {
			workers[i] = new ClientWorker(this, nTransWorker, runUntil,
						groupName + "_" + i);
			threads[i] = new Thread(workers[i]);

			threads[i].start();
		}

		isRunning = true;
	}

	public void
	jsFunction_launch()
			throws Exception {
		launch(-1, -1L);
	}

	public void
	jsFunction_launchSeconds(int s) 
			throws Exception {
		launch(-1, (long)s * 1000L);
	}

	public void
	jsFunction_launchMilliSeconds(int ms)
			throws Exception {
		launch(-1, (long)ms);
	}

	public void
	jsFunction_launchNumTransactions(int n) 
			throws Exception {
		launch(n, -1L);
	}

	/* ----
	 * running() & jsFunction_running()
	 *
	 *	Returns if the client group has been launched and not yet
	 *	waited for.
	 * ----
	 */
	public synchronized Boolean
	running() {
		return isRunning;
	}

	public synchronized Boolean
	jsFunction_running() {
		return isRunning;
	}

	/* ----
	 * waitfor() & jsFunction_waitfor()
	 *
	 *	If the group had been launched, wait for it to finish.
	 * ----
	 */
	public void
	waitfor () {
		if (!isRunning) {
			return;
		}

		for (int i = 0; i < numWorkers; i++) {
			try {
				threads[i].join();
			} catch (InterruptedException e) {
				log.error("ERROR group " + groupName + 
						" thread " + i + e.getMessage(),e);
			}
		}

		isRunning = false;
		workers = null;
		threads = null;
	}

	public void
	jsFunction_waitfor() {
		waitfor();
	}

	/* ----
	 * setSleep()
	 * ----
	 */
	public synchronized void
	setSleep (long min, long max, long reconnect) {
		minSleep		= min;
		maxSleep		= max;
		reconnectSleep	= reconnect;
	}

	public synchronized void
	jsFunction_setSleep (int min, int max, int reconnect) {
		minSleep		= (long)min;
		maxSleep		= (long)max;
		reconnectSleep	= (long)reconnect;
	}

	/* ----
	 * getRandomSleep()
	 * ----
	 */
	private synchronized long
	getRandomSleep() {
		if (minSleep == 0 || maxSleep < minSleep)
			return 0;;
		return random(minSleep, maxSleep);
	}

	/* ----
	 * getReconnectSleep()
	 * ----
	 */
	private synchronized long
	getReconnectSleep() {
		return reconnectSleep;
	}

	protected void
	stop () {
		if (!running()) {
			return;
		}

		long now = System.currentTimeMillis();
		for (int i = 0; i < numWorkers; i++) {
			workers[i].setRunUntil(now);
		}

		waitfor();
	}
	public void
	jsFunction_stop () {
		stop();
	}

	private long random (long min, long max) {
		return (long)(Math.random() * (max - min + 1) + min);
	}

	private class ClientWorker implements Runnable {
		ClientGroup		group;
		int				numTransWorker;
		long			runUntil;
		String			workerName;
		String			className;
		String[]		transactions;
		int				transIdx;
		long			transStart;
		long			transEnd;
		Boolean			transError;

		String			timingFileName;
		PrintWriter		timingWriter = null;

		ClientWorker (ClientGroup myGroup, int nTransWorker, 
				long rUntil, String wName) {
			group			= myGroup;
			numTransWorker	= nTransWorker;
			runUntil		= rUntil;
			workerName		= wName;
			className		= group.className;
			transactions	= new String[group.transactions.length];
			System.arraycopy(group.transactions, 0, transactions, 0,
					group.transactions.length);
			randomizeTransactions();
			transIdx		= 0;
		}

		public void run() {
			int numDoneWorker	= 0;
			String connectCmd	= "";

			log.info("INFO worker " + workerName + " start");

			/* ----
			 * Setup the JS engine to execute our test transactions
			 * ----
			 */
			Context		jsContext = Context.enter();
			Scriptable	jsScope = jsContext.newObject(group.engine.shellScope);
			jsScope.setPrototype(group.engine.shellScope);
			jsScope.setParentScope(null);
			Object		result;

			/* ----
			 * Expose the output channel. Maybe someone wants to send
			 * messages to the terminal or test coordinator.
			 * ----
			 */
			Object wrappedOut = Context.javaToJS(group.out, jsScope);
			ScriptableObject.putProperty(jsScope, "out", wrappedOut);
			
			/**
			 * ---
			 * Expose the ClientGroup to the test script.
			 * A worker script might need to make calls back to the
			 * group.
			 */
			Object wrappedGroup = Context.javaToJS(group.engine, jsScope);
			ScriptableObject.putProperty(jsScope, "myEngine", wrappedGroup);
			

			/* ----
			 * Tell it its NAME and create one instance of the
			 * requested test Class.
			 * ----
			 */
			try {
				jsContext.evaluateString(jsScope,
						"testObject = new " + className + 
								"(\"" + workerName + "\");",
						"<init>", 1, null);
			} catch (Exception e) {
				log.error("FATAL worker " + workerName +
						e.getMessage(),e);
				out.println("FATAL: worker " + workerName + e.getMessage());
				return;
			}

			jsContext.evaluateString(jsScope,"testObject.engine=myEngine","<init>",1,null);

			/* ----
			 * If requested, create the timing CSV file
			 * ----
			 */
			if (timingDir != null) {
				timingFileName = timingDir + "/" + workerName + ".csv";
				try {
					timingWriter = new PrintWriter(new BufferedWriter(
						new FileWriter(timingFileName, timingAppend)));
												
				} catch (Exception e) {
					log.error("FATAL worker " + workerName +
							e.getMessage(),e);
					out.println("FATAL: worker " + workerName + e.getMessage());
					return;
				}
			}
			
			while (true) {
				/* ----
				 * Check if the work is done
				 * ----
				 */
				if (numTransWorker > 0 && numDoneWorker >= numTransWorker)
					break;
				if (runUntil > 0 && System.currentTimeMillis() >= runUntil)
					break;

				/* ----
				 * We try to connect to the database any time we
				 * either don't have a connection, or if some of
				 * the connection parameters have changed. The test
				 * coordinator may do this while the test is running.
				 * connectCmd is an empty string if we don't have a
				 * connection or the connect() JS string we used last
				 * to establish a connection.
				 * ----
				 */
				String connectTmp;
				connectTmp = "testObject.connect(\"" + group.getDbUri() +
						"\", \"" + group.getDbUser() +
						"\", \"" + group.getDbPass() + "\");";
				if (!connectCmd.equals(connectTmp)) {
					/* ----
					 * Need to (re)connect. Disconnect first if we have
					 * a connection.
					 * ----
					 */
					if (!connectCmd.equals("")) {
						try {
							jsContext.evaluateString(jsScope,
									"testObject.disconnect();",
									"<disconnect>", 1, null);
							log.info("INFO worker " + workerName +
								" disconnect");
						} catch (Exception e) {
							log.error("ERROR worker " + workerName +
								" disconnect: " + e.getMessage(),e);
							out.println("ERROR: worker " + workerName + " disconnect:" + e.getMessage() );
						}
						connectCmd = "";
					}
					try {
						jsContext.evaluateString(jsScope,
								connectTmp,
								"<connect>", 1, null);
					} catch (Exception e) {
						String message ="ERROR worker " + workerName +
						" connect: " + e.getMessage();
						log.error(message,e);
						out.println(message);
						try {
							Thread.sleep(group.getReconnectSleep());
						} catch (InterruptedException e2) {
						}
						continue;
					}
					connectCmd = connectTmp;
					log.info("INFO worker " + workerName +
						" connect");
				}

				if (transIdx == transactions.length) {
					randomizeTransactions();
					transIdx = 0;
				}

				transStart = System.currentTimeMillis();
				try {
					jsContext.evaluateString(jsScope,
							"testObject." + transactions[transIdx] + "();",
							"<work>", 1, null);
					transEnd = System.currentTimeMillis();
					transError = false;
				} catch (Exception e) {
					transEnd = System.currentTimeMillis();
					transError = true;
					System.err.println("ERROR: worker " + workerName + " " +
							transactions[transIdx] + "() FAILED: " +
							e.getMessage());
					out.println("ERROR: worker " + workerName + " " +
							transactions[transIdx] + "() FAILED: " +
							e.getMessage());
					log.error("ERROR: worker " + workerName + " " +
							transactions[transIdx] + "() ",
							e);

					/* ----
					 * Setting connectCmd to invalid will cause the
					 * code above to reconnect to the DB
					 * ----
					 */
					log.info("INFO worker " + workerName + " connection set to invalid");
					connectCmd = "invalid";
				}
				
				numDoneWorker++;

				/* ----
				 * Calculate the nap time if configured.
				 * Subtract the previous transactions execution time
				 * so that we get to an even load more independent
				 * of the servers performance.
				 * ----
				 */
				long ms = group.getRandomSleep() - (transEnd - transStart);
				if (ms < 0) {
					ms = 0;
				}
				/* ----
				 * If requested, write the timing record
				 * ----
				 */
				if (timingWriter != null) {
					timingWriter.println(workerName + "," +
						transactions[transIdx] + "," +
						transError + "," +
						transStart + "," +
						transEnd + "," +
						(transEnd - transStart) + "," +
						ms);
				}

				/* ----
				 * Nap.
				 * ----
				 */
				if (ms > 0) {
					try {
						while (ms > 0) {
							Thread.sleep(ms >= 5000 ? 5000 : ms);
							ms -= 5000;
							if (ms > 0 && runUntil > 0 && 
									System.currentTimeMillis() >= runUntil)
								break;
						}
					} catch (InterruptedException e) {
					}
				} else {
					ms = 0;
				}

				transIdx++;
			}

			/* ----
			 * Close DB connection if we still have one.
			 * ----
			 */
			if (!connectCmd.equals("")) {
				try {
					jsContext.evaluateString(jsScope,
							"testObject.disconnect();",
							"<disconnect>", 1, null);
					log.info("INFO worker " + workerName +
						" disconnect");
				} catch (Exception e) {
					String message = "ERROR:worker " + workerName +
					" disconnect: " + e.getMessage();
					out.println(message);
					log.error(message,e);
				}
			}
			log.info("INFO worker " + workerName + " exit");

			/* ----
			 * Close the timing output file
			 * ----
			 */
			if (timingWriter != null) {
				try {
					timingWriter.close();
				} catch (Exception e) {
					String message = "ERROR:worker " + workerName +
					" close timing: " + e.getMessage();
					out.println(message);
					log.error(message,e);
				}
				timingWriter = null;
			}
		}

		private void
		randomizeTransactions() {
			int		len;
			int		numLoops;
			int		trans1;
			int		trans2;
			String	tmp;

			len = transactions.length;
			for (long i = 0; i < len / 2; i++) {
				trans1 = (int)random(0, len - 1);
				trans2 = (int)random(0, len - 1);

				if (trans1 != trans2) {
					tmp = transactions[trans1];
					transactions[trans1] = transactions[trans2];
					transactions[trans2] = tmp;
				}
			}
		}

		protected synchronized int
		getNumTransWorker() {
			return numTransWorker;
		}

		protected synchronized void
		setNumTransWorker(int n) {
			numTransWorker = n;
		}

		protected synchronized long
		getRunUntil() {
			return runUntil;
		}

		protected synchronized void
		setRunUntil(long t) {
			runUntil = t;
		}
	}
	
	
}

