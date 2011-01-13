/* ----
 * ClientEngine
 *
 * ----
 */
package info.slony.clustertest.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Function;
import org.mozilla.javascript.Scriptable;

public class ClientEngine {
	
	private static Logger log = Logger.getLogger(ClientEngine.class);
	
	protected ClientGroup[]		groupArray;
	private int					gaSize;
	private int					gaUsed;
	private BufferedReader		in;
	protected PrintWriter		out;
	protected Scriptable		shellScope;
	private ClientTelnetConnection.Shell	shell;
	
	
	
	private static ThreadLocal<ClientEngine> localEngine =
				new ThreadLocal<ClientEngine>();
	
	
	/**
	 * A map of all locks used by the engine.
	 * @
	 */
	private Map<String,Semaphore> lockMap = new HashMap<String,Semaphore>();

	
	
		
	ClientEngine (BufferedReader newIn, PrintWriter newOut, Scriptable scope) 
			throws Exception {
		gaSize = 2;
		gaUsed = 0;
		groupArray = new ClientGroup[gaSize];

		in = newIn;
		out = newOut;
		shellScope = scope;

		if (localEngine.get() != null) {
			throw new Exception("Only one ClientEngine supported per thread");
		}
		localEngine.set(this);
	}

	ClientEngine (BufferedReader newIn, PrintWriter newOut, 
			Scriptable scope, ClientTelnetConnection.Shell shell) 
			throws Exception {
		this(newIn, newOut, scope);
		this.shell = shell;
	}

	public static ClientEngine
	getLocalEngine() {
		return localEngine.get();
	}

	/* ----
	 * registerGroup()
	 *
	 *	Add a new connection group to the client configuration
	 * ----
	 */
	protected synchronized void 
	registerGroup(ClientGroup group) {
		/* ----
		 * Enlarge the group array if needed
		 * ----
		 */
		if (gaUsed == gaSize) {
			gaSize *= 2;
			ClientGroup[] tmpGA = new ClientGroup[gaSize];
			System.arraycopy(groupArray, 0, tmpGA, 0, gaUsed);
			groupArray = tmpGA;
		}

		groupArray[gaUsed++] = group;
	}

	protected void
	shutdown() {
		for (int idx = 0; idx < gaUsed; idx++) {
			ClientGroup group = groupArray[idx];

			if (group.running()) {
				log.error("must stop group " + group.groupName);

				group.stop();
			}
		}
	}

	public static Object
	quit (Context cx, Scriptable thisObj, Object[] args, Function funObj) {
		ClientEngine myEngine = getLocalEngine();
		if (myEngine != null) {
			if (myEngine.shell != null) {
				myEngine.shell.setQuit();
			}
		}
		return cx.getUndefinedValue();
	}

	public static Object
	random (Context cx, Scriptable thisObj, Object[] args, Function funObj)
			throws Exception {
		if (args.length != 2) {
			throw new Exception ("wrong number of arguments");
		}
		int		min = (int)(cx.toNumber(args[0]));
		int		max = (int)(cx.toNumber(args[1]));

		return (Object)((int)Math.floor(Math.random() * (max - min + 1) + min));
	}

	public static Object
	nurand (Context cx, Scriptable thisObj, Object[] args, Function funObj)
			throws Exception {
		if (args.length != 3) {
			throw new Exception ("wrong number of arguments");
		}
		int		a = (int)(cx.toNumber(args[0]));
		int		min = (int)(cx.toNumber(args[1]));
		int		max = (int)(cx.toNumber(args[2]));

		int r1 = (int)Math.floor(Math.random() * (a + 1));
		int r2 = (int)Math.floor(Math.random() * (max - min + 1) + min);
		return (Object)(((r1 | r2) % (max - min + 1)) + min);
	}

	public static Object
	digsyl (Context cx, Scriptable thisObj, Object[] args, Function funObj)
			throws Exception {
		if (args.length != 2) {
			throw new Exception ("wrong number of arguments");
		}
		int		num = (int)(cx.toNumber(args[0]));
		int		len = (int)(cx.toNumber(args[1]));
		String[] syl = {"BA", "OG", "AL", "RI", "RE",
						"SE", "AT", "UL", "IN", "NG" };

		String fmt = String.format("%%0%dd", len);
		String digits = String.format(fmt, num);
		byte[] bytes = digits.getBytes();

		String result = "";
		for (int i = 0; i < len; i++) {
			result += syl[bytes[i] - '0'];
		}

		return (Object)result;
	}

	public static Object
	sleep (Context cx, Scriptable thisObj, Object[] args, Function funObj)
			throws Exception {
		if (args.length != 1) {
			throw new Exception ("wrong number of arguments");
		}
		long	ms = (long)(cx.toNumber(args[0]));

		Thread.sleep(ms);

		return cx.getUndefinedValue();
	}
	
	
	/**
	 * This call is intended to be invoked by the ClientTelnetConnection on
	 * request of some external process to indicate that a lock has been granted
	 * to this client engine.
	 * @param cx  The javascript context.
	 * @param thisObj
	 * @param args  arg[0] should be a String containing the name of the lock that was obtained.
	 * @param funObj
	 * @return
	 * @throws Exception
	 */
	public static Object lockObtained(Context cx, Scriptable thisObj, Object[] args,
			Function funObj) throws Exception {
		
		Semaphore lock=null;
		String lockName = cx.toString(args[0]);
		ClientEngine myEngine = localEngine.get();
		synchronized(myEngine.lockMap) {
			lock = myEngine.lockMap.get(lockName);
			if(lock == null) {
				//This is an error
				//We should not be receiving notifications of obtaining a lock
				//that was not requested.
				return cx.getUndefinedValue();
			}
			
		}
		lock.release();
		
		return cx.getUndefinedValue();
	}
	
	/**
	 * Sends a message via stdout to the process that invoked this engine
	 * requesting that the lock identified by lockName be obtained.
	 * 
	 * This call will block until a different thread invokes the lockObtained() method 
	 * for the lock name requested.
	 * 
	 * @param lockName
	 * @throws InterruptedException
	 */
	public void requestLock(String lockName) throws InterruptedException {
		Semaphore lock=null;
		
		synchronized(lockMap) {
			lock = lockMap.get(lockName);
			if(lock == null) {
				lock = new Semaphore(0);
				lockMap.put(lockName,lock);
			}
			
		}//synchronized
		
		out.write("REQUEST_LOCK:" + lockName + "\n");
		out.flush();
		lock.acquire();	
		
		
	}
	/**
	 * Releases the lock by the given name.
	 * 
	 * A message will be sent via stdout to the process that invoked the engine
	 * that it should release the lock identified by lockName on behalf of this
	 * engine.
	 * 
	 * @param lockName
	 */
	public void releaseLock(String lockName) {
		out.write("RELEASE_LOCK:" + lockName + "\n");
		out.flush();
	}
			
	
}

