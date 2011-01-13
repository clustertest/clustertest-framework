/* ----------
 * class ClientTelnetConnection
 *
 *	Implements the telnet style ClientCoordinator command shell.
 *
 *	The ClientTelnetConnection creates a thread that runs a Rhino
 *	JavaScript interpreter, executing what is sent through the input
 *	steam and sending back the interpreter results on the output.
 * ----------
 */
package info.slony.clustertest.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.reflect.InvocationTargetException;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.mozilla.javascript.Context;
import org.mozilla.javascript.Scriptable;
import org.mozilla.javascript.ScriptableObject;

public class ClientTelnetConnection {
	private static Logger log = Logger.getLogger(ClientTelnetConnection.class);
	public BufferedReader	in;			// Buffered reader for line based input
	public PrintWriter	out;			// Output channel
	Shell	shell;			// Instance of the actual work thread
	Thread				shellThread;	// Thread handle
	private Boolean		done;			// Flag set when thread exits
	private QueuedOutputStream outputStreamQueue;
	
	/**
	 * 
	 * This class provides an OutputStream object to the 
	 * ClienEngine + children that queues up write() requests
	 * and then provides a consumer thread that transfers those
	 * write requests to the real OutputStream object.
	 * 
	 * This is being done for two reasons:
	 * 
	 * 1) The PipedOutputStream and PipedInputStream classes used when 
	 *    ClientTelnetConnection is invoked in the same JRE as the 
	 *    test coordinator do not work well if multiple threads write
	 *    to the OutputStream (specifically if a thread that has written
	 *    to the OutputStream later dies before all input is consumed)
	 *    
	 *  2) To prevent worker threads from being slowed down by a backlog of
	 *     output waiting to be consumed by the test coordinator.
	 * 
	 */
	private class QueuedOutputStream extends OutputStream {

		/**
		 * The queue.
		 */
		private LinkedList<byte[]> queue =new  LinkedList<byte[]>();
		private Thread myThread;
		private volatile boolean stopped=false;
		
		public QueuedOutputStream(final OutputStream destination) {
			myThread = new Thread(new Runnable() {
				public void run() {
					/**
					 * Loop until we are stopped, pulling items off the queue.
					 */
					while(!stopped) {
					
						try {
							synchronized(QueuedOutputStream.this) {
								if(queue.isEmpty()) {
									QueuedOutputStream.this.wait();
								}
								else {
									byte[] line = queue.remove(0);
									destination.write(line);
								}
							}//synchronized
						}//try
						catch(IOException e) {
							log.error("error writing output",e);
						}
						catch(InterruptedException e) {
							log.error("thread interruppted",e);
						}
					}//while
					try {
						destination.close();
					}
					catch(IOException e) {
						log.error("ioexception closing stream",e);
					}
				}//run
				
			},"QueuedOutputStream");
			myThread.start();
			
		}
		
		/**
		 * The stop method is used to signal the monitoring to stop
		 * and close the output socket.
		 */
		 public void stop() {
			stopped=true;
			synchronized(this) {
				this.notify();
			}
		}
		 
		 /**
		  * The close method of OutputStream.
		  * We do not do anything in this method - The users of the output stream
		  * should not be closing the stream for all threads.
		  */
		@Override
		public void close() throws IOException {
			
			
		}

		/**
		 * This method currently does nothing because the threads calling flush()
		 * are disconnected from the thread that writes to the real OutputStream.
		 * 
		 * 
		 */
		@Override
		public void flush() throws IOException {
			
		}

		@Override
		public void write(byte[] cbuf, int off, int len) throws IOException {
			
			byte outbuf[] = new byte[len];
			for(int idx=0; idx <len; idx++) {
				//We copy the output because the caller
				//might change the contents of the array after
				//the write call returns.
				outbuf[idx] = cbuf[off+idx];
			}
			synchronized(this) {
				queue.add(outbuf);
				this.notify();
			}
			
			
			
		}
		public void write(int c) throws IOException {
			byte [] array=new byte[4];
			
			/**
			 * Java is big endian
			 * We convert the integer to a byte format
			 * Is there a faster way of doing this?
			 * 
			 * In practice we hope this method isn't being called much
			 * since writing 4 bytes at a time isn't very efficient.
			 * 
			 */
			array[3] = (byte) c;
			array[2] = (byte) (c>>0x8);
			array[1] = (byte) (c>>0x10);
			array[0] = (byte) (c>>0x18);
			synchronized(this) {
				queue.add(array);
				this.notify();
			}
			
		}
		
	};

	
	public ClientTelnetConnection (InputStream inStream, OutputStream outStream) {
		/* ----
		 * Initialize global variables
		 * ----
		 */
		done = false;
		outputStreamQueue = new QueuedOutputStream(outStream);
		in = new BufferedReader(new InputStreamReader(inStream));
		out = new PrintWriter(outputStreamQueue, true);

		/* ----
		 * Create the thread handling this connection
		 * ----
		 */
		shell = new Shell();
		shellThread = new Thread(shell);

		shellThread.start();
	}

	/* ----
	 * isDone()
	 *
	 *	Query the flag that is set when the command shell thread exits
	 * ----
	 */
	public synchronized Boolean isDone () {
		return done;
	}

	/* ----
	 * setDone()
	 *
	 *	Set the flag that is set when the command shell thread exits
	 * ----
	 */
	private synchronized void setDone () {
		done = true;
	}

	/* ----
	 * waitfor()
	 *
	 *	Wait for the command shell thread to terminate, then close
	 *	the BufferedReader and PrintWriter, we created around the
	 *	IO streams.
	 * ----
	 */
	public void waitfor () {
		try {
			shellThread.join();
		} catch (InterruptedException e) {
			log.error("IOExeption: " + e.getMessage(),e);
		}
	}

	/* ----
	 * Shell
	 *
	 *	Implementation of the thread handling one test coordinator
	 *	connection. 
	 * ----
	 */
	protected class Shell implements Runnable {
		ClientEngine	engine;
		Boolean			quit;

		protected Shell () {
			engine = null;
			quit = false;
		}

		public void run () {
			telnetShell();

			if (engine != null)
				engine.shutdown();

			setDone();
			try {
				in.close();
			} catch (IOException e) {
				log.error("IOExeption: " + e.getMessage(),e);
			}
			outputStreamQueue.stop();
		}

		public synchronized Boolean getQuit () {
			return quit;
		}

		public synchronized void setQuit () {
			quit = true;
		}

		private void telnetShell () {
			String			inputLine;
			String			inputBuffer = "";
			int				lineCount = 0;
			int				lineBegin = 1;

			Context				jsContext;
			ScriptableObject	jsScope;
			Object				result;

			/* ----
			 * Create a new Rhino interpreter for this telnet session
			 * ----
			 */
			jsContext = Context.enter();
			jsScope = jsContext.initStandardObjects();

			try {
				engine = new ClientEngine(in, out, jsScope, this);
			} catch (Exception e) {
				log.error(e.getMessage(),e);
				return;
			}

			/* ----
			 * Expose the output channel and make the ClientGroup
			 * Class available in the JS scope.
			 * ----
			 */
			Object wrappedOut = Context.javaToJS(out, jsScope);
			ScriptableObject.putProperty(jsScope, "out", wrappedOut);

			Class<? extends Scriptable> clazz;
			clazz = (Class<? extends Scriptable>)ClientGroup.class;

			try {
				ScriptableObject.defineClass(jsScope, clazz);
			} catch (IllegalAccessException e) {
				log.error("IllegalAccessException defining class - " + e.getMessage(),e);
				return;
			} catch (InstantiationException e) {
				log.error("InstantiationException defining class - " + e.getMessage(),e);
				return;
			} catch (InvocationTargetException e) {
				log.error("InvocationTargetException defining class - " + e.getMessage(),e);
				return;
			}

			/* ----
			 * Define the global functions that we provide in the Engine.
			 * ----
			 */
			String[] globalFuncs = {
				"quit",
				"random",
				"nurand",
				"digsyl",
				"sleep",
				"lockObtained"
			};
			jsScope.defineFunctionProperties(globalFuncs,
					ClientEngine.class, ScriptableObject.DONTENUM);

			try {
				/* ----
				 * Consume the input from the test coordinator
				 * ----
				 */
				while (!getQuit() && ((inputLine = in.readLine()) != null)) {
					/* ----
					 * Add all input lines into the inputBuffer until we
					 * have a compilable string.
					 * ----
					 */
					lineCount++;
					inputBuffer = inputBuffer + inputLine + "\n";
					if (!jsContext.stringIsCompilableUnit(inputBuffer))
						continue;

					/* ----
					 * Evaluate that in the JS context. If it does return
					 * something other than undefined, send it to the
					 * coordinator.
					 * ----
					 */
					try {
						result = jsContext.evaluateString(jsScope, inputBuffer,
									"<shell>", lineBegin, null);
						if (result != jsContext.getUndefinedValue())
							out.println(Context.toString(result));
					} catch (Exception e) {
						out.println(e.getMessage());
						log.error(e.getMessage(),e);
					}
					inputBuffer = "";
					lineBegin += lineCount;
					lineCount = 0;
				}
				/* ----
				 * At the end of the script there may be something left in
				 * the input buffer. We don't want to silently suppress
				 * any errors resulting from a truncated file.
				 * ----
				 */
				if (!inputBuffer.equals("")) {
					try {
						result = jsContext.evaluateString(jsScope, inputBuffer,
									"<shell>", lineBegin, null);
						if (result != jsContext.getUndefinedValue())
							out.println(Context.toString(result));
					} catch (Exception e) {
						out.println(e.getMessage());
						log.error(e.getMessage(),e);
					}
				}
			} catch (IOException e) {
				log.error("IOExeption: " + e.getMessage(),e);
			}
		}
	}
	
	
}
