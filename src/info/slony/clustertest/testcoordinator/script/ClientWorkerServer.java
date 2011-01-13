package info.slony.clustertest.testcoordinator.script;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.channels.IllegalBlockingModeException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;


/**
 * 
 * The ClientWorkerServer allows worker clients to connect (through TCIP/IP sockets)
 * and will store a pool of connected clients available for work.
 * 
 * When a worker client is requested from ClientWorkerServer a socket will be returned that
 * is attached to the next available worker client.
 * 
 */
public class ClientWorkerServer {
	private static Logger log = Logger.getLogger(ClientWorkerServer.class);
	private Properties properties;
	
	/**
	 * The thread that monitors the socket for new connections (this thread calls accept()).
	 */
	private Thread acceptorThread;
	/**
	 * The port to listen for connections on.
	 */
	private int port;
	
	/**
	 * The ServerSocket that is being monitored for new connections.
	 */
	private ServerSocket listenSocket;
	private boolean shuttingDown=false;
	
	/**
	 * A list of sockets for worker clients that have connected and are
	 * waiting for work.
	 */
	List<Socket> waitingClients= new LinkedList<Socket>();
	
	/**
	 * Socket connnections for clients that have been assigned work (returned by this object).
	 */
	Set<Socket> workingClients=new HashSet<Socket>();

	
	public ClientWorkerServer(Properties properties) {
		this.properties = properties;
		String property = this.properties.getProperty("client.server.port","1444");
		
		try {
			port = Integer.parseInt(property);
		}
		catch(NumberFormatException e) {
			log.error("unable to parse client port(integer):" + property);
			throw e;
		}
	}
	
	public void start() {
		acceptorThread = new Thread(new Runnable() {
			public void run() {
				try {
					listenSocket = new ServerSocket(port);
					
					Socket worker=null;
					while((worker=listenSocket.accept())!=null ) {
						log.info("accepted a socket connection from " + worker.getRemoteSocketAddress().toString());
						/**
						 * We want keep-alive set.
						 * A long period of time can pass from when a worker connects until it is needed.
						 * If the worker goes away in the meantime we would like to know this.
						 * 
						 * 
						 */
						worker.setKeepAlive(true);						
						synchronized(ClientWorkerServer.this) {
							waitingClients.add(worker);
							ClientWorkerServer.this.notify();
						}
					}
				}
				catch(IOException e) {
					if(!shuttingDown) {
						log.error("error accepting connection",e);
					}
				}
				catch(SecurityException e) {
					log.error("error accepting connection",e);
				}
				catch(IllegalBlockingModeException e) {
					log.error("error accepting connection",e);
				}
			}
		});
		acceptorThread.start();
	}
	
	public void stop() {
		try {
			log.info("closing down server socket");
			shuttingDown=true;
			listenSocket.close();
			try {
				this.notifyAll();
			}
			catch(IllegalMonitorStateException e) {
				log.warn("exception while shutting down",e);
			}
		}
		catch(IOException e) {
			//This is not really an error
			//We are closing the socket.
			log.debug("exception on close",e);
		}
		synchronized(this) {
			for(Iterator<Socket> iter = workingClients.iterator(); iter.hasNext();) {
				Socket worker = iter.next();
				try {
					worker.close();
				}
				catch(IOException e) {
					
				}
			}
			for(Iterator<Socket> iter = waitingClients.iterator(); iter.hasNext();) {
				Socket worker = iter.next();
				try {
					worker.close();
				}
				catch(IOException e) {
					
				}
			}
		}
	}
	
	public void releaseWorker(Socket worker) {
		synchronized(this) {
			log.info("releasing worker " + worker.getRemoteSocketAddress().toString());
			workingClients.remove(worker);
			try {
				worker.close();
			}
			catch(IOException e) {
				//probably already closed, not interesting
			}
		}
	}
	
	public Socket getWorker() {
		synchronized(this) {
			Socket worker = null;
			while(waitingClients.isEmpty() && !shuttingDown) {
				try {
					log.debug("waiting for a client");
					this.wait();
					if(shuttingDown) {
						return null;
					}
					if(waitingClients.isEmpty()) {
						continue;
					}
					worker = waitingClients.get(0);
			
					if(worker.isInputShutdown() || worker.isOutputShutdown()) {
						//The worker is no longer around.
						waitingClients.remove(0);
						log.info("The worker connection has closed");
						try {
							worker.close();
						}
						catch(IOException e) {
							//expected since the socket is mostly shutdown.
						}
						worker=null;
					}
					
				}
				catch(InterruptedException e) {
					log.info("thread interrupted",e);
				}
				
			}//while
			worker = waitingClients.remove(0);
			workingClients.add(worker);
			log.info("returning a worker");
			return worker;
		}
	}
}
