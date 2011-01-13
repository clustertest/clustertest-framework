package info.slony.clustertest.client;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.log4j.PropertyConfigurator;

public class ClientCoordinator {

	public static void main(String args[]) {
		int		optIdx = 0;
		String	optArg = null;
		int		serverPort = -1;
		int		errors = 0;
		
		/*
		 * The address of the test coordinator to connect to.
		 * 
		 */
		String coordinatorAddress=null;
		/**
		 * The port of the test coordinator to connect to.
		 */
		int  coordinatorPort=1444;
		PropertyConfigurator.configure("conf/log4j.properties");
		/* ----
		 * Parse command line options
		 * ----
		 */
		while (optIdx < args.length) {
			String	opt;
			String	val;
			int		idx;

			if (args[optIdx].charAt(0) != '-') {
				break;
			}

			if ((idx = args[optIdx].indexOf("=")) > 0) {
				opt = args[optIdx].substring(0, idx);
				val = args[optIdx].substring(idx + 1);
			} else {
				opt = args[optIdx];
				val = null;
			}
			optIdx++;

			if (opt.equals("-server")) {
				if (val == null) {
					System.err.println("Option -server requires a port number");
					errors++;
					continue;
				}
				try {
					serverPort = Integer.parseInt(val);
				} catch (Exception e) {
					System.err.println("Value for option -server not a number");
					errors++;
					continue;
				}
				continue;
			}
			else if(opt.equals("-coordinator")) {
				//Invoking as a client.  Requires a server:port to connect to.
				if(val == null) {
					System.err.println("-coordinator requires a server:port t connect to");
					errors++;
					continue;
				}
				int colonIdx = val.indexOf(":");
				if(colonIdx < 0) {
					System.err.println("-client requires a server:port t connect to");
					errors++;
					continue;
				}
				coordinatorAddress = val.substring(0,colonIdx);
				try {
					coordinatorPort = Integer.parseInt(val.substring(colonIdx+1));
				}
				catch(NumberFormatException e) {
					System.err.println("port must be an integer");
					errors++;
					continue;
				}
				continue;
			}
			System.err.println("Unknown option '" + opt + "'");
			errors++;
		}

		if (optIdx != args.length) {
			System.err.println("Extra command line arguments");
			errors++;
		}

		if (errors > 0) {
			System.err.println("");
			System.err.println("usage: ClientCoordinator [options]");
			System.err.println("");
			System.err.println("Options:");
			System.err.println("  -server=<portNumber>");
			System.err.println("  -coordinator=<host>:<port>");
			System.exit(1);
		}

		/* ----
		 * Load the Postgres JDBC driver
		 * ----
		 */
		try {
			Class.forName("org.postgresql.Driver");
		} catch (Exception e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}

		
		if (coordinatorAddress!=null) {
			//Connect to the coordinator through TCP/IP and let it send us work
			try {
				while(true) {
					System.out.println("Starting up a ClientTelnetConnection");
					InetAddress address=InetAddress.getByName(coordinatorAddress);
					InetSocketAddress socketAddress = new InetSocketAddress(address,coordinatorPort);
					Socket socket = new Socket();
					socket.connect(socketAddress);
					ClientTelnetConnection conn;
					conn = new ClientTelnetConnection(socket.getInputStream(), socket.getOutputStream());
					conn.waitfor();
					socket.close();
				}
				
			}
			catch(IOException e) {
				System.err.println("error communicating with test coordinator:" + e.getMessage());
			}
			catch(SecurityException e) {
				System.err.println("error communicating with test coordinator:" + e.getMessage());
			}
		}
		else if (serverPort < 0) {
			/* ----
			 * Create one connection over stdin/stdout and execute it.
			 * Every connection is Telnet style and handled in its own
			 * thread, so we can easily switch to an actual socket server
			 * later.
			 * ----
			 */
			ClientTelnetConnection conn;
			conn = new ClientTelnetConnection(System.in, System.out);
			conn.waitfor();
		}
		else {
			/* ----
			 * Run in server mode. Create the server socket, accept
			 * incoming connections and spawn off a new ClientTelnetConnection
			 * for each of them.
			 * ----
			 */
			ServerSocket	serverSocket;
			Socket			testCoordinator;

			try {
				serverSocket = new ServerSocket(serverPort);
				while (true) {
					testCoordinator = serverSocket.accept();
					new ClientTelnetConnection(
							testCoordinator.getInputStream(),
							testCoordinator.getOutputStream());
					testCoordinator = null;
				}
			} catch (IOException iox) {
				System.err.println(iox.getMessage());
				System.exit(2);
			}
		}
	}
}
