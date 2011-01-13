package info.slony.clustertest.testcoordinator.script;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.channels.Selector;

/**
 * 
 * The WorkerPoolClientConnector allows jobs(javascripts to run) to be sent to a waiting
 * worker client.
 * 
 * Worker client connect to the server and wait for scripts to be assigned to them.
 *
 * 
 */
public class WorkerPoolClientConnector implements ClientConnector {

	private InputStream inputStream;
	private OutputStream outputStream;
	private ClientWorkerServer server;
	private Socket socket;
	
	public WorkerPoolClientConnector(ClientWorkerServer server)  throws IOException {
		this.server=server;
		socket = server.getWorker();
		if(socket != null) {
			inputStream=this.socket.getInputStream();
			outputStream = this.socket.getOutputStream();
		}
		
	}
	
	public InputStream getInputStream() {
		return inputStream;
	}
	public OutputStream getOutputStream() {
		return outputStream;
	}
	public void waitfor()throws IOException {
		
		
	}
	public void stop() {
		server.releaseWorker(socket);
	}
	
}
