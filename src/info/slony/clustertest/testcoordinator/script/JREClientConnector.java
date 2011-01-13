package info.slony.clustertest.testcoordinator.script;

import info.slony.clustertest.client.ClientTelnetConnection;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;

public class JREClientConnector implements ClientConnector {

	private ClientTelnetConnection client =null;
	private PipedInputStream toClient_inPipeStream = new PipedInputStream();
	private PipedOutputStream toClient_outPipeStream = new PipedOutputStream();
	private PipedOutputStream fromClient_outPipeStream=new PipedOutputStream();
	private PipedInputStream fromClient_inPipeStream=new PipedInputStream();
	
	
	JREClientConnector() throws IOException {
		
		toClient_outPipeStream.connect(toClient_inPipeStream);
		fromClient_inPipeStream.connect(fromClient_outPipeStream);
		
		
		client=new ClientTelnetConnection(toClient_inPipeStream,fromClient_outPipeStream);
		
	}
	public InputStream getInputStream() {
		return fromClient_inPipeStream;
	}
	public OutputStream getOutputStream() {
		return toClient_outPipeStream;
	}
	
	public void stop() {
		
	}
}
