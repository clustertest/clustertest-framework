package info.slony.clustertest.testcoordinator.script;


import java.io.InputStream;
import java.io.OutputStream;

public interface ClientConnector {

	public InputStream getInputStream();
	public OutputStream getOutputStream();
	public void stop();
}
