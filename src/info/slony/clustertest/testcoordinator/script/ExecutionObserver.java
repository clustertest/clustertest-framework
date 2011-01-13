package info.slony.clustertest.testcoordinator.script;

import info.slony.clustertest.testcoordinator.EventSource;



/**
 * This interface allows processes to monitor the execution of an embedded script.
 * 
 *
 */
public interface ExecutionObserver {
	
	
	public void onEvent(EventSource executor,String eventName);
}
