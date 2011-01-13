package info.slony.clustertest.testcoordinator.script;

import info.slony.clustertest.testcoordinator.Coordinator;
import info.slony.clustertest.testcoordinator.Event;
import info.slony.clustertest.testcoordinator.EventSource;

import java.util.TimerTask;

import org.apache.log4j.Logger;

public class TimerHandle extends TimerTask implements EventSource {
	private Coordinator coordinator;
	private String name;
	private static Logger log = Logger.getLogger(TimerHandle.class);
	private  boolean isFinished=false;
	public TimerHandle(String name, Coordinator coordinator) {
		this.coordinator=coordinator;
		this.name=name;
	
	}
	
	public void run() {
		Event event = new Event();
		event.source=this;
		event.eventName=Coordinator.EVENT_TIMER;
		log.debug("timer " + name + " is firing");
		coordinator.queueEvent(event);
		isFinished=true;
	}
	public boolean isFinished() {
		return this.isFinished;
	}
}
