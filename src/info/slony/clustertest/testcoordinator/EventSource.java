package info.slony.clustertest.testcoordinator;

public interface EventSource {
	/**
	 * Has this event source finished running.
	 * An event source that has finished running won't generate any new events in
	 * the future.
	 * @return true if the event source is finished.
	 */
	public boolean isFinished();
}
