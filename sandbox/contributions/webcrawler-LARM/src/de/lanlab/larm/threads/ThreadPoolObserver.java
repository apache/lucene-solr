package de.lanlab.larm.threads;

import de.lanlab.larm.util.Observer;

/**
 * an observer that observes the thread pool...
 */
public interface ThreadPoolObserver extends Observer
{
	public void queueUpdate(String info, String action);
 	public void threadUpdate(int threadNr, String action, String info);
}
