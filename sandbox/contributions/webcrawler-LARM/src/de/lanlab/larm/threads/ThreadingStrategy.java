package de.lanlab.larm.threads;

public interface ThreadingStrategy
{
	public void doTask(InterruptableTask t, Object key);
	public void interrupt();
	public void stop();
}
