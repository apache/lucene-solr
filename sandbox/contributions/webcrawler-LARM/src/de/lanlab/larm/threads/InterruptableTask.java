
package de.lanlab.larm.threads;

public interface InterruptableTask
{
	public void run(ServerThread thread);
	public void interrupt();
	public String getInfo();
}
