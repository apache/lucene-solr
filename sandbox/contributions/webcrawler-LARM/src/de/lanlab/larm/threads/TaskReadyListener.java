package de.lanlab.larm.threads;

import de.lanlab.larm.util.Observer;

public interface TaskReadyListener extends Observer
{
	public void taskReady(ServerThread s);
}

