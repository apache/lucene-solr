
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c) <p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.util;

public interface InputStreamObserver
{
    public void notifyOpened(ObservableInputStream in, long timeElapsed);
    public void notifyClosed(ObservableInputStream in, long timeElapsed);
    public void notifyRead(ObservableInputStream in, long timeElapsed, int nrRead, int totalRead);
    public void notifyFinished(ObservableInputStream in, long timeElapsed, int totalRead);
}