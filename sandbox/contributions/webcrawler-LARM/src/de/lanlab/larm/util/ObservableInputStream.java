
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c) <p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.util;

import java.io.*;

public class ObservableInputStream extends FilterInputStream
{
    private boolean reporting = true;
    private long startTime;
    private int totalRead = 0;
    private int step = 1;
    private int nextStep = 0;

    InputStreamObserver observer;

    public ObservableInputStream(InputStream in, InputStreamObserver iso, int reportingStep)
    {
        super(in);
        startTime = System.currentTimeMillis();
        observer = iso;
        observer.notifyOpened(this, System.currentTimeMillis() - startTime);
        nextStep = step = reportingStep;
    }

    public void close() throws IOException
    {
        super.close();
        observer.notifyClosed(this, System.currentTimeMillis() - startTime);
    }

    public void setReporting(boolean reporting)
    {
        this.reporting = reporting;
    }

    public boolean isReporting()
    {
        return reporting;
    }

    public void setReportingStep(int step)
    {
        this.step = step;
    }

    public int read() throws IOException
    {
        int readByte = super.read();
        if(reporting)
        {
            notifyObserver(readByte>=0? 1 : 0);
        }
        return readByte;
    }

    public int read(byte[] b) throws IOException
    {
        int nrRead = super.read(b);
        if(reporting)
        {
            notifyObserver(nrRead);
        }
        return nrRead;
    }

    private void notifyObserver(int nrRead)
    {
        if(nrRead > 0)
        {
            totalRead += nrRead;
            if(totalRead > nextStep)
            {
                nextStep += step;
                observer.notifyRead(this, System.currentTimeMillis() - startTime, nrRead, totalRead);
            }
        }
        else
        {
            observer.notifyFinished(this, System.currentTimeMillis() - startTime, totalRead);
        }
    }

    public int read(byte[] b, int offs, int size) throws IOException
    {
        int nrRead = super.read(b, offs, size);
        if(reporting)
        {
            notifyObserver(nrRead);
        }
        return nrRead;
    }
}

