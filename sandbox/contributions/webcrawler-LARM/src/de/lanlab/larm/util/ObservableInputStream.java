/* ====================================================================
 * The Apache Software License, Version 1.1
 *
 * Copyright (c) 2001 The Apache Software Foundation.  All rights
 * reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 * 1. Redistributions of source code must retain the above copyright
 *    notice, this list of conditions and the following disclaimer.
 *
 * 2. Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in
 *    the documentation and/or other materials provided with the
 *    distribution.
 *
 * 3. The end-user documentation included with the redistribution,
 *    if any, must include the following acknowledgment:
 *       "This product includes software developed by the
 *        Apache Software Foundation (http://www.apache.org/)."
 *    Alternately, this acknowledgment may appear in the software itself,
 *    if and wherever such third-party acknowledgments normally appear.
 *
 * 4. The names "Apache" and "Apache Software Foundation" and
 *    "Apache Lucene" must not be used to endorse or promote products
 *    derived from this software without prior written permission. For
 *    written permission, please contact apache@apache.org.
 *
 * 5. Products derived from this software may not be called "Apache",
 *    "Apache Lucene", nor may "Apache" appear in their name, without
 *    prior written permission of the Apache Software Foundation.
 *
 * THIS SOFTWARE IS PROVIDED ``AS IS'' AND ANY EXPRESSED OR IMPLIED
 * WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES
 * OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
 * DISCLAIMED.  IN NO EVENT SHALL THE APACHE SOFTWARE FOUNDATION OR
 * ITS CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF
 * USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
 * ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
 * OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT
 * OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF
 * SUCH DAMAGE.
 * ====================================================================
 *
 * This software consists of voluntary contributions made by many
 * individuals on behalf of the Apache Software Foundation.  For more
 * information on the Apache Software Foundation, please see
 * <http://www.apache.org/>.
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

