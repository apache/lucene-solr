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
import java.util.*;
import java.text.*;

/**
 * this class is only used for SPEED. Its log function is not thread safe by
 * default.
 * It uses a BufferdWriter.
 * It registers with a logger manager, which can be used to flush several loggers
 * at once
 * @todo: including the date slows down a lot
 *
 */
public class SimpleLogger
{
    private SimpleDateFormat formatter = new SimpleDateFormat ("HH:mm:ss:SSSS");

    Writer logFile;

    StringBuffer buffer = new StringBuffer(1000);

    long startTime = System.currentTimeMillis();
    boolean includeDate;

    public void setStartTime(long startTime)
    {
        this.startTime = startTime;
    }

    public synchronized void logThreadSafe(String text)
    {
        log(text);
    }

    public synchronized void logThreadSafe(Throwable t)
    {
        log(t);
    }

    public void log(String text)
    {
        try
        {
            buffer.setLength(0);
            if(includeDate)
            {
                buffer.append(formatter.format(new Date())).append(": ").append(System.currentTimeMillis()-startTime).append(" ms: ");
            }
            buffer.append(text).append("\n");
            logFile.write(buffer.toString());
            if(flushAtOnce)
            {
                logFile.flush();
            }
        }
        catch(IOException e)
        {
            System.out.println("Couldn't write to logfile");
        }
    }

    public void log(Throwable t)
    {
        t.printStackTrace(new PrintWriter(logFile));
    }

    boolean flushAtOnce = false;

    public void setFlushAtOnce(boolean flush)
    {
        this.flushAtOnce = flush;
    }

    public SimpleLogger(String name)
    {
        init(name, true);
    }

    public SimpleLogger(String name, boolean includeDate)
    {
        init(name, includeDate);
    }

    public void flush() throws IOException
    {
        logFile.flush();
    }

    private void init(String name, boolean includeDate)
    {
        try
        {
           logFile = new BufferedWriter(new FileWriter("logs/" + name + ".log"));
           SimpleLoggerManager.getInstance().register(this);
        }
        catch(IOException e)
        {
           System.out.println("IOException while creating logfile " + name + ":");
           e.printStackTrace();
        }
    }
}
