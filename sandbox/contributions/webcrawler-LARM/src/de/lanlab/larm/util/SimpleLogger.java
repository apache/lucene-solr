package de.lanlab.larm.util;

/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */
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
