package de.lanlab.larm.util;

/**
 * Title:        LARM Lanlab Retrieval Machine
 * Description:
 * Copyright:    Copyright (c)
 * Company:
 * @author
 * @version 1.0
 */

import java.util.*;
import java.io.IOException;

/**
 * this singleton manages all loggers. It can be used to flush all SimpleLoggers
 * at once
 */
public class SimpleLoggerManager
{
    static SimpleLoggerManager instance = null;

    ArrayList logs;

    private SimpleLoggerManager()
    {
        logs = new ArrayList();
    }

    public void register(SimpleLogger logger)
    {
        logs.add(logger);
    }

    public void flush() throws IOException
    {
        Iterator it = logs.iterator();
        IOException ex = null;
        while(it.hasNext())
        {
            try
            {
                SimpleLogger logger = (SimpleLogger)it.next();
                logger.flush();
            }
            catch(IOException e)
            {
               ex = e;
            }
        }
        if(ex != null)
        {
            throw ex;
        }
    }

    public static SimpleLoggerManager getInstance()
    {
        if(instance == null)
        {
            instance = new SimpleLoggerManager();
        }
        return instance;
    }
}