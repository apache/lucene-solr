
/**
 * Title:        LARM Lanlab Retrieval Machine<p>
 * Description:  <p>
 * Copyright:    Copyright (c) <p>
 * Company:      <p>
 * @author
 * @version 1.0
 */
package de.lanlab.larm.fetcher;

/**
 * contains all global constants used in this package
 */
public class Constants
{

    /**
     * user agent string a fetcher task gives to the corresponding server
     */
    public static final String USER_AGENT = "Mozilla/4.06 [en] (WinNT; I)";

    /**
     * Crawler Identification
     */
    public static final String CRAWLER_AGENT = "Fetcher/0.95";

    /**
     * size of the temporary buffer to read web documents in
     */
    public final static int FETCHERTASK_READSIZE = 4096;

    /**
     * don't read more than... bytes
     */
    public final static int FETCHERTASK_MAXFILESIZE = 2000000;

}