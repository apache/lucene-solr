package de.lanlab.larm.net;

import java.net.*;

/**
 *  Description of the Class
 *
 *@author     cmarschn
 *@created    2. Mai 2001
 */
public class HttpTimeoutFactory implements URLStreamHandlerFactory {
	int fiTimeoutVal;


	/**
	 *  Constructor for the HttpTimeoutFactory object
	 *
	 *@param  iT  Description of Parameter
	 */
	public HttpTimeoutFactory(int iT) {
		fiTimeoutVal = iT;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  str  Description of Parameter
	 *@return      Description of the Returned Value
	 */
	public URLStreamHandler createURLStreamHandler(String str) {
		return new HttpTimeoutHandler(fiTimeoutVal);
	}

    static HttpTimeoutFactory instance = null;

    /**
     * gets an instance. only the first call will create it. In subsequent calls the iT
     * parameter doesn't have a meaning.
     */
    public static HttpTimeoutFactory getInstance(int iT)
    {
        if(instance == null)
        {
            instance = new HttpTimeoutFactory(iT);
        }
        return instance;
    }
}

