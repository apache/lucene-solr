package de.lanlab.larm.net;

import java.net.*;
import java.io.IOException;

/**
 *  Description of the Class
 *
 *@author     cmarschn
 *@created    2. Mai 2001
 */
public class HttpTimeoutHandler extends sun.net.www.protocol.http.Handler {
	int timeoutVal;
	HttpURLConnectionTimeout fHUCT;


	/**
	 *  Constructor for the HttpTimeoutHandler object
	 *
	 *@param  iT  Description of Parameter
	 */
	public HttpTimeoutHandler(int iT) {
		timeoutVal = iT;
	}


	/**
	 *  Gets the Socket attribute of the HttpTimeoutHandler object
	 *
	 *@return    The Socket value
	 */
	public Socket getSocket() {
		return fHUCT.getSocket();
	}


	/**
	 *  Description of the Method
	 *
	 *@exception  Exception  Description of Exception
	 */
	public void close() throws Exception {
		fHUCT.close();
	}


	/**
	 *  Description of the Method
	 *
	 *@param  u                Description of Parameter
	 *@return                  Description of the Returned Value
	 *@exception  IOException  Description of Exception
	 */
	protected java.net.URLConnection openConnection(URL u) throws IOException {
		return fHUCT = new HttpURLConnectionTimeout(u, this, timeoutVal);
	}


	/**
	 *  Gets the Proxy attribute of the HttpTimeoutHandler object
	 *
	 *@return    The Proxy value
	 */
	String getProxy() {
		return proxy;
		// breaking encapsulation
	}


	/**
	 *  Gets the ProxyPort attribute of the HttpTimeoutHandler object
	 *
	 *@return    The ProxyPort value
	 */
	int getProxyPort() {
		return proxyPort;
		// breaking encapsulation
	}
}

