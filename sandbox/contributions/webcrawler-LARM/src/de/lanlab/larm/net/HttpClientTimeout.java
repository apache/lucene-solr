package de.lanlab.larm.net;

// whatever package you want
import sun.net.www.http.HttpClient;
import sun.net.www.MessageHeader;
import sun.net.ProgressEntry;

import java.net.*;
import java.io.*;


/**
 *  Description of the Class
 *
 *@author     cmarschn
 *@created    2. Mai 2001
 */
public class HttpClientTimeout extends HttpClient {
	private int timeout = -1;


	/**
	 *  Constructor for the HttpClientTimeout object
	 *
	 *@param  url              Description of Parameter
	 *@param  proxy            Description of Parameter
	 *@param  proxyPort        Description of Parameter
	 *@exception  IOException  Description of Exception
	 */
	public HttpClientTimeout(URL url, String proxy, int proxyPort) throws IOException {
		super(url, proxy, proxyPort);
	}


	/**
	 *  Constructor for the HttpClientTimeout object
	 *
	 *@param  url              Description of Parameter
	 *@exception  IOException  Description of Exception
	 */
	public HttpClientTimeout(URL url) throws IOException {
		super(url, null, -1);
	}


	/**
	 *  Sets the Timeout attribute of the HttpClientTimeout object
	 *
	 *@param  i                    The new Timeout value
	 *@exception  SocketException  Description of Exception
	 */
	public void setTimeout(int i) throws SocketException {
		this.timeout = -1;
		serverSocket.setSoTimeout(i);
	}


	/**
	 *  Gets the Socket attribute of the HttpClientTimeout object
	 *
	 *@return    The Socket value
	 */
	public Socket getSocket() {
		return serverSocket;
	}


	/**
	 *  Description of the Method
	 *
	 *@param  header                   Description of Parameter
	 *@param  entry                    Description of Parameter
	 *@return                          Description of the Returned Value
	 *@exception  java.io.IOException  Description of Exception
	 */
	public boolean parseHTTP(MessageHeader header, ProgressEntry entry) throws java.io.IOException {
		if (this.timeout != -1) {
			try {
				serverSocket.setSoTimeout(this.timeout);
			}
			catch (SocketException e) {
				throw new java.io.IOException("unable to set socket timeout!");
			}
		}
		return super.parseHTTP(header, entry);
	}


	/**
	 *  Description of the Method
	 *
	 *@exception  IOException  Description of Exception
	 */
	public void close() throws IOException {
		serverSocket.close();
	}


	/*
	 * public void SetTimeout(int i) throws SocketException {
	 * serverSocket.setSoTimeout(i);
	 * }
	 */
	/*
	 * This class has no public constructor for HTTP.  This method is used to
	 * get an HttpClient to the specifed URL.  If there's currently an
	 * active HttpClient to that server/port, you'll get that one.
	 *
	 * no longer syncrhonized -- it slows things down too much
	 * synchronize at a higher level
	 */
	/**
	 *  Gets the New attribute of the HttpClientTimeout class
	 *
	 *@param  url              Description of Parameter
	 *@return                  The New value
	 *@exception  IOException  Description of Exception
	 */
	public static HttpClientTimeout getNew(URL url) throws IOException {
		/*
		 * see if one's already around
		 */
		HttpClientTimeout ret = (HttpClientTimeout) kac.get(url);
		if (ret == null) {
			ret = new HttpClientTimeout(url);
			// CTOR called openServer()
		}
		else {
			ret.url = url;
		}
		// don't know if we're keeping alive until we parse the headers
		// for now, keepingAlive is false
		return ret;
	}
}

