/*
 *  @(#)HTTPConnection.java				0.3-3 06/05/2001
 *
 *  This file is part of the HTTPClient package
 *  Copyright (C) 1996-2001 Ronald Tschalär
 *
 *  This library is free software; you can redistribute it and/or
 *  modify it under the terms of the GNU Lesser General Public
 *  License as published by the Free Software Foundation; either
 *  version 2 of the License, or (at your option) any later version.
 *
 *  This library is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 *  Lesser General Public License for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public
 *  License along with this library; if not, write to the Free
 *  Software Foundation, Inc., 59 Temple Place, Suite 330, Boston,
 *  MA 02111-1307, USA
 *
 *  For questions, suggestions, bug-reports, enhancement-requests etc.
 *  I may be contacted at:
 *
 *  ronald@innovation.ch
 *
 *  The HTTPClient's home page is located at:
 *
 *  http://www.innovation.ch/java/HTTPClient/
 *
 */
package HTTPClient;

import java.io.OutputStream;
import java.io.DataOutputStream;
import java.io.FilterOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URL;
import java.net.Socket;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.ConnectException;
import java.net.UnknownHostException;
import java.net.NoRouteToHostException;
import java.util.Vector;
import java.applet.Applet;

/**
 * This class implements http protocol requests; it contains most of HTTP/1.1
 * and ought to be unconditionally compliant. Redirections are automatically
 * handled, and authorizations requests are recognized and dealt with via an
 * authorization handler. Only full HTTP/1.0 and HTTP/1.1 requests are
 * generated. HTTP/1.1, HTTP/1.0 and HTTP/0.9 responses are recognized. <P>
 *
 * Using the HTTPClient should be quite simple. First add the import statement '
 * <code>import HTTPClient.*;</code>' to your file(s). Request can then be sent
 * using one of the methods <var>Head()</var> , <var>Get()</var> , <var>Post()
 * </var>, etc in <var>HTTPConnection</var> . These methods all return an
 * instance of <var>HTTPResponse</var> which has methods for accessing the
 * response headers (<var>getHeader()</var> , <var>getHeaderAsInt()</var> ,
 * etc), various response info (<var>getStatusCode()</var> , <var>
 * getReasonLine()</var> , etc) and the reponse data (<var>getData()</var> ,
 * <var>getText()</var> , and <var>getInputStream()</var> ). Following are some
 * examples. <P>
 *
 * If this is in an applet you can retrieve files from your server as follows:
 * <PRE>
 *     try
 *     {
 *         HTTPConnection con = new HTTPConnection(this);
 *         HTTPResponse   rsp = con.Get("/my_file");
 *         if (rsp.getStatusCode() >= 300)
 *         {
 *             System.err.println("Received Error: "+rsp.getReasonLine());
 *             System.err.println(rsp.getText());
 *         }
 *         else
 *             data = rsp.getData();
 *
 *         rsp = con.Get("/another_file");
 *         if (rsp.getStatusCode() >= 300)
 *         {
 *             System.err.println("Received Error: "+rsp.getReasonLine());
 *             System.err.println(rsp.getText());
 *         }
 *         else
 *             other_data = rsp.getData();
 *     }
 *     catch (IOException ioe)
 *     {
 *         System.err.println(ioe.toString());
 *     }
 *     catch (ModuleException me)
 *     {
 *         System.err.println("Error handling request: " + me.getMessage());
 *     }
 * </PRE> This will get the files "/my_file" and "/another_file" and put their
 * contents into byte[]'s accessible via <code>getData()</code>. Note that you
 * need to only create a new <var>HTTPConnection</var> when sending a request to
 * a new server (different host or port); although you may create a new <var>
 * HTTPConnection</var> for every request to the same server this <strong>not
 * </strong> recommended, as various information about the server is cached
 * after the first request (to optimize subsequent requests) and persistent
 * connections are used whenever possible. <P>
 *
 * To POST form data you would use something like this (assuming you have two
 * fields called <var>name</var> and <var>e-mail</var> , whose contents are
 * stored in the variables <var>name</var> and <var>email</var> ): <PRE>
 *     try
 *     {
 *         NVPair form_data[] = new NVPair[2];
 *         form_data[0] = new NVPair("name", name);
 *         form_data[1] = new NVPair("e-mail", email);
 *
 *         HTTPConnection con = new HTTPConnection(this);
 *         HTTPResponse   rsp = con.Post("/cgi-bin/my_script", form_data);
 *         if (rsp.getStatusCode() >= 300)
 *         {
 *             System.err.println("Received Error: "+rsp.getReasonLine());
 *             System.err.println(rsp.getText());
 *         }
 *         else
 *             stream = rsp.getInputStream();
 *     }
 *     catch (IOException ioe)
 *     {
 *         System.err.println(ioe.toString());
 *     }
 *     catch (ModuleException me)
 *     {
 *         System.err.println("Error handling request: " + me.getMessage());
 *     }
 * </PRE> Here the response data is read at leasure via an <var>InputStream
 * </var> instead of all at once into a <var>byte[]</var> . <P>
 *
 * As another example, if you have a URL you're trying to send a request to you
 * would do something like the following: <PRE>
 *     try
 *     {
 *         URL url = new URL("http://www.mydomain.us/test/my_file");
 *         HTTPConnection con = new HTTPConnection(url);
 *         HTTPResponse   rsp = con.Put(url.getFile(), "Hello World");
 *         if (rsp.getStatusCode() >= 300)
 *         {
 *             System.err.println("Received Error: "+rsp.getReasonLine());
 *             System.err.println(rsp.getText());
 *         }
 *         else
 *             text = rsp.getText();
 *     }
 *     catch (IOException ioe)
 *     {
 *         System.err.println(ioe.toString());
 *     }
 *     catch (ModuleException me)
 *     {
 *         System.err.println("Error handling request: " + me.getMessage());
 *     }
 * </PRE> <P>
 *
 * There are a whole number of methods for each request type; however the
 * general forms are ([...] means that the enclosed is optional):
 * <ul>
 *   <li> Head ( file [, form-data [, headers ] ] )
 *   <li> Head ( file [, query [, headers ] ] )
 *   <li> Get ( file [, form-data [, headers ] ] )
 *   <li> Get ( file [, query [, headers ] ] )
 *   <li> Post ( file [, form-data [, headers ] ] )
 *   <li> Post ( file [, data [, headers ] ] )
 *   <li> Post ( file [, stream [, headers ] ] )
 *   <li> Put ( file , data [, headers ] )
 *   <li> Put ( file , stream [, headers ] )
 *   <li> Delete ( file [, headers ] )
 *   <li> Options ( file [, headers [, data] ] )
 *   <li> Options ( file [, headers [, stream] ] )
 *   <li> Trace ( file [, headers ] )
 * </ul>
 *
 *
 * @author    Ronald Tschalär
 * @created   29. Dezember 2001
 * @version   0.3-3 06/05/2001
 */
public class HTTPConnection implements GlobalConstants, HTTPClientModuleConstants
{
    /**
     * The current version of this package.
     */
    public final static String version = "RPT-HTTPClient/0.3-3";

    /**
     * The default context
     */
    private final static Object dflt_context = new Object();

    /**
     * The current context
     */
    private Object Context = null;

    /**
     * The protocol used on this connection
     */
    private int Protocol;

    /**
     * The server's protocol version; M.m stored as (M<<16 | m)
     */
    int ServerProtocolVersion;

    /**
     * Have we gotten the server's protocol version yet?
     */
    boolean ServProtVersKnown;

    /**
     * The protocol version we send in a request; this is always HTTP/1.1 unless
     * we're talking to a broken server in which case it's HTTP/1.0
     */
    private String RequestProtocolVersion;

    /**
     * The remote host this connection is associated with
     */
    private String Host;

    /**
     * The remote port this connection is attached to
     */
    private int Port;

    /**
     * The local address this connection is associated with
     */
    private InetAddress LocalAddr;

    /**
     * The local port this connection is attached to
     */
    private int LocalPort;

    /**
     * The current proxy host to use (if any)
     */
    private String Proxy_Host = null;

    /**
     * The current proxy port
     */
    private int Proxy_Port;

    /**
     * The default proxy host to use (if any)
     */
    private static String Default_Proxy_Host = null;

    /**
     * The default proxy port
     */
    private static int Default_Proxy_Port;

    /**
     * The list of hosts for which no proxy is to be used
     */
    private static CIHashtable non_proxy_host_list = new CIHashtable();
    private static Vector non_proxy_dom_list = new Vector();
    private static Vector non_proxy_addr_list = new Vector();
    private static Vector non_proxy_mask_list = new Vector();

    /**
     * The socks server to use
     */
    private SocksClient Socks_client = null;

    /**
     * The default socks server to use
     */
    private static SocksClient Default_Socks_client = null;

    /**
     * the current stream demultiplexor
     */
    private StreamDemultiplexor input_demux = null;

    /**
     * a list of active stream demultiplexors
     */
    LinkedList DemuxList = new LinkedList();

    /**
     * a list of active requests
     */
    private LinkedList RequestList = new LinkedList();

    /**
     * does the server support keep-alive's?
     */
    private boolean doesKeepAlive = false;

    /**
     * have we been able to determine the above yet?
     */
    private boolean keepAliveUnknown = true;

    /**
     * the maximum number of requests over a HTTP/1.0 keep-alive connection
     */
    private int keepAliveReqMax = -1;

    /**
     * the number of requests over a HTTP/1.0 keep-alive connection left
     */
    private int keepAliveReqLeft;

    /**
     * hack to force buffering of data instead of using chunked T-E
     */
    private static boolean no_chunked = false;

    /**
     * hack to force HTTP/1.0 requests
     */
    private static boolean force_1_0 = false;

    /**
     * hack to be able to disable pipelining
     */
    private static boolean neverPipeline = false;

    /**
     * hack to be able to disable keep-alives
     */
    private static boolean noKeepAlives = false;

    /**
     * hack to work around M$ bug
     */
    private static boolean haveMSLargeWritesBug = false;

    /**
     * hack to only enable defered handling of streamed requests when configured
     * to do so.
     */
    static boolean deferStreamed = false;

    /**
     * the default timeout to use for new connections
     */
    private static int DefaultTimeout = 0;

    /**
     * the timeout to use for reading responses
     */
    private int Timeout;

    /**
     * The list of default http headers
     */
    private NVPair[] DefaultHeaders = new NVPair[0];

    /**
     * The default list of modules (as a Vector of Class objects)
     */
    private static Vector DefaultModuleList;

    /**
     * The list of modules (as a Vector of Class objects)
     */
    private Vector ModuleList;

    /**
     * controls whether modules are allowed to interact with user
     */
    private static boolean defaultAllowUI = true;

    /**
     * controls whether modules are allowed to interact with user
     */
    private boolean allowUI;

    static
    {
        /*
         *  Let's try and see if we can figure out whether any proxies are
         *  being used.
         */
        try
        {
            // JDK 1.1 naming

            String host = System.getProperty("http.proxyHost");
            if (host == null)
            {
                throw new Exception();
            }
            // try JDK 1.0.x naming
            int port = Integer.getInteger("http.proxyPort", -1).intValue();

            Log.write(Log.CONN, "Conn:  using proxy " + host + ":" + port);
            setProxyServer(host, port);
        }
        catch (Exception e)
        {
            try
            {
                // JDK 1.0.x naming

                if (Boolean.getBoolean("proxySet"))
                {
                    String host = System.getProperty("proxyHost");
                    int port = Integer.getInteger("proxyPort", -1).intValue();
                    Log.write(Log.CONN, "Conn:  using proxy " + host + ":" + port);
                    setProxyServer(host, port);
                }
            }
            catch (Exception ee)
            {
                Default_Proxy_Host = null;
            }
        }

        /*
         *  now check for the non-proxy list
         */
        try
        {
            String hosts = System.getProperty("HTTPClient.nonProxyHosts");
            if (hosts == null)
            {
                hosts = System.getProperty("http.nonProxyHosts");
            }

            String[] list = Util.splitProperty(hosts);
            dontProxyFor(list);
        }
        catch (Exception e)
        {
        }

        /*
         *  we can't turn the JDK SOCKS handling off, so we don't use the
         *  properties 'socksProxyHost' and 'socksProxyPort'. Instead we
         *  define 'HTTPClient.socksHost', 'HTTPClient.socksPort' and
         *  'HTTPClient.socksVersion'.
         */
        try
        {
            String host = System.getProperty("HTTPClient.socksHost");
            if (host != null && host.length() > 0)
            {
                int port = Integer.getInteger("HTTPClient.socksPort", -1).intValue();
                int version = Integer.getInteger("HTTPClient.socksVersion", -1).intValue();
                Log.write(Log.CONN, "Conn:  using SOCKS " + host + ":" + port);
                if (version == -1)
                {
                    setSocksServer(host, port);
                }
                else
                {
                    setSocksServer(host, port, version);
                }
            }
        }
        catch (Exception e)
        {
            Default_Socks_client = null;
        }

        // Set up module list

        String modules = "HTTPClient.RetryModule|" +
                "HTTPClient.CookieModule|" +
                "HTTPClient.RedirectionModule|" +
                "HTTPClient.AuthorizationModule|" +
                "HTTPClient.DefaultModule|" +
                "HTTPClient.TransferEncodingModule|" +
                "HTTPClient.ContentMD5Module|" +
                "HTTPClient.ContentEncodingModule";

        boolean in_applet = false;
        try
        {
            modules = System.getProperty("HTTPClient.Modules", modules);
        }
        catch (SecurityException se)
        {
            in_applet = true;
        }

        DefaultModuleList = new Vector();
        String[] list = Util.splitProperty(modules);
        for (int idx = 0; idx < list.length; idx++)
        {
            try
            {
                DefaultModuleList.addElement(Class.forName(list[idx]));
                Log.write(Log.CONN, "Conn:  added module " + list[idx]);
            }
            catch (ClassNotFoundException cnfe)
            {
                if (!in_applet)
                {
                    throw new NoClassDefFoundError(cnfe.getMessage());
                }

                /*
                 *  Just ignore it. This allows for example applets to just
                 *  load the necessary modules - if you don't need a module
                 *  then don't provide it, and it won't be added to the
                 *  list. The disadvantage is that if you accidently misstype
                 *  a module name this will lead to a "silent" error.
                 */
            }
        }

        /*
         *  Hack: disable pipelining
         */
        try
        {
            neverPipeline = Boolean.getBoolean("HTTPClient.disable_pipelining");
            if (neverPipeline)
            {
                Log.write(Log.CONN, "Conn:  disabling pipelining");
            }
        }
        catch (Exception e)
        {
        }

        /*
         *  Hack: disable keep-alives
         */
        try
        {
            noKeepAlives = Boolean.getBoolean("HTTPClient.disableKeepAlives");
            if (noKeepAlives)
            {
                Log.write(Log.CONN, "Conn:  disabling keep-alives");
            }
        }
        catch (Exception e)
        {
        }

        /*
         *  Hack: force HTTP/1.0 requests
         */
        try
        {
            force_1_0 = Boolean.getBoolean("HTTPClient.forceHTTP_1.0");
            if (force_1_0)
            {
                Log.write(Log.CONN, "Conn:  forcing HTTP/1.0 requests");
            }
        }
        catch (Exception e)
        {
        }

        /*
         *  Hack: prevent chunking of request data
         */
        try
        {
            no_chunked = Boolean.getBoolean("HTTPClient.dontChunkRequests");
            if (no_chunked)
            {
                Log.write(Log.CONN, "Conn:  never chunking requests");
            }
        }
        catch (Exception e)
        {
        }

        /*
         *  M$ bug: large writes hang the stuff
         */
        try
        {
            if (System.getProperty("os.name").indexOf("Windows") >= 0 &&
                    System.getProperty("java.version").startsWith("1.1"))
            {
                haveMSLargeWritesBug = true;
            }
            if (haveMSLargeWritesBug)
            {
                Log.write(Log.CONN, "Conn:  splitting large writes into 20K chunks (M$ bug)");
            }
        }
        catch (Exception e)
        {
        }

        /*
         *  Deferring the handling of responses to requests which used an output
         *  stream is new in V0.3-3. Because it can cause memory leaks for apps
         *  which aren't expecting this, we only enable this feature if
         *  explicitly requested to do so.
         */
        try
        {
            deferStreamed = Boolean.getBoolean("HTTPClient.deferStreamed");
            if (deferStreamed)
            {
                Log.write(Log.CONN, "Conn:  enabling defered handling of " +
                        "responses to streamed requests");
            }
        }
        catch (Exception e)
        {
        }
    }


    // Constructors

    /**
     * Constructs a connection to the host from where the applet was loaded.
     * Note that current security policies only let applets connect home.
     *
     * @param applet                        the current applet
     * @exception ProtocolNotSuppException  Description of the Exception
     */
    public HTTPConnection(Applet applet)
        throws ProtocolNotSuppException
    {
        this(applet.getCodeBase().getProtocol(),
                applet.getCodeBase().getHost(),
                applet.getCodeBase().getPort());
    }


    /**
     * Constructs a connection to the specified host on port 80
     *
     * @param host  the host
     */
    public HTTPConnection(String host)
    {
        Setup(HTTP, host, 80, null, -1);
    }


    /**
     * Constructs a connection to the specified host on the specified port
     *
     * @param host  the host
     * @param port  the port
     */
    public HTTPConnection(String host, int port)
    {
        Setup(HTTP, host, port, null, -1);
    }


    /**
     * Constructs a connection to the specified host on the specified port,
     * using the specified protocol (currently only "http" is supported).
     *
     * @param prot                          the protocol
     * @param host                          the host
     * @param port                          the port, or -1 for the default port
     * @exception ProtocolNotSuppException  if the protocol is not HTTP
     */
    public HTTPConnection(String prot, String host, int port)
        throws ProtocolNotSuppException
    {
        this(prot, host, port, null, -1);
    }


    /**
     * Constructs a connection to the specified host on the specified port,
     * using the specified protocol (currently only "http" is supported), local
     * address, and local port.
     *
     * @param prot                          the protocol
     * @param host                          the host
     * @param port                          the port, or -1 for the default port
     * @param localAddr                     the local address to bind to
     * @param localPort                     Description of the Parameter
     * @exception ProtocolNotSuppException  if the protocol is not HTTP
     */
    public HTTPConnection(String prot, String host, int port,
            InetAddress localAddr, int localPort)
        throws ProtocolNotSuppException
    {
        prot = prot.trim().toLowerCase();

        //if (!prot.equals("http")  &&  !prot.equals("https"))
        if (!prot.equals("http"))
        {
            throw new ProtocolNotSuppException("Unsupported protocol '" + prot + "'");
        }

        if (prot.equals("http"))
        {
            Setup(HTTP, host, port, localAddr, localPort);
        }
        else if (prot.equals("https"))
        {
            Setup(HTTPS, host, port, localAddr, localPort);
        }
        else if (prot.equals("shttp"))
        {
            Setup(SHTTP, host, port, localAddr, localPort);
        }
        else if (prot.equals("http-ng"))
        {
            Setup(HTTP_NG, host, port, localAddr, localPort);
        }
    }


    /**
     * Constructs a connection to the host (port) as given in the url.
     *
     * @param url                           the url
     * @exception ProtocolNotSuppException  if the protocol is not HTTP
     */
    public HTTPConnection(URL url)
        throws ProtocolNotSuppException
    {
        this(url.getProtocol(), url.getHost(), url.getPort());
    }


    /**
     * Constructs a connection to the host (port) as given in the uri.
     *
     * @param uri                           the uri
     * @exception ProtocolNotSuppException  if the protocol is not HTTP
     */
    public HTTPConnection(URI uri)
        throws ProtocolNotSuppException
    {
        this(uri.getScheme(), uri.getHost(), uri.getPort());
    }


    /**
     * Sets the class variables. Must not be public.
     *
     * @param prot       the protocol
     * @param host       the host
     * @param port       the port
     * @param localAddr  the local address to bind to; if null, it's ignored
     * @param localPort  the local port to bind to
     */
    private void Setup(int prot, String host, int port, InetAddress localAddr,
            int localPort)
    {
        Protocol = prot;
        Host = host.trim().toLowerCase();
        Port = port;
        LocalAddr = localAddr;
        LocalPort = localPort;

        if (Port == -1)
        {
            Port = URI.defaultPort(getProtocol());
        }

        if (Default_Proxy_Host != null && !matchNonProxy(Host))
        {
            setCurrentProxy(Default_Proxy_Host, Default_Proxy_Port);
        }
        else
        {
            setCurrentProxy(null, 0);
        }

        Socks_client = Default_Socks_client;
        Timeout = DefaultTimeout;
        ModuleList = (Vector) DefaultModuleList.clone();
        allowUI = defaultAllowUI;
        if (noKeepAlives)
        {
            setDefaultHeaders(new NVPair[]{new NVPair("Connection", "close")});
        }
    }


    /**
     * Determines if the given host matches any entry in the non-proxy list.
     *
     * @param host  the host to match - must be trim()'d and lowercase
     * @return      true if a match is found, false otherwise
     * @see         #dontProxyFor(java.lang.String)
     */
    private boolean matchNonProxy(String host)
    {
        // Check host name list

        if (non_proxy_host_list.get(host) != null)
        {
            return true;
        }

        // Check domain name list

        for (int idx = 0; idx < non_proxy_dom_list.size(); idx++)
        {
            if (host.endsWith((String) non_proxy_dom_list.elementAt(idx)))
            {
                return true;
            }
        }

        // Check IP-address and subnet list

        if (non_proxy_addr_list.size() == 0)
        {
            return false;
        }

        InetAddress[] host_addr;
        try
        {
            host_addr = InetAddress.getAllByName(host);
        }
        catch (UnknownHostException uhe)
        {
            return false;
        }
        // maybe the proxy has better luck

        for (int idx = 0; idx < non_proxy_addr_list.size(); idx++)
        {
            byte[] addr = (byte[]) non_proxy_addr_list.elementAt(idx);
            byte[] mask = (byte[]) non_proxy_mask_list.elementAt(idx);

            ip_loop :
            for (int idx2 = 0; idx2 < host_addr.length; idx2++)
            {
                byte[] raw_addr = host_addr[idx2].getAddress();
                if (raw_addr.length != addr.length)
                {
                    continue;
                }

                for (int idx3 = 0; idx3 < raw_addr.length; idx3++)
                {
                    if ((raw_addr[idx3] & mask[idx3]) != (addr[idx3] & mask[idx3]))
                    {
                        continue ip_loop;
                    }
                }
                return true;
            }
        }

        return false;
    }


    // Methods

    /**
     * Sends the HEAD request. This request is just like the corresponding GET
     * except that it only returns the headers and no data.
     *
     * @param file                 the absolute path of the file
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        #Get(java.lang.String)
     */
    public HTTPResponse Head(String file)
        throws IOException, ModuleException
    {
        return Head(file, (String) null, null);
    }


    /**
     * Sends the HEAD request. This request is just like the corresponding GET
     * except that it only returns the headers and no data.
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        #Get(java.lang.String, HTTPClient.NVPair[])
     */
    public HTTPResponse Head(String file, NVPair form_data[])
        throws IOException, ModuleException
    {
        return Head(file, form_data, null);
    }


    /**
     * Sends the HEAD request. This request is just like the corresponding GET
     * except that it only returns the headers and no data.
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        #Get(java.lang.String, HTTPClient.NVPair[],
     *      HTTPClient.NVPair[])
     */
    public HTTPResponse Head(String file, NVPair[] form_data, NVPair[] headers)
        throws IOException, ModuleException
    {
        String File = stripRef(file);
        String
                query = Codecs.nv2query(form_data);
        if (query != null && query.length() > 0)
        {
            File += "?" + query;
        }

        return setupRequest("HEAD", File, headers, null, null);
    }


    /**
     * Sends the HEAD request. This request is just like the corresponding GET
     * except that it only returns the headers and no data.
     *
     * @param file                 the absolute path of the file
     * @param query                the query string; it will be urlencoded
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        #Get(java.lang.String, java.lang.String)
     */
    public HTTPResponse Head(String file, String query)
        throws IOException, ModuleException
    {
        return Head(file, query, null);
    }


    /**
     * Sends the HEAD request. This request is just like the corresponding GET
     * except that it only returns the headers and no data.
     *
     * @param file                 the absolute path of the file
     * @param query                the query string; it will be urlencoded
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        #Get(java.lang.String, java.lang.String,
     *      HTTPClient.NVPair[])
     */
    public HTTPResponse Head(String file, String query, NVPair[] headers)
        throws IOException, ModuleException
    {
        String File = stripRef(file);
        if (query != null && query.length() > 0)
        {
            File += "?" + Codecs.URLEncode(query);
        }

        return setupRequest("HEAD", File, headers, null, null);
    }


    /**
     * GETs the file.
     *
     * @param file                 the absolute path of the file
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Get(String file)
        throws IOException, ModuleException
    {
        return Get(file, (String) null, null);
    }


    /**
     * GETs the file with a query consisting of the specified form-data. The
     * data is urlencoded, turned into a string of the form
     * "name1=value1&name2=value2" and then sent as a query string.
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Get(String file, NVPair form_data[])
        throws IOException, ModuleException
    {
        return Get(file, form_data, null);
    }


    /**
     * GETs the file with a query consisting of the specified form-data. The
     * data is urlencoded, turned into a string of the form
     * "name1=value1&name2=value2" and then sent as a query string.
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Get(String file, NVPair[] form_data, NVPair[] headers)
        throws IOException, ModuleException
    {
        String File = stripRef(file);
        String
                query = Codecs.nv2query(form_data);
        if (query != null && query.length() > 0)
        {
            File += "?" + query;
        }

        return setupRequest("GET", File, headers, null, null);
    }


    /**
     * GETs the file using the specified query string. The query string is first
     * urlencoded.
     *
     * @param file                 the absolute path of the file
     * @param query                the query
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Get(String file, String query)
        throws IOException, ModuleException
    {
        return Get(file, query, null);
    }


    /**
     * GETs the file using the specified query string. The query string is first
     * urlencoded.
     *
     * @param file                 the absolute path of the file
     * @param query                the query string
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Get(String file, String query, NVPair[] headers)
        throws IOException, ModuleException
    {
        String File = stripRef(file);
        if (query != null && query.length() > 0)
        {
            File += "?" + Codecs.URLEncode(query);
        }

        return setupRequest("GET", File, headers, null, null);
    }


    /**
     * POSTs to the specified file. No data is sent.
     *
     * @param file                 the absolute path of the file
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file)
        throws IOException, ModuleException
    {
        return Post(file, (byte[]) null, null);
    }


    /**
     * POSTs form-data to the specified file. The data is first urlencoded and
     * then turned into a string of the form "name1=value1&name2=value2". A
     * <var>Content-type</var> header with the value <var>
     * application/x-www-form-urlencoded</var> is added.
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, NVPair form_data[])
        throws IOException, ModuleException
    {
        NVPair[] headers =
                {new NVPair("Content-type", "application/x-www-form-urlencoded")};

        return Post(file, Codecs.nv2query(form_data), headers);
    }


    /**
     * POST's form-data to the specified file using the specified headers. The
     * data is first urlencoded and then turned into a string of the form
     * "name1=value1&name2=value2". If no <var>Content-type</var> header is
     * given then one is added with a value of <var>
     * application/x-www-form-urlencoded</var> .
     *
     * @param file                 the absolute path of the file
     * @param form_data            an array of Name/Value pairs
     * @param headers              additional headers
     * @return                     a HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, NVPair form_data[], NVPair headers[])
        throws IOException, ModuleException
    {
        int idx;
        for (idx = 0; idx < headers.length; idx++)
        {
            if (headers[idx].getName().equalsIgnoreCase("Content-type"))
            {
                break;
            }
        }
        if (idx == headers.length)
        {
            headers = Util.resizeArray(headers, idx + 1);
            headers[idx] =
                    new NVPair("Content-type", "application/x-www-form-urlencoded");
        }

        return Post(file, Codecs.nv2query(form_data), headers);
    }


    /**
     * POSTs the data to the specified file. The data is converted to an array
     * of bytes using the default character converter. The request is sent using
     * the content-type "application/octet-stream".
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        java.lang.String#getBytes()
     */
    public HTTPResponse Post(String file, String data)
        throws IOException, ModuleException
    {
        return Post(file, data, null);
    }


    /**
     * POSTs the data to the specified file using the specified headers.
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        java.lang.String#getBytes()
     */
    public HTTPResponse Post(String file, String data, NVPair[] headers)
        throws IOException, ModuleException
    {
        byte tmp[] = null;

        if (data != null && data.length() > 0)
        {
            tmp = data.getBytes();
        }

        return Post(file, tmp, headers);
    }


    /**
     * POSTs the raw data to the specified file. The request is sent using the
     * content-type "application/octet-stream"
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, byte data[])
        throws IOException, ModuleException
    {
        return Post(file, data, null);
    }


    /**
     * POSTs the raw data to the specified file using the specified headers.
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, byte data[], NVPair[] headers)
        throws IOException, ModuleException
    {
        if (data == null)
        {
            data = new byte[0];
        }
        // POST must always have a CL
        return setupRequest("POST", stripRef(file), headers, data, null);
    }


    /**
     * POSTs the data written to the output stream to the specified file. The
     * request is sent using the content-type "application/octet-stream"
     *
     * @param file                 the absolute path of the file
     * @param stream               the output stream on which the data is
     *      written
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, HttpOutputStream stream)
        throws IOException, ModuleException
    {
        return Post(file, stream, null);
    }


    /**
     * POSTs the data written to the output stream to the specified file using
     * the specified headers.
     *
     * @param file                 the absolute path of the file
     * @param stream               the output stream on which the data is
     *      written
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Post(String file, HttpOutputStream stream,
            NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest("POST", stripRef(file), headers, null, stream);
    }


    /**
     * PUTs the data into the specified file. The data is converted to an array
     * of bytes using the default character converter. The request ist sent
     * using the content-type "application/octet-stream".
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        java.lang.String#getBytes()
     */
    public HTTPResponse Put(String file, String data)
        throws IOException, ModuleException
    {
        return Put(file, data, null);
    }


    /**
     * PUTs the data into the specified file using the additional headers for
     * the request.
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     * @see                        java.lang.String#getBytes()
     */
    public HTTPResponse Put(String file, String data, NVPair[] headers)
        throws IOException, ModuleException
    {
        byte tmp[] = null;

        if (data != null && data.length() > 0)
        {
            tmp = data.getBytes();
        }

        return Put(file, tmp, headers);
    }


    /**
     * PUTs the raw data into the specified file. The request is sent using the
     * content-type "application/octet-stream".
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Put(String file, byte data[])
        throws IOException, ModuleException
    {
        return Put(file, data, null);
    }


    /**
     * PUTs the raw data into the specified file using the additional headers.
     *
     * @param file                 the absolute path of the file
     * @param data                 the data
     * @param headers              any additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Put(String file, byte data[], NVPair[] headers)
        throws IOException, ModuleException
    {
        if (data == null)
        {
            data = new byte[0];
        }
        // PUT must always have a CL
        return setupRequest("PUT", stripRef(file), headers, data, null);
    }


    /**
     * PUTs the data written to the output stream into the specified file. The
     * request is sent using the content-type "application/octet-stream".
     *
     * @param file                 the absolute path of the file
     * @param stream               the output stream on which the data is
     *      written
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Put(String file, HttpOutputStream stream)
        throws IOException, ModuleException
    {
        return Put(file, stream, null);
    }


    /**
     * PUTs the data written to the output stream into the specified file using
     * the additional headers.
     *
     * @param file                 the absolute path of the file
     * @param stream               the output stream on which the data is
     *      written
     * @param headers              any additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Put(String file, HttpOutputStream stream,
            NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest("PUT", stripRef(file), headers, null, stream);
    }


    /**
     * Request OPTIONS from the server. If <var>file</var> is "*" then the
     * request applies to the server as a whole; otherwise it applies only to
     * that resource.
     *
     * @param file                 the absolute path of the resource, or "*"
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Options(String file)
        throws IOException, ModuleException
    {
        return Options(file, null, (byte[]) null);
    }


    /**
     * Request OPTIONS from the server. If <var>file</var> is "*" then the
     * request applies to the server as a whole; otherwise it applies only to
     * that resource.
     *
     * @param file                 the absolute path of the resource, or "*"
     * @param headers              the headers containing optional info.
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Options(String file, NVPair[] headers)
        throws IOException, ModuleException
    {
        return Options(file, headers, (byte[]) null);
    }


    /**
     * Request OPTIONS from the server. If <var>file</var> is "*" then the
     * request applies to the server as a whole; otherwise it applies only to
     * that resource.
     *
     * @param file                 the absolute path of the resource, or "*"
     * @param headers              the headers containing optional info.
     * @param data                 any data to be sent in the optional body
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Options(String file, NVPair[] headers, byte[] data)
        throws IOException, ModuleException
    {
        return setupRequest("OPTIONS", stripRef(file), headers, data, null);
    }


    /**
     * Request OPTIONS from the server. If <var>file</var> is "*" then the
     * request applies to the server as a whole; otherwise it applies only to
     * that resource.
     *
     * @param file                 the absolute path of the resource, or "*"
     * @param headers              the headers containing optional info.
     * @param stream               an output stream for sending the optional
     *      body
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Options(String file, NVPair[] headers,
            HttpOutputStream stream)
        throws IOException, ModuleException
    {
        return setupRequest("OPTIONS", stripRef(file), headers, null, stream);
    }


    /**
     * Requests that <var>file</var> be DELETEd from the server.
     *
     * @param file                 the absolute path of the resource
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Delete(String file)
        throws IOException, ModuleException
    {
        return Delete(file, null);
    }


    /**
     * Requests that <var>file</var> be DELETEd from the server.
     *
     * @param file                 the absolute path of the resource
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Delete(String file, NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest("DELETE", stripRef(file), headers, null, null);
    }


    /**
     * Requests a TRACE. Headers of particular interest here are "Via" and
     * "Max-Forwards".
     *
     * @param file                 the absolute path of the resource
     * @param headers              additional headers
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Trace(String file, NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest("TRACE", stripRef(file), headers, null, null);
    }


    /**
     * Requests a TRACE.
     *
     * @param file                 the absolute path of the resource
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse Trace(String file)
        throws IOException, ModuleException
    {
        return Trace(file, null);
    }


    /**
     * This is here to allow an arbitrary, non-standard request to be sent. I'm
     * assuming you know what you are doing...
     *
     * @param method               the extension method
     * @param file                 the absolute path of the resource, or null
     * @param data                 optional data, or null
     * @param headers              optional headers, or null
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse ExtensionMethod(String method, String file,
            byte[] data, NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest(method.trim(), stripRef(file), headers, data, null);
    }


    /**
     * This is here to allow an arbitrary, non-standard request to be sent. I'm
     * assuming you know what you are doing...
     *
     * @param method               the extension method
     * @param file                 the absolute path of the resource, or null
     * @param headers              optional headers, or null
     * @param os                   Description of the Parameter
     * @return                     an HTTPResponse structure containing the
     *      response
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    public HTTPResponse ExtensionMethod(String method, String file,
            HttpOutputStream os, NVPair[] headers)
        throws IOException, ModuleException
    {
        return setupRequest(method.trim(), stripRef(file), headers, null, os);
    }


    /**
     * Aborts all the requests currently in progress on this connection and
     * closes all associated sockets. You usually do <em>not</em> need to invoke
     * this - it only meant for when you need to abruptly stop things, such as
     * for example the stop button in a browser. <P>
     *
     * Note: there is a small window where a request method such as <code>Get()</code>
     * may have been invoked but the request has not been built and added to the
     * list. Any request in this window will not be aborted.
     *
     * @since   V0.2-3
     */
    public void stop()
    {
        for (Request req = (Request) RequestList.enumerate(); req != null;
                req = (Request) RequestList.next())
        {
            req.aborted = true;
        }

        for (StreamDemultiplexor demux =
                (StreamDemultiplexor) DemuxList.enumerate();
                demux != null; demux = (StreamDemultiplexor) DemuxList.next())
        {
            demux.abort();
        }
    }


    /**
     * Sets the default http headers to be sent with each request. The actual
     * headers sent are determined as follows: for each header specified in
     * multiple places a value given as part of the request takes priority over
     * any default values set by this method, which in turn takes priority over
     * any built-in default values. A different way of looking at it is that we
     * start off with a list of all headers specified with the request, then add
     * any default headers set by this method which aren't already in our list,
     * and finally add any built-in headers which aren't yet in the list. There
     * is one exception to this rule: the "Content-length" header is always
     * ignored; and when posting form-data any default "Content-type" is ignored
     * in favor of the built-in "application/x-www-form-urlencoded" (however it
     * will be overriden by any content-type header specified as part of the
     * request). <P>
     *
     * Typical headers you might want to set here are "Accept" and its
     * "Accept-*" relatives, "Connection", "From", "User-Agent", etc.
     *
     * @param headers  an array of header-name/value pairs (do not give the
     *      separating ':').
     */
    public void setDefaultHeaders(NVPair[] headers)
    {
        int length = (headers == null ? 0 : headers.length);
        NVPair[] def_hdrs = new NVPair[length];

        // weed out undesired headers
        int sidx;

        // weed out undesired headers
        int didx;
        for (sidx = 0, didx = 0; sidx < length; sidx++)
        {
            if (headers[sidx] == null)
            {
                continue;
            }

            String name = headers[sidx].getName().trim();
            if (name.equalsIgnoreCase("Content-length"))
            {
                continue;
            }

            def_hdrs[didx++] = headers[sidx];
        }

        if (didx < length)
        {
            def_hdrs = Util.resizeArray(def_hdrs, didx);
        }

        synchronized (DefaultHeaders)
        {
            DefaultHeaders = def_hdrs;
        }
    }


    /**
     * Gets the current list of default http headers.
     *
     * @return   an array of header/value pairs.
     */
    public NVPair[] getDefaultHeaders()
    {
        synchronized (DefaultHeaders)
        {
            return (NVPair[]) DefaultHeaders.clone();
        }
    }


    /**
     * Returns the protocol this connection is talking.
     *
     * @return   a string containing the (lowercased) protocol
     */
    public String getProtocol()
    {
        switch (Protocol)
        {
            case HTTP:
                return "http";
            case HTTPS:
                return "https";
            case SHTTP:
                return "shttp";
            case HTTP_NG:
                return "http-ng";
            default:
                throw new Error("HTTPClient Internal Error: invalid protocol " +
                        Protocol);
        }
    }


    /**
     * Returns the host this connection is talking to.
     *
     * @return   a string containing the (lowercased) host name.
     */
    public String getHost()
    {
        return Host;
    }


    /**
     * Returns the port this connection connects to. This is always the actual
     * port number, never -1.
     *
     * @return   the port number
     */
    public int getPort()
    {
        return Port;
    }


    /**
     * Returns the host of the proxy this connection is using.
     *
     * @return   a string containing the (lowercased) host name.
     */
    public String getProxyHost()
    {
        return Proxy_Host;
    }


    /**
     * Returns the port of the proxy this connection is using.
     *
     * @return   the port number
     */
    public int getProxyPort()
    {
        return Proxy_Port;
    }


    /**
     * See if the given uri is compatible with this connection. Compatible means
     * that the given uri can be retrieved using this connection object.
     *
     * @param uri  the URI to check
     * @return     true if they're compatible, false otherwise
     * @since      V0.3-2
     */
    public boolean isCompatibleWith(URI uri)
    {
        if (!uri.getScheme().equals(getProtocol()) ||
                !uri.getHost().equalsIgnoreCase(Host))
        {
            return false;
        }

        int port = uri.getPort();
        if (port == -1)
        {
            port = URI.defaultPort(uri.getScheme());
        }
        return port == Port;
    }


    /**
     * Sets/Resets raw mode. In raw mode all modules are bypassed, meaning the
     * automatic handling of authorization requests, redirections, cookies, etc.
     * is turned off. <P>
     *
     * The default is false.
     *
     * @param raw    if true removes all modules (except for the retry module)
     * @deprecated   This is not really needed anymore; in V0.2 request were
     *      synchronous and therefore to do pipelining you needed to disable the
     *      processing of responses.
     * @see          #removeModule(java.lang.Class)
     */
    public void setRawMode(boolean raw)
    {
        // Don't remove the retry module
        String[] modules = {"HTTPClient.CookieModule",
                "HTTPClient.RedirectionModule",
                "HTTPClient.AuthorizationModule",
                "HTTPClient.DefaultModule",
                "HTTPClient.TransferEncodingModule",
                "HTTPClient.ContentMD5Module",
                "HTTPClient.ContentEncodingModule"};

        for (int idx = 0; idx < modules.length; idx++)
        {
            try
            {
                if (raw)
                {
                    removeModule(Class.forName(modules[idx]));
                }
                else
                {
                    addModule(Class.forName(modules[idx]), -1);
                }
            }
            catch (ClassNotFoundException cnfe)
            {
            }
        }
    }


    /**
     * Sets the default timeout value to be used for each new HTTPConnection.
     * The default is 0.
     *
     * @param time  the timeout in milliseconds.
     * @see         #setTimeout(int)
     */
    public static void setDefaultTimeout(int time)
    {
        DefaultTimeout = time;
    }


    /**
     * Gets the default timeout value to be used for each new HTTPConnection.
     *
     * @return   the timeout in milliseconds.
     * @see      #setTimeout(int)
     */
    public static int getDefaultTimeout()
    {
        return DefaultTimeout;
    }


    /**
     * Sets the timeout to be used for creating connections and reading
     * responses. When a timeout expires the operation will throw an
     * InterruptedIOException. The operation may be restarted again afterwards.
     * If the operation is not restarted and it is a read operation (i.e
     * HTTPResponse.xxxx()) then <code>resp.getInputStream().close()</code>
     * <strong>should</strong> be invoked. <P>
     *
     * When creating new sockets the timeout will limit the time spent doing the
     * host name translation and establishing the connection with the server.
     * <P>
     *
     * The timeout also influences the reading of the response headers. However,
     * it does not specify a how long, for example, getStatusCode() may take, as
     * might be assumed. Instead it specifies how long a read on the socket may
     * take. If the response dribbles in slowly with packets arriving quicker
     * than the timeout then the method will complete normally. I.e. the
     * exception is only thrown if nothing arrives on the socket for the
     * specified time. Furthermore, the timeout only influences the reading of
     * the headers, not the reading of the body. <P>
     *
     * Read Timeouts are associated with responses, so that you may change this
     * value before each request and it won't affect the reading of responses to
     * previous requests.
     *
     * @param time  the time in milliseconds. A time of 0 means wait
     *      indefinitely.
     * @see         #stop()
     */
    public void setTimeout(int time)
    {
        Timeout = time;
    }


    /**
     * Gets the timeout used for reading response data.
     *
     * @return   the current timeout value
     * @see      #setTimeout(int)
     */
    public int getTimeout()
    {
        return Timeout;
    }


    /**
     * Controls whether modules are allowed to prompt the user or pop up dialogs
     * if neccessary.
     *
     * @param allow  if true allows modules to interact with user.
     */
    public void setAllowUserInteraction(boolean allow)
    {
        allowUI = allow;
    }


    /**
     * returns whether modules are allowed to prompt or popup dialogs if
     * neccessary.
     *
     * @return   true if modules are allowed to interact with user.
     */
    public boolean getAllowUserInteraction()
    {
        return allowUI;
    }


    /**
     * Sets the default allow-user-action.
     *
     * @param allow  if true allows modules to interact with user.
     */
    public static void setDefaultAllowUserInteraction(boolean allow)
    {
        defaultAllowUI = allow;
    }


    /**
     * Gets the default allow-user-action.
     *
     * @return   true if modules are allowed to interact with user.
     */
    public static boolean getDefaultAllowUserInteraction()
    {
        return defaultAllowUI;
    }


    /**
     * Returns the default list of modules.
     *
     * @return   an array of classes
     */
    public static Class[] getDefaultModules()
    {
        return getModules(DefaultModuleList);
    }


    /**
     * Adds a module to the default list. It must implement the <var>
     * HTTPClientModule</var> interface. If the module is already in the list
     * then this method does nothing. This method only affects instances of
     * HTTPConnection created after this method has been invoked; it does not
     * affect existing instances. <P>
     *
     * Example: <PRE>
     * HTTPConnection.addDefaultModule(Class.forName("HTTPClient.CookieModule"), 1);
     * </PRE> adds the cookie module as the second module in the list. <P>
     *
     * The default list is created at class initialization time from the
     * property <var>HTTPClient.Modules</var> . This must contain a "|"
     * separated list of classes in the order they're to be invoked. If this
     * property is not set it defaults to: "HTTPClient.RetryModule |
     * HTTPClient.CookieModule | HTTPClient.RedirectionModule |
     * HTTPClient.AuthorizationModule | HTTPClient.DefaultModule |
     * HTTPClient.TransferEncodingModule | HTTPClient.ContentMD5Module |
     * HTTPClient.ContentEncodingModule"
     *
     * @param module  the module's Class object
     * @param pos     the position of this module in the list; if <var>pos</var>
     *      >= 0 then this is the absolute position in the list (0 is the first
     *      position); if <var>pos</var> < 0 then this is the position relative
     *      to the end of the list (-1 means the last element, -2 the second to
     *      last element, etc).
     * @return        true if module was successfully added; false if the module
     *      is already in the list.
     * @see           HTTPClientModule
     */
    public static boolean addDefaultModule(Class module, int pos)
    {
        return addModule(DefaultModuleList, module, pos);
    }


    /**
     * Removes a module from the default list. If the module is not in the list
     * it does nothing. This method only affects instances of HTTPConnection
     * created after this method has been invoked; it does not affect existing
     * instances.
     *
     * @param module  the module's Class object
     * @return        true if module was successfully removed; false otherwise
     */
    public static boolean removeDefaultModule(Class module)
    {
        return removeModule(DefaultModuleList, module);
    }


    /**
     * Returns the list of modules used currently.
     *
     * @return   an array of classes
     */
    public Class[] getModules()
    {
        return getModules(ModuleList);
    }


    /**
     * Adds a module to the current list. It must implement the <var>
     * HTTPClientModule</var> interface. If the module is already in the list
     * then this method does nothing.
     *
     * @param module  the module's Class object
     * @param pos     the position of this module in the list; if <var>pos</var>
     *      >= 0 then this is the absolute position in the list (0 is the first
     *      position); if <var>pos</var> < 0 then this is the position relative
     *      to the end of the list (-1 means the last element, -2 the second to
     *      last element, etc).
     * @return        true if module was successfully added; false if the module
     *      is already in the list.
     * @see           HTTPClientModule
     */
    public boolean addModule(Class module, int pos)
    {
        return addModule(ModuleList, module, pos);
    }


    /**
     * Removes a module from the current list. If the module is not in the list
     * it does nothing.
     *
     * @param module  the module's Class object
     * @return        true if module was successfully removed; false otherwise
     */
    public boolean removeModule(Class module)
    {
        return removeModule(ModuleList, module);
    }


    /**
     * Gets the modules attribute of the HTTPConnection class
     *
     * @param list  Description of the Parameter
     * @return      The modules value
     */
    private final static Class[] getModules(Vector list)
    {
        synchronized (list)
        {
            Class[] modules = new Class[list.size()];
            list.copyInto(modules);
            return modules;
        }
    }


    /**
     * Adds a feature to the Module attribute of the HTTPConnection class
     *
     * @param list    The feature to be added to the Module attribute
     * @param module  The feature to be added to the Module attribute
     * @param pos     The feature to be added to the Module attribute
     * @return        Description of the Return Value
     */
    private final static boolean addModule(Vector list, Class module, int pos)
    {
        if (module == null)
        {
            return false;
        }

        // check if module implements HTTPClientModule
        try
        {
            HTTPClientModule tmp = (HTTPClientModule) module.newInstance();
        }
        catch (RuntimeException re)
        {
            throw re;
        }
        catch (Exception e)
        {
            throw new RuntimeException(e.toString());
        }

        synchronized (list)
        {
            // check if module already in list
            if (list.contains(module))
            {
                return false;
            }

            // add module to list
            if (pos < 0)
            {
                list.insertElementAt(module, DefaultModuleList.size() + pos + 1);
            }
            else
            {
                list.insertElementAt(module, pos);
            }
        }

        Log.write(Log.CONN, "Conn:  Added module " + module.getName() +
                " to " +
                ((list == DefaultModuleList) ? "default " : "") +
                "list");

        return true;
    }


    /**
     * Description of the Method
     *
     * @param list    Description of the Parameter
     * @param module  Description of the Parameter
     * @return        Description of the Return Value
     */
    private final static boolean removeModule(Vector list, Class module)
    {
        if (module == null)
        {
            return false;
        }

        boolean removed = list.removeElement(module);
        if (removed)
        {
            Log.write(Log.CONN, "Conn:  Removed module " + module.getName() +
                    " from " +
                    ((list == DefaultModuleList) ? "default " : "") +
                    "list");
        }

        return removed;
    }


    /**
     * Sets the current context. The context is used by modules such as the
     * AuthorizationModule and the CookieModule which keep lists of info that is
     * normally shared between all instances of HTTPConnection. This is usually
     * the desired behaviour. However, in some cases one would like to simulate
     * multiple independent clients within the same application and hence the
     * sharing of such info should be restricted. This is where the context
     * comes in. Modules will only share their info between requests using the
     * same context (i.e. they keep multiple lists, one for each context). <P>
     *
     * The context may be any object. Contexts are considered equal if <code>equals()</code>
     * returns true. Examples of useful context objects are threads (e.g. if you
     * are running multiple clients, one per thread) and sockets (e.g. if you
     * are implementing a gateway). <P>
     *
     * When a new HTTPConnection is created it is initialized with a default
     * context which is the same for all instances. This method must be invoked
     * immediately after a new HTTPConnection is created and before any request
     * method is invoked. Furthermore, this method may only be called once (i.e.
     * the context is "sticky").
     *
     * @param context  the new context; must be non-null
     */
    public void setContext(Object context)
    {
        if (context == null)
        {
            throw new IllegalArgumentException("Context must be non-null");
        }
        if (Context != null)
        {
            throw new IllegalStateException("Context already set");
        }

        Context = context;
    }


    /**
     * Returns the current context.
     *
     * @return   the current context, or the default context if <code>setContext()</code>
     *      hasn't been invoked
     * @see      #setContext(java.lang.Object)
     */
    public Object getContext()
    {
        if (Context != null)
        {
            return Context;
        }
        else
        {
            return dflt_context;
        }
    }


    /**
     * Returns the default context.
     *
     * @return   the default context
     * @see      #setContext(java.lang.Object)
     */
    public static Object getDefaultContext()
    {
        return dflt_context;
    }


    /**
     * Adds an authorization entry for the "digest" authorization scheme to the
     * list. If an entry already exists for the "digest" scheme and the
     * specified realm then it is overwritten. <P>
     *
     * This is a convenience method and just invokes the corresponding method in
     * AuthorizationInfo.
     *
     * @param realm   the realm
     * @param user    the username
     * @param passwd  The feature to be added to the DigestAuthorization
     *      attribute
     * @see           AuthorizationInfo#addDigestAuthorization(java.lang.String,
     *      int, java.lang.String, java.lang.String, java.lang.String)
     */
    public void addDigestAuthorization(String realm, String user, String passwd)
    {
        AuthorizationInfo.addDigestAuthorization(Host, Port, realm, user,
                passwd, getContext());
    }


    /**
     * Adds an authorization entry for the "basic" authorization scheme to the
     * list. If an entry already exists for the "basic" scheme and the specified
     * realm then it is overwritten. <P>
     *
     * This is a convenience method and just invokes the corresponding method in
     * AuthorizationInfo.
     *
     * @param realm   the realm
     * @param user    the username
     * @param passwd  The feature to be added to the BasicAuthorization
     *      attribute
     * @see           AuthorizationInfo#addBasicAuthorization(java.lang.String,
     *      int, java.lang.String, java.lang.String, java.lang.String)
     */
    public void addBasicAuthorization(String realm, String user, String passwd)
    {
        AuthorizationInfo.addBasicAuthorization(Host, Port, realm, user,
                passwd, getContext());
    }


    /**
     * Sets the default proxy server to use. The proxy will only be used for new
     * <var>HTTPConnection</var> s created after this call and will not affect
     * currrent instances of <var>HTTPConnection</var> . A null or empty string
     * <var>host</var> parameter disables the proxy. <P>
     *
     * In an application or using the Appletviewer an alternative to this method
     * is to set the following properties (either in the properties file or on
     * the command line): <var>http.proxyHost</var> and <var>http.proxyPort
     * </var>. Whether <var>http.proxyHost</var> is set or not determines
     * whether a proxy server is used. <P>
     *
     * If the proxy server requires authorization and you wish to set this
     * authorization information in the code, then you may use any of the <var>
     * AuthorizationInfo.addXXXAuthorization()</var> methods to do so. Specify
     * the same <var>host</var> and <var>port</var> as in this method. If you
     * have not given any authorization info and the proxy server requires
     * authorization then you will be prompted for the necessary info via a
     * popup the first time you do a request.
     *
     * @param host  the host on which the proxy server resides.
     * @param port  the port the proxy server is listening on.
     * @see         #setCurrentProxy(java.lang.String, int)
     */
    public static void setProxyServer(String host, int port)
    {
        if (host == null || host.trim().length() == 0)
        {
            Default_Proxy_Host = null;
        }
        else
        {
            Default_Proxy_Host = host.trim().toLowerCase();
            Default_Proxy_Port = port;
        }
    }


    /**
     * Sets the proxy used by this instance. This can be used to override the
     * proxy setting inherited from the default proxy setting. A null or empty
     * string <var>host</var> parameter disables the proxy. <P>
     *
     * Note that if you set a proxy for the connection using this method, and a
     * request made over this connection is redirected to a different server,
     * then the connection used for new server will <em>not</em> pick this proxy
     * setting, but instead will use the default proxy settings.
     *
     * @param host  the host the proxy runs on
     * @param port  the port the proxy is listening on
     * @see         #setProxyServer(java.lang.String, int)
     */
    public synchronized void setCurrentProxy(String host, int port)
    {
        if (host == null || host.trim().length() == 0)
        {
            Proxy_Host = null;
        }
        else
        {
            Proxy_Host = host.trim().toLowerCase();
            if (port <= 0)
            {
                Proxy_Port = 80;
            }
            else
            {
                Proxy_Port = port;
            }
        }

        // the proxy might be talking a different version, so renegotiate
        switch (Protocol)
        {
            case HTTP:
            case HTTPS:
                if (force_1_0)
                {
                    ServerProtocolVersion = HTTP_1_0;
                    ServProtVersKnown = true;
                    RequestProtocolVersion = "HTTP/1.0";
                }
                else
                {
                    ServerProtocolVersion = HTTP_1_1;
                    ServProtVersKnown = false;
                    RequestProtocolVersion = "HTTP/1.1";
                }
                break;
            case HTTP_NG:
                ServerProtocolVersion = -1;
                /*
                 *  Unknown
                 */
                ServProtVersKnown = false;
                RequestProtocolVersion = "";
                break;
            case SHTTP:
                ServerProtocolVersion = -1;
                /*
                 *  Unknown
                 */
                ServProtVersKnown = false;
                RequestProtocolVersion = "Secure-HTTP/1.3";
                break;
            default:
                throw new Error("HTTPClient Internal Error: invalid protocol " +
                        Protocol);
        }

        keepAliveUnknown = true;
        doesKeepAlive = false;

        input_demux = null;
        early_stall = null;
        late_stall = null;
        prev_resp = null;
    }


    /**
     * Add <var>host</var> to the list of hosts which should be accessed
     * directly, not via any proxy set by <code>setProxyServer()</code>. <P>
     *
     * The <var>host</var> may be any of:
     * <UL>
     *   <LI> a complete host name (e.g. "www.disney.com")
     *   <LI> a domain name; domain names must begin with a dot (e.g.
     *   ".disney.com")
     *   <LI> an IP-address (e.g. "12.34.56.78")
     *   <LI> an IP-subnet, specified as an IP-address and a netmask separated
     *   by a "/" (e.g. "34.56.78/255.255.255.192"); a 0 bit in the netmask
     *   means that that bit won't be used in the comparison (i.e. the addresses
     *   are AND'ed with the netmask before comparison).
     * </UL>
     * <P>
     *
     * The two properties <var>HTTPClient.nonProxyHosts</var> and <var>
     * http.nonProxyHosts</var> are used when this class is loaded to initialize
     * the list of non-proxy hosts. The second property is only read if the
     * first one is not set; the second property is also used the JDK's
     * URLConnection. These properties must contain a "|" separated list of
     * entries which conform to the above rules for the <var>host</var>
     * parameter (e.g. "11.22.33.44|.disney.com").
     *
     * @param host                a host name, domain name, IP-address or
     *      IP-subnet.
     * @exception ParseException  if the length of the netmask does not match
     *      the length of the IP-address
     */
    public static void dontProxyFor(String host)
        throws ParseException
    {
        host = host.trim().toLowerCase();

        // check for domain name

        if (host.charAt(0) == '.')
        {
            if (!non_proxy_dom_list.contains(host))
            {
                non_proxy_dom_list.addElement(host);
            }
            return;
        }

        // check for host name

        for (int idx = 0; idx < host.length(); idx++)
        {
            if (!Character.isDigit(host.charAt(idx)) &&
                    host.charAt(idx) != '.' && host.charAt(idx) != '/')
            {
                non_proxy_host_list.put(host, "");
                return;
            }
        }

        // must be an IP-address

        byte[] ip_addr;
        byte[] ip_mask;
        int slash;
        if ((slash = host.indexOf('/')) != -1)
        {
            // IP subnet

            ip_addr = string2arr(host.substring(0, slash));
            ip_mask = string2arr(host.substring(slash + 1));
            if (ip_addr.length != ip_mask.length)
            {
                throw new ParseException("length of IP-address (" +
                        ip_addr.length + ") != length of netmask (" +
                        ip_mask.length + ")");
            }
        }
        else
        {
            ip_addr = string2arr(host);
            ip_mask = new byte[ip_addr.length];
            for (int idx = 0; idx < ip_mask.length; idx++)
            {
                ip_mask[idx] = (byte) 255;
            }
        }

        // check if addr or subnet already exists

        ip_loop :
        for (int idx = 0; idx < non_proxy_addr_list.size(); idx++)
        {
            byte[] addr = (byte[]) non_proxy_addr_list.elementAt(idx);
            byte[] mask = (byte[]) non_proxy_mask_list.elementAt(idx);
            if (addr.length != ip_addr.length)
            {
                continue;
            }

            for (int idx2 = 0; idx2 < addr.length; idx2++)
            {
                if ((ip_addr[idx2] & mask[idx2]) != (addr[idx2] & mask[idx2]) ||
                        (mask[idx2] != ip_mask[idx2]))
                {
                    continue ip_loop;
                }
            }

            return;
            // already exists
        }
        non_proxy_addr_list.addElement(ip_addr);
        non_proxy_mask_list.addElement(ip_mask);
    }


    /**
     * Convenience method to add a number of hosts at once. If any one host is
     * null or cannot be parsed it is ignored.
     *
     * @param hosts  The list of hosts to set
     * @see          #dontProxyFor(java.lang.String)
     * @since        V0.3-2
     */
    public static void dontProxyFor(String[] hosts)
    {
        if (hosts == null || hosts.length == 0)
        {
            return;
        }

        for (int idx = 0; idx < hosts.length; idx++)
        {
            try
            {
                if (hosts[idx] != null)
                {
                    dontProxyFor(hosts[idx]);
                }
            }
            catch (ParseException pe)
            {
                // ignore it
            }
        }
    }


    /**
     * Remove <var>host</var> from the list of hosts for which the proxy should
     * not be used. This modifies the same list that <code>dontProxyFor()</code>
     * uses, i.e. this is used to undo a <code>dontProxyFor()</code> setting.
     * The syntax for <var>host</var> is specified in <code>dontProxyFor()</code>
     * .
     *
     * @param host                a host name, domain name, IP-address or
     *      IP-subnet.
     * @return                    true if the remove was sucessful, false
     *      otherwise
     * @exception ParseException  if the length of the netmask does not match
     *      the length of the IP-address
     * @see                       #dontProxyFor(java.lang.String)
     */
    public static boolean doProxyFor(String host)
        throws ParseException
    {
        host = host.trim().toLowerCase();

        // check for domain name

        if (host.charAt(0) == '.')
        {
            return non_proxy_dom_list.removeElement(host);
        }

        // check for host name

        for (int idx = 0; idx < host.length(); idx++)
        {
            if (!Character.isDigit(host.charAt(idx)) &&
                    host.charAt(idx) != '.' && host.charAt(idx) != '/')
            {
                return (non_proxy_host_list.remove(host) != null);
            }
        }

        // must be an IP-address

        byte[] ip_addr;
        byte[] ip_mask;
        int slash;
        if ((slash = host.indexOf('/')) != -1)
        {
            // IP subnet

            ip_addr = string2arr(host.substring(0, slash));
            ip_mask = string2arr(host.substring(slash + 1));
            if (ip_addr.length != ip_mask.length)
            {
                throw new ParseException("length of IP-address (" +
                        ip_addr.length + ") != length of netmask (" +
                        ip_mask.length + ")");
            }
        }
        else
        {
            ip_addr = string2arr(host);
            ip_mask = new byte[ip_addr.length];
            for (int idx = 0; idx < ip_mask.length; idx++)
            {
                ip_mask[idx] = (byte) 255;
            }
        }

        ip_loop :
        for (int idx = 0; idx < non_proxy_addr_list.size(); idx++)
        {
            byte[] addr = (byte[]) non_proxy_addr_list.elementAt(idx);
            byte[] mask = (byte[]) non_proxy_mask_list.elementAt(idx);
            if (addr.length != ip_addr.length)
            {
                continue;
            }

            for (int idx2 = 0; idx2 < addr.length; idx2++)
            {
                if ((ip_addr[idx2] & mask[idx2]) != (addr[idx2] & mask[idx2]) ||
                        (mask[idx2] != ip_mask[idx2]))
                {
                    continue ip_loop;
                }
            }

            non_proxy_addr_list.removeElementAt(idx);
            non_proxy_mask_list.removeElementAt(idx);
            return true;
        }
        return false;
    }


    /**
     * Turn an IP-address string into an array (e.g. "12.34.56.78" into { 12,
     * 34, 56, 78 }).
     *
     * @param ip  IP-address
     * @return    IP-address in network byte order
     */
    private static byte[] string2arr(String ip)
    {
        byte[] arr;
        char[] ip_char = new char[ip.length()];
        ip.getChars(0, ip_char.length, ip_char, 0);

        int cnt = 0;
        for (int idx = 0; idx < ip_char.length; idx++)
        {
            if (ip_char[idx] == '.')
            {
                cnt++;
            }
        }
        arr = new byte[cnt + 1];

        cnt = 0;
        int pos = 0;
        for (int idx = 0; idx < ip_char.length; idx++)
        {
            if (ip_char[idx] == '.')
            {
                arr[cnt] = (byte) Integer.parseInt(ip.substring(pos, idx));
                cnt++;
                pos = idx + 1;
            }
        }
        arr[cnt] = (byte) Integer.parseInt(ip.substring(pos));

        return arr;
    }


    /**
     * Sets the SOCKS server to use. The server will only be used for new
     * HTTPConnections created after this call and will not affect currrent
     * instances of HTTPConnection. A null or empty string host parameter
     * disables SOCKS. <P>
     *
     * The code will try to determine the SOCKS version to use at connection
     * time. This might fail for a number of reasons, however, in which case you
     * must specify the version explicitly.
     *
     * @param host  the host on which the proxy server resides. The port used is
     *      the default port 1080.
     * @see         #setSocksServer(java.lang.String, int, int)
     */
    public static void setSocksServer(String host)
    {
        setSocksServer(host, 1080);
    }


    /**
     * Sets the SOCKS server to use. The server will only be used for new
     * HTTPConnections created after this call and will not affect currrent
     * instances of HTTPConnection. A null or empty string host parameter
     * disables SOCKS. <P>
     *
     * The code will try to determine the SOCKS version to use at connection
     * time. This might fail for a number of reasons, however, in which case you
     * must specify the version explicitly.
     *
     * @param host  the host on which the proxy server resides.
     * @param port  the port the proxy server is listening on.
     * @see         #setSocksServer(java.lang.String, int, int)
     */
    public static void setSocksServer(String host, int port)
    {
        if (port <= 0)
        {
            port = 1080;
        }

        if (host == null || host.length() == 0)
        {
            Default_Socks_client = null;
        }
        else
        {
            Default_Socks_client = new SocksClient(host, port);
        }
    }


    /**
     * Sets the SOCKS server to use. The server will only be used for new
     * HTTPConnections created after this call and will not affect currrent
     * instances of HTTPConnection. A null or empty string host parameter
     * disables SOCKS. <P>
     *
     * In an application or using the Appletviewer an alternative to this method
     * is to set the following properties (either in the properties file or on
     * the command line): <var>HTTPClient.socksHost</var> , <var>
     * HTTPClient.socksPort</var> and <var>HTTPClient.socksVersion</var> .
     * Whether <var>HTTPClient.socksHost</var> is set or not determines whether
     * a SOCKS server is used; if <var>HTTPClient.socksPort</var> is not set it
     * defaults to 1080; if <var>HTTPClient.socksVersion</var> is not set an
     * attempt will be made to automatically determine the version used by the
     * server. <P>
     *
     * Note: If you have also set a proxy server then a connection will be made
     * to the SOCKS server, which in turn then makes a connection to the proxy
     * server (possibly via other SOCKS servers), which in turn makes the final
     * connection. <P>
     *
     * If the proxy server is running SOCKS version 5 and requires
     * username/password authorization, and you wish to set this authorization
     * information in the code, then you may use the <var>
     * AuthorizationInfo.addAuthorization()</var> method to do so. Specify the
     * same <var>host</var> and <var>port</var> as in this method, give the
     * <var>scheme</var> "SOCKS5" and the <var>realm</var> "USER/PASS", set the
     * <var>cookie</var> to null and the <var>params</var> to an array
     * containing a single <var>NVPair</var> in turn containing the username and
     * password. Example: <PRE>
     *     NVPair[] up = { new NVPair(username, password) };
     *     AuthorizationInfo.addAuthorization(host, port, "SOCKS5", "USER/PASS",
     *                                        null, up);
     * </PRE> If you have not given any authorization info and the proxy server
     * requires authorization then you will be prompted for the necessary info
     * via a popup the first time you do a request.
     *
     * @param host                the host on which the proxy server resides.
     * @param port                the port the proxy server is listening on.
     * @param version             the SOCKS version the server is running.
     *      Currently this must be '4' or '5'.
     * @exception SocksException  If <var>version</var> is not '4' or '5'.
     */
    public static void setSocksServer(String host, int port, int version)
        throws SocksException
    {
        if (port <= 0)
        {
            port = 1080;
        }

        if (host == null || host.length() == 0)
        {
            Default_Socks_client = null;
        }
        else
        {
            Default_Socks_client = new SocksClient(host, port, version);
        }
    }


    /**
     * Removes the #... part. Returns the stripped name, or "" if either the
     * <var>file</var> is null or is the empty string (after stripping).
     *
     * @param file  the name to strip
     * @return      the stripped name
     */
    private final String stripRef(String file)
    {
        if (file == null)
        {
            return "";
        }

        int hash = file.indexOf('#');
        if (hash != -1)
        {
            file = file.substring(0, hash);
        }

        return file.trim();
    }


    // private helper methods

    /**
     * Sets up the request, creating the list of headers to send and creating
     * instances of the modules. This may be invoked by subclasses which add
     * further methods (such as those from DAV and IPP).
     *
     * @param method               GET, POST, etc.
     * @param resource             the resource
     * @param headers              an array of headers to be used
     * @param entity               the entity (or null)
     * @param stream               the output stream (or null) - only one of
     *      stream and entity may be non-null
     * @return                     the response.
     * @exception ModuleException  if an exception is encountered in any module.
     * @exception IOException      Description of the Exception
     */
    protected final HTTPResponse setupRequest(String method, String resource,
            NVPair[] headers, byte[] entity,
            HttpOutputStream stream)
        throws IOException, ModuleException
    {
        Request req = new Request(this, method, resource,
                mergedHeaders(headers), entity, stream,
                allowUI);
        RequestList.addToEnd(req);

        try
        {
            HTTPResponse resp = new HTTPResponse(gen_mod_insts(), Timeout, req, defaultIncrement);
            handleRequest(req, resp, null, true);
            return resp;
        }
        finally
        {
            RequestList.remove(req);
        }
    }


    /**
     * This merges built-in default headers, user-specified default headers, and
     * method-specified headers. Method-specified take precedence over user
     * defaults, which take precedence over built-in defaults. The following
     * headers are removed if found: "Content-length".
     *
     * @param spec  the headers specified in the call to the method
     * @return      an array consisting of merged headers.
     */
    private NVPair[] mergedHeaders(NVPair[] spec)
    {
        int spec_len = (spec != null ? spec.length : 0);
        int
                defs_len;
        NVPair[] merged;

        synchronized (DefaultHeaders)
        {
            defs_len = (DefaultHeaders != null ? DefaultHeaders.length : 0);
            merged = new NVPair[spec_len + defs_len];

            // copy default headers
            System.arraycopy(DefaultHeaders, 0, merged, 0, defs_len);
        }

        // merge in selected headers
        int sidx;

        // merge in selected headers
        int didx = defs_len;
        for (sidx = 0; sidx < spec_len; sidx++)
        {
            if (spec[sidx] == null)
            {
                continue;
            }

            String s_name = spec[sidx].getName().trim();
            if (s_name.equalsIgnoreCase("Content-length"))
            {
                continue;
            }

            int search;
            for (search = 0; search < didx; search++)
            {
                if (merged[search].getName().trim().equalsIgnoreCase(s_name))
                {
                    break;
                }
            }

            merged[search] = spec[sidx];
            if (search == didx)
            {
                didx++;
            }
        }

        if (didx < merged.length)
        {
            merged = Util.resizeArray(merged, didx);
        }

        return merged;
    }


    /**
     * Generate an array of instances of the current modules.
     *
     * @return   Description of the Return Value
     */
    private HTTPClientModule[] gen_mod_insts()
    {
        synchronized (ModuleList)
        {
            HTTPClientModule[] mod_insts =
                    new HTTPClientModule[ModuleList.size()];

            for (int idx = 0; idx < ModuleList.size(); idx++)
            {
                Class mod = (Class) ModuleList.elementAt(idx);
                try
                {
                    mod_insts[idx] = (HTTPClientModule) mod.newInstance();
                }
                catch (Exception e)
                {
                    throw new Error("HTTPClient Internal Error: could not " +
                            "create instance of " + mod.getName() +
                            " -\n" + e);
                }
            }

            return mod_insts;
        }
    }


    /**
     * handles the Request. First the request handler for each module is is
     * invoked, and then if no response was generated the request is sent.
     *
     * @param req                  the Request
     * @param http_resp            the HTTPResponse
     * @param resp                 the Response
     * @param usemodules           if false then skip module loop
     * @exception IOException      if any module or sendRequest throws it
     * @exception ModuleException  if any module throws it
     */
    void handleRequest(Request req, HTTPResponse http_resp, Response resp,
            boolean usemodules)
        throws IOException, ModuleException
    {
        Response[] rsp_arr = {resp};
        HTTPClientModule[] modules = http_resp.getModules();

        // invoke requestHandler for each module

        if (usemodules)
        {
            doModules :
            for (int idx = 0; idx < modules.length; idx++)
            {
                int sts = modules[idx].requestHandler(req, rsp_arr);
                switch (sts)
                {
                    case REQ_CONTINUE:
                        // continue processing
                        break;
                    case REQ_RESTART:
                        // restart processing with first module
                        idx = -1;
                        continue doModules;
                    case REQ_SHORTCIRC:
                        // stop processing and send
                        break doModules;
                    case REQ_RESPONSE:
                    // go to phase 2
                    case REQ_RETURN:
                        // return response immediately
                        if (rsp_arr[0] == null)
                        {
                            throw new Error("HTTPClient Internal Error: no " +
                                    "response returned by module " +
                                    modules[idx].getClass().getName());
                        }
                        http_resp.set(req, rsp_arr[0]);
                        if (req.getStream() != null)
                        {
                            req.getStream().ignoreData(req);
                        }
                        if (req.internal_subrequest)
                        {
                            return;
                        }
                        if (sts == REQ_RESPONSE)
                        {
                            http_resp.handleResponse();
                        }
                        else
                        {
                            http_resp.init(rsp_arr[0]);
                        }
                        return;
                    case REQ_NEWCON_RST:
                        // new connection
                        if (req.internal_subrequest)
                        {
                            return;
                        }
                        req.getConnection().
                                handleRequest(req, http_resp, rsp_arr[0], true);
                        return;
                    case REQ_NEWCON_SND:
                        // new connection, send immediately
                        if (req.internal_subrequest)
                        {
                            return;
                        }
                        req.getConnection().
                                handleRequest(req, http_resp, rsp_arr[0], false);
                        return;
                    default:
                        // not valid
                        throw new Error("HTTPClient Internal Error: invalid status" +
                                " " + sts + " returned by module " +
                                modules[idx].getClass().getName());
                }
            }
        }

        if (req.internal_subrequest)
        {
            return;
        }

        // Send the request across the wire

        if (req.getStream() != null && req.getStream().getLength() == -1)
        {
            if (!ServProtVersKnown || ServerProtocolVersion < HTTP_1_1 ||
                    no_chunked)
            {
                req.getStream().goAhead(req, null, http_resp.getTimeout());
                http_resp.set(req, req.getStream());
            }
            else
            {
                // add Transfer-Encoding header if necessary
                int idx;
                NVPair[] hdrs = req.getHeaders();
                for (idx = 0; idx < hdrs.length; idx++)
                {
                    if (hdrs[idx].getName().equalsIgnoreCase("Transfer-Encoding"))
                    {
                        break;
                    }
                }

                if (idx == hdrs.length)
                {
                    hdrs = Util.resizeArray(hdrs, idx + 1);
                    hdrs[idx] = new NVPair("Transfer-Encoding", "chunked");
                    req.setHeaders(hdrs);
                }
                else
                {
                    String v = hdrs[idx].getValue();
                    try
                    {
                        if (!Util.hasToken(v, "chunked"))
                        {
                            hdrs[idx] = new NVPair("Transfer-Encoding",
                                    v + ", chunked");
                        }
                    }
                    catch (ParseException pe)
                    {
                        throw new IOException(pe.toString());
                    }
                }

                http_resp.set(req, sendRequest(req, http_resp.getTimeout()));
            }
        }
        else
        {
            http_resp.set(req, sendRequest(req, http_resp.getTimeout()));
        }

        if (req.aborted)
        {
            throw new IOException("Request aborted by user");
        }
    }


    /**
     * These mark the response to stall the next request on, if any
     */
    private volatile Response early_stall = null;
    private volatile Response late_stall = null;
    private volatile Response prev_resp = null;
    /**
     * This marks the socket output stream as still being used
     */
    private boolean output_finished = true;


    /**
     * sends the request over the line.
     *
     * @param req                  the request
     * @param con_timeout          the timeout to use when establishing a socket
     *      connection; an InterruptedIOException is thrown if the procedure
     *      times out.
     * @return                     Description of the Return Value
     * @exception IOException      if thrown by the socket
     * @exception ModuleException  if any module throws it during the SSL-
     *      tunneling handshake
     */
    Response sendRequest(Request req, int con_timeout)
        throws IOException, ModuleException
    {
        ByteArrayOutputStream hdr_buf = new ByteArrayOutputStream(600);
        Response resp = null;
        boolean keep_alive;

        // The very first request is special in that we need its response
        // before any further requests may be made. This is to set things
        // like the server version.

        if (early_stall != null)
        {
            try
            {
                Log.write(Log.CONN, "Conn:  Early-stalling Request: " +
                        req.getMethod() + " " +
                        req.getRequestURI());

                synchronized (early_stall)
                {
                    // wait till the response is received
                    try
                    {
                        early_stall.getVersion();
                    }
                    catch (IOException ioe)
                    {
                    }
                    early_stall = null;
                }
            }
            catch (NullPointerException npe)
            {
            }
        }

        String[] con_hdrs = assembleHeaders(req, hdr_buf);

        // determine if the connection should be kept alive after this
        // request

        try
        {
            if (ServerProtocolVersion >= HTTP_1_1 &&
                    !Util.hasToken(con_hdrs[0], "close")
                     ||
                    ServerProtocolVersion == HTTP_1_0 &&
                    Util.hasToken(con_hdrs[0], "keep-alive")
                    )
            {
                keep_alive = true;
            }
            else
            {
                keep_alive = false;
            }
        }
        catch (ParseException pe)
        {
            throw new IOException(pe.toString());
        }

        synchronized (this)
        {
            // Sometimes we must stall the pipeline until the previous request
            // has been answered. However, if we are going to open up a new
            // connection anyway we don't really need to stall.

            if (late_stall != null)
            {
                if (input_demux != null || keepAliveUnknown)
                {
                    Log.write(Log.CONN, "Conn:  Stalling Request: " +
                            req.getMethod() + " " + req.getRequestURI());

                    try
                    {
                        // wait till the response is received

                        late_stall.getVersion();
                        if (keepAliveUnknown)
                        {
                            determineKeepAlive(late_stall);
                        }
                    }
                    catch (IOException ioe)
                    {
                    }
                }

                late_stall = null;
            }

            /*
             *  POSTs must not be pipelined because of problems if the connection
             *  is aborted. Since it is generally impossible to know what urls
             *  POST will influence it is impossible to determine if a sequence
             *  of requests containing a POST is idempotent.
             *  Also, for retried requests we don't want to pipeline either.
             */
            if ((req.getMethod().equals("POST") || req.dont_pipeline) &&
                    prev_resp != null && input_demux != null)
            {
                Log.write(Log.CONN, "Conn:  Stalling Request: " +
                        req.getMethod() + " " + req.getRequestURI());

                try
                {
                    // wait till the response is received
                    prev_resp.getVersion();
                }
                catch (IOException ioe)
                {
                }
            }

            // If the previous request used an output stream, then wait till
            // all the data has been written

            if (!output_finished)
            {
                try
                {
                    wait();
                }
                catch (InterruptedException ie)
                {
                    throw new IOException(ie.toString());
                }
            }

            if (req.aborted)
            {
                throw new IOException("Request aborted by user");
            }

            int try_count = 3;
            /*
             *  what a hack! This is to handle the case where the server closes
             *  the connection but we don't realize it until we try to send
             *  something. The problem is that we only get IOException, but
             *  we need a finer specification (i.e. whether it's an EPIPE or
             *  something else); I don't trust relying on the message part
             *  of IOException (which on SunOS/Solaris gives 'Broken pipe',
             *  but what on Windoze/Mac?).
             */
            while (try_count-- > 0)
            {
                try
                {
                    // get a client socket

                    Socket sock;
                    if (input_demux == null ||
                            (sock = input_demux.getSocket()) == null)
                    {
                        sock = getSocket(con_timeout);

                        if (Protocol == HTTPS)
                        {
                            if (Proxy_Host != null)
                            {
                                Socket[] sarr = {sock};
                                resp = enableSSLTunneling(sarr, req, con_timeout);
                                if (resp != null)
                                {
                                    resp.final_resp = true;
                                    return resp;
                                }
                                sock = sarr[0];
                            }

                            sock.setSoTimeout(con_timeout);
                            //sock = new SSLSocket(sock);
                        }

                        input_demux = new StreamDemultiplexor(Protocol, sock, this);
                        DemuxList.addToEnd(input_demux);
                        keepAliveReqLeft = keepAliveReqMax;
                    }

                    if (req.aborted)
                    {
                        throw new IOException("Request aborted by user");
                    }

                    Log.write(Log.CONN, "Conn:  Sending Request: ", hdr_buf);

                    // Send headers

                    OutputStream sock_out = sock.getOutputStream();
                    if (haveMSLargeWritesBug)
                    {
                        sock_out = new MSLargeWritesBugStream(sock_out);
                    }

                    hdr_buf.writeTo(sock_out);

                    // Wait for "100 Continue" status if necessary

                    try
                    {
                        if (ServProtVersKnown &&
                                ServerProtocolVersion >= HTTP_1_1 &&
                                Util.hasToken(con_hdrs[1], "100-continue"))
                        {
                            resp = new Response(req, (Proxy_Host != null && Protocol != HTTPS), input_demux);
                            resp.timeout = 60;
                            if (resp.getContinue() != 100)
                            {
                                break;
                            }
                        }
                    }
                    catch (ParseException pe)
                    {
                        throw new IOException(pe.toString());
                    }
                    catch (InterruptedIOException iioe)
                    {
                    }
                    finally
                    {
                        if (resp != null)
                        {
                            resp.timeout = 0;
                        }
                    }

                    // POST/PUT data

                    if (req.getData() != null && req.getData().length > 0)
                    {
                        if (req.delay_entity > 0)
                        {
                            // wait for something on the network; check available()
                            // roughly every 100 ms

                            long num_units = req.delay_entity / 100;
                            long one_unit = req.delay_entity / num_units;

                            for (int idx = 0; idx < num_units; idx++)
                            {
                                if (input_demux.available(null) != 0)
                                {
                                    break;
                                }
                                try
                                {
                                    Thread.sleep(one_unit);
                                }
                                catch (InterruptedException ie)
                                {
                                }
                            }

                            if (input_demux.available(null) == 0)
                            {
                                sock_out.write(req.getData());
                            }
                            // he's still waiting
                            else
                            {
                                keep_alive = false;
                            }
                            // Uh oh!
                        }
                        else
                        {
                            sock_out.write(req.getData());
                        }
                    }

                    if (req.getStream() != null)
                    {
                        req.getStream().goAhead(req, sock_out, 0);
                    }
                    else
                    {
                        sock_out.flush();
                    }

                    // get a new response.
                    // Note: this does not do a read on the socket.

                    if (resp == null)
                    {
                        resp = new Response(req, (Proxy_Host != null &&
                                Protocol != HTTPS),
                                input_demux);
                    }
                }
                catch (IOException ioe)
                {
                    Log.write(Log.CONN, "Conn:  ", ioe);

                    closeDemux(ioe, true);

                    if (try_count == 0 || ioe instanceof UnknownHostException ||
                            ioe instanceof ConnectException ||
                            ioe instanceof NoRouteToHostException ||
                            ioe instanceof InterruptedIOException || req.aborted)
                    {
                        throw ioe;
                    }

                    Log.write(Log.CONN, "Conn:  Retrying request");
                    continue;
                }

                break;
            }

            prev_resp = resp;

            // close the stream after this response if necessary

            if ((!keepAliveUnknown && !doesKeepAlive) || !keep_alive ||
                    (keepAliveReqMax != -1 && keepAliveReqLeft-- == 0))
            {
                input_demux.markForClose(resp);
                input_demux = null;
            }
            else
            {
                input_demux.restartTimer();
            }

            if (keepAliveReqMax != -1)
            {
                Log.write(Log.CONN, "Conn:  Number of requests left: " +
                        keepAliveReqLeft);
            }

            /*
             *  We don't pipeline the first request, as we need some info
             *  about the server (such as which http version it complies with)
             */
            if (!ServProtVersKnown)
            {
                early_stall = resp;
                resp.markAsFirstResponse(req);
            }

            /*
             *  Also don't pipeline until we know if the server supports
             *  keep-alive's or not.
             *  Note: strictly speaking, HTTP/1.0 keep-alives don't mean we can
             *  pipeline requests. I seem to remember some (beta?) version
             *  of Netscape's Enterprise server which barfed if you tried
             *  push requests down it's throat w/o waiting for the previous
             *  response first. However, I've not been able to find such a
             *  server lately, and so I'm taking the risk and assuming we
             *  can in fact pipeline requests to HTTP/1.0 servers.
             */
            if (keepAliveUnknown ||
            // We don't pipeline POST's ...
                    !IdempotentSequence.methodIsIdempotent(req.getMethod()) ||
                    req.dont_pipeline ||
            // Retries disable pipelining too
                    neverPipeline)
            {
                // Emergency measure: prevent all pipelining
                late_stall = resp;
            }

            /*
             *  If there is an output stream then just tell the other threads to
             *  wait; the stream will notify() when it's done. If there isn't any
             *  stream then wake up a waiting thread (if any).
             */
            if (req.getStream() != null)
            {
                output_finished = false;
            }
            else
            {
                output_finished = true;
                notify();
            }

            // Looks like were finally done

            Log.write(Log.CONN, "Conn:  Request sent");
        }

        return resp;
    }


    /**
     * Gets a socket. Creates a socket to the proxy if set, or else to the
     * actual destination.
     *
     * @param con_timeout      if not 0 then start a new thread to establish the
     *      the connection and join(con_timeout) it. If the join() times out an
     *      InteruptedIOException is thrown.
     * @return                 The socket value
     * @exception IOException  Description of the Exception
     */
    private Socket getSocket(int con_timeout)
        throws IOException
    {
        Socket sock = null;

        String actual_host;
        int actual_port;

        if (Proxy_Host != null)
        {
            actual_host = Proxy_Host;
            actual_port = Proxy_Port;
        }
        else
        {
            actual_host = Host;
            actual_port = Port;
        }

        Log.write(Log.CONN, "Conn:  Creating Socket: " + actual_host + ":" +
                actual_port);

        if (con_timeout == 0)
        {
            // normal connection establishment

            if (Socks_client != null)
            {
                sock = Socks_client.getSocket(actual_host, actual_port);
            }
            else
            {
                // try all A records
                InetAddress[] addr_list = InetAddress.getAllByName(actual_host);
                for (int idx = 0; idx < addr_list.length; idx++)
                {
                    try
                    {
                        if (LocalAddr == null)
                        {
                            sock = new Socket(addr_list[idx], actual_port);
                        }
                        else
                        {
                            sock = new Socket(addr_list[idx], actual_port,
                                    LocalAddr, LocalPort);
                        }
                        break;
                        // success
                    }
                    catch (SocketException se)
                    {
                        if (idx == addr_list.length - 1)
                        {
                            throw se;
                        }
                        // we tried them all
                    }
                }
            }
        }
        else
        {
            EstablishConnection con =
                    new EstablishConnection(actual_host, actual_port, Socks_client);
            con.start();
            try
            {
                con.join((long) con_timeout);
            }
            catch (InterruptedException ie)
            {
            }

            if (con.getException() != null)
            {
                throw con.getException();
            }
            if ((sock = con.getSocket()) == null)
            {
                con.forget();
                if ((sock = con.getSocket()) == null)
                {
                    throw new InterruptedIOException("Connection establishment timed out");
                }
            }
        }

        return sock;
    }


    /**
     * Enable SSL Tunneling if we're talking to a proxy. See ietf draft
     * draft-luotonen-ssl-tunneling-03 for more info.
     *
     * @param sock                 the socket
     * @param req                  the request initiating this connection
     * @param timeout              the timeout
     * @return                     the proxy's last response if unsuccessful, or
     *      null if tunnel successfuly established
     * @exception IOException
     * @exception ModuleException
     */
    private Response enableSSLTunneling(Socket[] sock, Request req, int timeout)
        throws IOException, ModuleException
    {
        // copy User-Agent and Proxy-Auth headers from request

        Vector hdrs = new Vector();
        for (int idx = 0; idx < req.getHeaders().length; idx++)
        {
            String name = req.getHeaders()[idx].getName();
            if (name.equalsIgnoreCase("User-Agent") ||
                    name.equalsIgnoreCase("Proxy-Authorization"))
            {
                hdrs.addElement(req.getHeaders()[idx]);
            }
        }

        // create initial CONNECT subrequest

        NVPair[] h = new NVPair[hdrs.size()];
        hdrs.copyInto(h);
        Request connect = new Request(this, "CONNECT", Host + ":" + Port, h,
                null, null, req.allowUI());
        connect.internal_subrequest = true;

        ByteArrayOutputStream hdr_buf = new ByteArrayOutputStream(600);
        HTTPResponse r = new HTTPResponse(gen_mod_insts(), timeout, connect, defaultIncrement);

        // send and handle CONNECT request until successful or tired

        Response resp = null;

        while (true)
        {
            handleRequest(connect, r, resp, true);

            hdr_buf.reset();
            assembleHeaders(connect, hdr_buf);

            Log.write(Log.CONN, "Conn:  Sending SSL-Tunneling Subrequest: ",
                    hdr_buf);

            // send CONNECT

            hdr_buf.writeTo(sock[0].getOutputStream());

            // return if successful

            resp = new Response(connect, sock[0].getInputStream());
            if (resp.getStatusCode() == 200)
            {
                return null;
            }

            // failed!

            // make life easy: read data and close socket

            try
            {
                resp.getData();
            }
            catch (IOException ioe)
            {
            }
            try
            {
                sock[0].close();
            }
            catch (IOException ioe)
            {
            }

            // handle response

            r.set(connect, resp);
            if (!r.handleResponse())
            {
                return resp;
            }

            sock[0] = getSocket(timeout);
        }
    }


    /**
     * This writes out the headers on the <var>hdr_buf</var> . It takes special
     * precautions for the following headers: <DL> <DT>Content-type<DI>This is
     * only written if the request has an entity. If the request has an entity
     * and no content-type header was given for the request it defaults to
     * "application/octet-stream" <DT>Content-length<DI>This header is generated
     * if the request has an entity and the entity isn't being sent with the
     * Transfer-Encoding "chunked". <DT>User-Agent <DI>If not present it will be
     * generated with the current HTTPClient version strings. Otherwise the
     * version string is appended to the given User-Agent string. <DT>Connection
     * <DI>This header is only written if no proxy is used. If no connection
     * header is specified and the server is not known to understand HTTP/1.1 or
     * later then a "Connection: keep-alive" header is generated. <DT>
     * Proxy-Connection<DI>This header is only written if a proxy is used. If no
     * connection header is specified and the proxy is not known to understand
     * HTTP/1.1 or later then a "Proxy-Connection: keep-alive" header is
     * generated. <DT>Keep-Alive <DI>This header is only written if the
     * Connection or Proxy-Connection header contains the Keep-Alive token. <DT>
     * Expect <DI>If there is no entity and this header contains the
     * "100-continue" token then this token is removed. before writing the
     * header. <DT>TE <DI>If this header does not exist, it is created; else if
     * the "trailers" token is not specified this token is added; else the
     * header is not touched. </DL> Furthermore, it escapes various characters
     * in request-URI.
     *
     * @param req              the Request
     * @param hdr_buf          the buffer onto which to write the headers
     * @return                 an array of headers; the first element contains
     *      the the value of the Connection or Proxy-Connectin header, the
     *      second element the value of the Expect header.
     * @exception IOException  if writing on <var>hdr_buf</var> generates an an
     *      IOException, or if an error occurs during parsing of a header
     */
    private String[] assembleHeaders(Request req,
            ByteArrayOutputStream hdr_buf)
        throws IOException
    {
        DataOutputStream dataout = new DataOutputStream(hdr_buf);
        String[] con_hdrs = {"", ""};
        NVPair[] hdrs = req.getHeaders();

        // remember various headers

        int ho_idx = -1;

        // remember various headers

        int
                ct_idx = -1;

        // remember various headers

        int
                ua_idx = -1;

        // remember various headers

        int
                co_idx = -1;

        // remember various headers

        int
                pc_idx = -1;

        // remember various headers

        int
                ka_idx = -1;

        // remember various headers

        int
                ex_idx = -1;

        // remember various headers

        int
                te_idx = -1;

        // remember various headers

        int
                tc_idx = -1;

        // remember various headers

        int
                ug_idx = -1;
        for (int idx = 0; idx < hdrs.length; idx++)
        {
            String name = hdrs[idx].getName().trim().toLowerCase();
            if (name.equals("host"))
            {
                ho_idx = idx;
            }
            else if (name.equals("content-type"))
            {
                ct_idx = idx;
            }
            else if (name.equals("user-agent"))
            {
                ua_idx = idx;
            }
            else if (name.equals("connection"))
            {
                co_idx = idx;
            }
            else if (name.equals("proxy-connection"))
            {
                pc_idx = idx;
            }
            else if (name.equals("keep-alive"))
            {
                ka_idx = idx;
            }
            else if (name.equals("expect"))
            {
                ex_idx = idx;
            }
            else if (name.equals("te"))
            {
                te_idx = idx;
            }
            else if (name.equals("transfer-encoding"))
            {
                tc_idx = idx;
            }
            else if (name.equals("upgrade"))
            {
                ug_idx = idx;
            }
        }

        // Generate request line and Host header

        String file = Util.escapeUnsafeChars(req.getRequestURI());
        if (Proxy_Host != null && Protocol != HTTPS && !file.equals("*"))
        {
            dataout.writeBytes(req.getMethod() + " http://" + Host + ":" + Port +
                    file + " " + RequestProtocolVersion + "\r\n");
        }
        else
        {
            dataout.writeBytes(req.getMethod() + " " + file + " " +
                    RequestProtocolVersion + "\r\n");
        }

        String h_hdr = (ho_idx >= 0) ? hdrs[ho_idx].getValue().trim() : Host;
        if (Port != URI.defaultPort(getProtocol()))
        {
            dataout.writeBytes("Host: " + h_hdr + ":" + Port + "\r\n");
        }
        else
        {
            // Netscape-Enterprise has some bugs...
            dataout.writeBytes("Host: " + h_hdr + "\r\n");
        }

        /*
         *  What follows is the setup for persistent connections. We default
         *  to doing persistent connections for both HTTP/1.0 and HTTP/1.1,
         *  unless we're using a proxy server and HTTP/1.0 in which case we
         *  must make sure we don't do persistence (because of the problem of
         *  1.0 proxies blindly passing the Connection header on).
         *
         *  Note: there is a "Proxy-Connection" header for use with proxies.
         *  This however is only understood by Netscape and Netapp caches.
         *  Furthermore, it suffers from the same problem as the Connection
         *  header in HTTP/1.0 except that at least two proxies must be
         *  involved. But I've taken the risk now and decided to send the
         *  Proxy-Connection header. If I get complaints I'll remove it again.
         *
         *  In any case, with this header we can now modify the above to send
         *  the Proxy-Connection header whenever we wouldn't send the normal
         *  Connection header.
         */
        String co_hdr = null;
        if (!(ServProtVersKnown && ServerProtocolVersion >= HTTP_1_1 &&
                co_idx == -1))
        {
            if (co_idx == -1)
            {
                // no connection header given by user
                co_hdr = "Keep-Alive";
                con_hdrs[0] = "Keep-Alive";
            }
            else
            {
                con_hdrs[0] = hdrs[co_idx].getValue().trim();
                co_hdr = con_hdrs[0];
            }

            try
            {
                if (ka_idx != -1 &&
                        Util.hasToken(con_hdrs[0], "keep-alive"))
                {
                    dataout.writeBytes("Keep-Alive: " +
                            hdrs[ka_idx].getValue().trim() + "\r\n");
                }
            }
            catch (ParseException pe)
            {
                throw new IOException(pe.toString());
            }
        }

        if ((Proxy_Host != null && Protocol != HTTPS) &&
                !(ServProtVersKnown && ServerProtocolVersion >= HTTP_1_1))
        {
            if (co_hdr != null)
            {
                dataout.writeBytes("Proxy-Connection: ");
                dataout.writeBytes(co_hdr);
                dataout.writeBytes("\r\n");
                co_hdr = null;
            }
        }

        if (co_hdr != null)
        {
            try
            {
                if (!Util.hasToken(co_hdr, "TE"))
                {
                    co_hdr += ", TE";
                }
            }
            catch (ParseException pe)
            {
                throw new IOException(pe.toString());
            }
        }
        else
        {
            co_hdr = "TE";
        }

        if (ug_idx != -1)
        {
            co_hdr += ", Upgrade";
        }

        if (co_hdr != null)
        {
            dataout.writeBytes("Connection: ");
            dataout.writeBytes(co_hdr);
            dataout.writeBytes("\r\n");
        }

        // handle TE header

        if (te_idx != -1)
        {
            dataout.writeBytes("TE: ");
            Vector pte;
            try
            {
                pte = Util.parseHeader(hdrs[te_idx].getValue());
            }
            catch (ParseException pe)
            {
                throw new IOException(pe.toString());
            }

            if (!pte.contains(new HttpHeaderElement("trailers")))
            {
                dataout.writeBytes("trailers, ");
            }

            dataout.writeBytes(hdrs[te_idx].getValue().trim() + "\r\n");
        }
        else
        {
            dataout.writeBytes("TE: trailers\r\n");
        }

        // User-Agent

        if (ua_idx != -1)
        {
            dataout.writeBytes("User-Agent: " + hdrs[ua_idx].getValue().trim() + "\r\n");
        }
        else
        {
            dataout.writeBytes("User-Agent: " + version + "\r\n");
        }

        // Write out any headers left

        for (int idx = 0; idx < hdrs.length; idx++)
        {
            if (idx != ct_idx && idx != ua_idx && idx != co_idx &&
                    idx != pc_idx && idx != ka_idx && idx != ex_idx &&
                    idx != te_idx && idx != ho_idx)
            {
                dataout.writeBytes(hdrs[idx].getName().trim() + ": " +
                        hdrs[idx].getValue().trim() + "\r\n");
            }
        }

        // Handle Content-type, Content-length and Expect headers

        if (req.getData() != null || req.getStream() != null)
        {
            dataout.writeBytes("Content-type: ");
            if (ct_idx != -1)
            {
                dataout.writeBytes(hdrs[ct_idx].getValue().trim());
            }
            else
            {
                dataout.writeBytes("application/octet-stream");
            }
            dataout.writeBytes("\r\n");

            if (req.getData() != null)
            {
                dataout.writeBytes("Content-length: " + req.getData().length +
                        "\r\n");
            }
            else if (req.getStream().getLength() != -1 && tc_idx == -1)
            {
                dataout.writeBytes("Content-length: " +
                        req.getStream().getLength() + "\r\n");
            }

            if (ex_idx != -1)
            {
                con_hdrs[1] = hdrs[ex_idx].getValue().trim();
                dataout.writeBytes("Expect: " + con_hdrs[1] + "\r\n");
            }
        }
        else if (ex_idx != -1)
        {
            Vector expect_tokens;
            try
            {
                expect_tokens = Util.parseHeader(hdrs[ex_idx].getValue());
            }
            catch (ParseException pe)
            {
                throw new IOException(pe.toString());
            }

            // remove any 100-continue tokens

            HttpHeaderElement cont = new HttpHeaderElement("100-continue");
            while (expect_tokens.removeElement(cont))
            {
                ;
            }

            // write out header if any tokens left

            if (!expect_tokens.isEmpty())
            {
                con_hdrs[1] = Util.assembleHeader(expect_tokens);
                dataout.writeBytes("Expect: " + con_hdrs[1] + "\r\n");
            }
        }

        dataout.writeBytes("\r\n");
        // end of header

        return con_hdrs;
    }


    /**
     * The very first request is special in that we use it to figure out the
     * protocol version the server (or proxy) is compliant with.
     *
     * @param req              Description of the Parameter
     * @param resp             Description of the Parameter
     * @return                 true if all went fine, false if the request needs
     *      to be resent
     * @exception IOException  if any exception is thrown by the response
     */
    boolean handleFirstRequest(Request req, Response resp)
        throws IOException
    {
        // read response headers to get protocol version used by
        // the server.

        ServerProtocolVersion = String2ProtVers(resp.getVersion());
        ServProtVersKnown = true;

        /*
         *  We need to treat connections through proxies specially, because
         *  many HTTP/1.0 proxies do not downgrade an HTTP/1.1 response
         *  version to HTTP/1.0 (i.e. when we are talking to an HTTP/1.1
         *  server through an HTTP/1.0 proxy we are mislead to thinking we're
         *  talking to an HTTP/1.1 proxy). We use the absence of the Via
         *  header to detect whether we're talking to an HTTP/1.0 proxy,
         *  unless the status code indicates an error from the proxy
         *  itself. However, this only works when the chain contains
         *  only HTTP/1.0 proxies; if you have <client - 1.0 proxy - 1.1
         *  proxy - server> then this will fail too. Unfortunately there
         *  seems to be no way to reliably detect broken HTTP/1.0
         *  proxies...
         */
        int sts = resp.getStatusCode();
        if ((Proxy_Host != null && Protocol != HTTPS) &&
                resp.getHeader("Via") == null &&
                sts != 407 && sts != 502 && sts != 504)
        {
            ServerProtocolVersion = HTTP_1_0;
        }

        Log.write(Log.CONN, "Conn:  Protocol Version established: " +
                ProtVers2String(ServerProtocolVersion));

        // some (buggy) servers return an error status if they get a
        // version they don't comprehend

        if (ServerProtocolVersion == HTTP_1_0 &&
                (resp.getStatusCode() == 400 || resp.getStatusCode() == 500))
        {
            if (input_demux != null)
            {
                input_demux.markForClose(resp);
            }
            input_demux = null;
            RequestProtocolVersion = "HTTP/1.0";
            return false;
        }

        return true;
    }


    /**
     * Description of the Method
     *
     * @param resp             Description of the Parameter
     * @exception IOException  Description of the Exception
     */
    private void determineKeepAlive(Response resp)
        throws IOException
    {
        // try and determine if this server does keep-alives

        String con;

        try
        {
            if (ServerProtocolVersion >= HTTP_1_1 ||
                    (
                    (
                    ((Proxy_Host == null || Protocol == HTTPS) &&
                    (con = resp.getHeader("Connection")) != null)
                     ||
                    ((Proxy_Host != null && Protocol != HTTPS) &&
                    (con = resp.getHeader("Proxy-Connection")) != null)
                    ) &&
                    Util.hasToken(con, "keep-alive")
                    )
                    )
            {
                doesKeepAlive = true;
                keepAliveUnknown = false;

                Log.write(Log.CONN, "Conn:  Keep-Alive enabled");
            }
            else if (resp.getStatusCode() < 400)
            {
                keepAliveUnknown = false;
            }

            // get maximum number of requests

            if (doesKeepAlive && ServerProtocolVersion == HTTP_1_0 &&
                    (con = resp.getHeader("Keep-Alive")) != null)
            {
                HttpHeaderElement max =
                        Util.getElement(Util.parseHeader(con), "max");
                if (max != null && max.getValue() != null)
                {
                    keepAliveReqMax = Integer.parseInt(max.getValue());
                    keepAliveReqLeft = keepAliveReqMax;

                    Log.write(Log.CONN, "Conn:  Max Keep-Alive requests: " +
                            keepAliveReqMax);
                }
            }
        }
        catch (ParseException pe)
        {
        }
        catch (NumberFormatException nfe)
        {
        }
        catch (ClassCastException cce)
        {
        }
    }


    /**
     * Description of the Method
     */
    synchronized void outputFinished()
    {
        output_finished = true;
        notify();
    }


    /**
     * Description of the Method
     *
     * @param ioe        Description of the Parameter
     * @param was_reset  Description of the Parameter
     */
    synchronized void closeDemux(IOException ioe, boolean was_reset)
    {
        if (input_demux != null)
        {
            input_demux.close(ioe, was_reset);
        }

        early_stall = null;
        late_stall = null;
        prev_resp = null;
    }


    /**
     * Description of the Method
     *
     * @param prot_vers  Description of the Parameter
     * @return           Description of the Return Value
     */
    final static String ProtVers2String(int prot_vers)
    {
        return "HTTP/" + (prot_vers >>> 16) + "." + (prot_vers & 0xFFFF);
    }


    /**
     * Description of the Method
     *
     * @param prot_vers  Description of the Parameter
     * @return           Description of the Return Value
     */
    final static int String2ProtVers(String prot_vers)
    {
        String vers = prot_vers.substring(5);
        int dot = vers.indexOf('.');
        return Integer.parseInt(vers.substring(0, dot)) << 16 |
                Integer.parseInt(vers.substring(dot + 1));
    }


    /**
     * Generates a string of the form protocol://host.domain:port .
     *
     * @return   the string
     */
    public String toString()
    {
        return getProtocol() + "://" + getHost() +
                (getPort() != URI.defaultPort(getProtocol()) ? ":" + getPort() : "");
    }


    /**
     * Description of the Class
     *
     * @author    Administrator
     * @created   29. Dezember 2001
     */
    private class EstablishConnection extends Thread
    {
        String actual_host;
        int actual_port;
        IOException exception;
        Socket sock;
        SocksClient Socks_client;
        boolean close;


        /**
         * Constructor for the EstablishConnection object
         *
         * @param host   Description of the Parameter
         * @param port   Description of the Parameter
         * @param socks  Description of the Parameter
         */
        EstablishConnection(String host, int port, SocksClient socks)
        {
            super("EstablishConnection (" + host + ":" + port + ")");
            try
            {
                setDaemon(true);
            }
            catch (SecurityException se)
            {
            }
            // Oh well...

            actual_host = host;
            actual_port = port;
            Socks_client = socks;

            exception = null;
            sock = null;
            close = false;
        }


        /**
         * Main processing method for the EstablishConnection object
         */
        public void run()
        {
            try
            {
                if (Socks_client != null)
                {
                    sock = Socks_client.getSocket(actual_host, actual_port);
                }
                else
                {
                    // try all A records
                    InetAddress[] addr_list = InetAddress.getAllByName(actual_host);
                    for (int idx = 0; idx < addr_list.length; idx++)
                    {
                        try
                        {
                            if (LocalAddr == null)
                            {
                                sock = new Socket(addr_list[idx], actual_port);
                            }
                            else
                            {
                                sock = new Socket(addr_list[idx], actual_port,
                                        LocalAddr, LocalPort);
                            }
                            break;
                            // success
                        }
                        catch (SocketException se)
                        {
                            if (idx == addr_list.length - 1 || close)
                            {
                                throw se;
                            }
                            // we tried them all
                        }
                    }
                }
            }
            catch (IOException ioe)
            {
                exception = ioe;
            }

            if (close && sock != null)
            {
                try
                {
                    sock.close();
                }
                catch (IOException ioe)
                {
                }
                sock = null;
            }
        }


        /**
         * Gets the exception attribute of the EstablishConnection object
         *
         * @return   The exception value
         */
        IOException getException()
        {
            return exception;
        }


        /**
         * Gets the socket attribute of the EstablishConnection object
         *
         * @return   The socket value
         */
        Socket getSocket()
        {
            return sock;
        }


        /**
         * Description of the Method
         */
        void forget()
        {
            close = true;
        }
    }


    /**
     * M$ has yet another bug in their WinSock: if you try to write too much
     * data at once it'll hang itself. This filter therefore splits big writes
     * up into multiple writes of at most 20K.
     *
     * @author    Administrator
     * @created   29. Dezember 2001
     */
    private class MSLargeWritesBugStream extends FilterOutputStream
    {
        private final int CHUNK_SIZE = 20000;


        /**
         * Constructor for the MSLargeWritesBugStream object
         *
         * @param os  Description of the Parameter
         */
        MSLargeWritesBugStream(OutputStream os)
        {
            super(os);
        }


        /**
         * Description of the Method
         *
         * @param b                Description of the Parameter
         * @param off              Description of the Parameter
         * @param len              Description of the Parameter
         * @exception IOException  Description of the Exception
         */
        public void write(byte[] b, int off, int len)
            throws IOException
        {
            while (len > CHUNK_SIZE)
            {
                out.write(b, off, CHUNK_SIZE);
                off += CHUNK_SIZE;
                len -= CHUNK_SIZE;
            }
            out.write(b, off, len);
        }
    }

    public void setDefaultReadIncement(int increment)
    {
        this.defaultIncrement = increment;
    }

    int defaultIncrement = 1000;


}
