/*
 *  @(#)HTTPResponse.java				0.3-3 06/05/2001
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
 *  You shou
 *  d have received a copy of the GNU Lesser General Public
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

import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.InputStream;
import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.Date;
import java.util.LinkedList;
import java.util.Enumeration;
import java.util.Iterator;

/**
 * This defines the http-response class returned by the requests. It's basically
 * a wrapper around the Response class which first lets all the modules handle
 * the response before finally giving the info to the user.
 *
 * @author    Ronald Tschalär
 * @created   29. Dezember 2001
 * @version   0.3-3 06/05/2001
 * @since     0.3
 */

class ByteBlock
{
    byte[] block;
    int length;
    ByteBlock(int size)
    {
        block = new byte[size];
    }
}

public class HTTPResponse implements HTTPClientModuleConstants
{
    /**
     * the list of modules
     */
    private HTTPClientModule[] modules;

    /**
     * the timeout for reads
     */
    private int timeout;

    /**
     * the request
     */
    private Request request = null;

    /**
     * the current response
     */
    Response response = null;

    /**
     * the HttpOutputStream to synchronize on
     */
    private HttpOutputStream out_stream = null;

    /**
     * our input stream from the stream demux
     */
    private InputStream inp_stream;

    /**
     * the status code returned.
     */
    private int StatusCode;

    /**
     * the reason line associated with the status code.
     */
    private String ReasonLine;

    /**
     * the HTTP version of the response.
     */
    private String Version;

    /**
     * the original URI used.
     */
    private URI OriginalURI = null;

    /**
     * the final URI of the document.
     */
    private URI EffectiveURI = null;

    /**
     * any headers which were received and do not fit in the above list.
     */
    private CIHashtable Headers = null;

    /**
     * any trailers which were received and do not fit in the above list.
     */
    private CIHashtable Trailers = null;

    /**
     * the ContentLength of the data.
     */
    private int ContentLength = -1;

    /**
     * the data (body) returned.
     */
    private byte[] Data = null;

    /**
     * signals if we have got and parsed the headers yet?
     */
    private boolean initialized = false;

    /**
     * signals if we have got the trailers yet?
     */
    private boolean got_trailers = false;

    /**
     * marks this response as aborted (stop() in HTTPConnection)
     */
    private boolean aborted = false;

    /**
     * should the request be retried by the application?
     */
    private boolean retry = false;

    /**
     * the method used in the request
     */
    private String method = null;


    // Constructors

    /**
     * Creates a new HTTPResponse.
     *
     * @param modules        the list of modules handling this response
     * @param timeout        the timeout to be used on stream read()'s
     * @param orig           Description of the Parameter
     * @param readIncrement  Description of the Parameter
     */
    HTTPResponse(HTTPClientModule[] modules, int timeout, Request orig, int readIncrement)
    {
        this.modules = modules;
        this.timeout = timeout;
        try
        {
            int qp = orig.getRequestURI().indexOf('?');
            this.OriginalURI = new URI(orig.getConnection().getProtocol(),
                    null,
                    orig.getConnection().getHost(),
                    orig.getConnection().getPort(),
                    qp < 0 ? orig.getRequestURI() :
                    orig.getRequestURI().substring(0, qp),
                    qp < 0 ? null :
                    orig.getRequestURI().substring(qp + 1),
                    null);
        }
        catch (ParseException pe)
        {
        }
        this.method = orig.getMethod();
        this.readIncrement = readIncrement;
    }


    int readIncrement = 1000;


    /**
     * Sets the readIncrement attribute of the HTTPResponse object
     *
     * @param readIncrement  The new readIncrement value
     */
    public void setReadIncrement(int readIncrement)
    {
        this.readIncrement = readIncrement;
    }


    /**
     * Gets the readIncrement attribute of the HTTPResponse object
     *
     * @return   The readIncrement value
     */
    public int getReadIncrement()
    {
        return this.readIncrement;
    }


    /**
     * @param req   the request
     * @param resp  the response
     */
    void set(Request req, Response resp)
    {
        this.request = req;
        this.response = resp;
        resp.http_resp = this;
        resp.timeout = timeout;
        this.aborted = resp.final_resp;
    }


    /**
     * @param req         the request
     * @param out_stream  Description of the Parameter
     */
    void set(Request req, HttpOutputStream out_stream)
    {
        this.request = req;
        this.out_stream = out_stream;
    }


    // Methods

    /**
     * Give the status code for this request. These are grouped as follows:
     * <UL>
     *   <LI> 1xx - Informational (new in HTTP/1.1)
     *   <LI> 2xx - Success
     *   <LI> 3xx - Redirection
     *   <LI> 4xx - Client Error
     *   <LI> 5xx - Server Error
     * </UL>
     *
     *
     * @return                     The statusCode value
     * @exception IOException      if any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public final int getStatusCode()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return StatusCode;
    }


    /**
     * Give the reason line associated with the status code.
     *
     * @return                     The reasonLine value
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public final String getReasonLine()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return ReasonLine;
    }


    /**
     * Get the HTTP version used for the response.
     *
     * @return                     The version value
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public final String getVersion()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return Version;
    }


    /**
     * Get the name and type of server.
     *
     * @return                     The server value
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     * @deprecated                 This method is a remnant of V0.1; use <code>getHeader("Server")</code>
     *      instead.
     * @see                        #getHeader(java.lang.String)
     */
    public final String getServer()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return getHeader("Server");
    }


    /**
     * Get the original URI used in the request.
     *
     * @return   the URI used in primary request
     */
    public final URI getOriginalURI()
    {
        return OriginalURI;
    }


    /**
     * Get the final URL of the document. This is set if the original request
     * was deferred via the "moved" (301, 302, or 303) return status.
     *
     * @return                     the effective URL, or null if no redirection
     *      occured
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     * @deprecated                 use getEffectiveURI() instead
     * @see                        #getEffectiveURI
     */
    public final URL getEffectiveURL()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        if (EffectiveURI != null)
        {
            return EffectiveURI.toURL();
        }
        return null;
    }


    /**
     * Get the final URI of the document. If the request was redirected via the
     * "moved" (301, 302, 303, or 307) return status this returns the URI used
     * in the last redirection; otherwise it returns the original URI.
     *
     * @return                     the effective URI
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public final URI getEffectiveURI()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        if (EffectiveURI != null)
        {
            return EffectiveURI;
        }
        return OriginalURI;
    }


    /**
     * Retrieves the value for a given header.
     *
     * @param hdr                  the header name.
     * @return                     the value for the header, or null if
     *      non-existent.
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public String getHeader(String hdr)
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return (String) Headers.get(hdr.trim());
    }


    /**
     * Retrieves the value for a given header. The value is parsed as an int.
     *
     * @param hdr                        the header name.
     * @return                           the value for the header if the header
     *      exists
     * @exception NumberFormatException  if the header's value is not a number
     *      or if the header does not exist.
     * @exception IOException            if any exception occurs on the socket.
     * @exception ModuleException        if any module encounters an exception.
     */
    public int getHeaderAsInt(String hdr)
        throws IOException, ModuleException, NumberFormatException
    {
        String val = getHeader(hdr);
        if (val == null)
        {
            throw new NumberFormatException("null");
        }
        return Integer.parseInt(val);
    }


    /**
     * Retrieves the value for a given header. The value is parsed as a date; if
     * this fails it is parsed as a long representing the number of seconds
     * since 12:00 AM, Jan 1st, 1970. If this also fails an exception is thrown.
     * <br>
     * Note: When sending dates use Util.httpDate().
     *
     * @param hdr                           the header name.
     * @return                              the value for the header, or null if
     *      non-existent.
     * @exception IllegalArgumentException  if the header's value is neither a
     *      legal date nor a number.
     * @exception IOException               if any exception occurs on the
     *      socket.
     * @exception ModuleException           if any module encounters an
     *      exception.
     */
    public Date getHeaderAsDate(String hdr)
        throws IOException, IllegalArgumentException, ModuleException
    {
        String raw_date = getHeader(hdr);
        if (raw_date == null)
        {
            return null;
        }

        // asctime() format is missing an explicit GMT specifier
        if (raw_date.toUpperCase().indexOf("GMT") == -1 &&
                raw_date.indexOf(' ') > 0)
        {
            raw_date += " GMT";
        }

        Date date;

        try
        {
            date = Util.parseHttpDate(raw_date);
        }
        catch (IllegalArgumentException iae)
        {
            // some servers erroneously send a number, so let's try that
            long time;
            try
            {
                time = Long.parseLong(raw_date);
            }
            catch (NumberFormatException nfe)
            {
                throw iae;
            }
            // give up
            if (time < 0)
            {
                time = 0;
            }
            date = new Date(time * 1000L);
        }

        return date;
    }


    /**
     * Returns an enumeration of all the headers available via getHeader().
     *
     * @return                     Description of the Return Value
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public Enumeration listHeaders()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }
        return Headers.keys();
    }


    /**
     * Retrieves the value for a given trailer. This should not be invoked until
     * all response data has been read. If invoked before it will call <code>getData()</code>
     * to force the data to be read.
     *
     * @param trailer              the trailer name.
     * @return                     the value for the trailer, or null if
     *      non-existent.
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     * @see                        #getData()
     */
    public String getTrailer(String trailer)
        throws IOException, ModuleException
    {
        if (!got_trailers)
        {
            getTrailers();
        }
        return (String) Trailers.get(trailer.trim());
    }


    /**
     * Retrieves the value for a given tailer. The value is parsed as an int.
     *
     * @param trailer                    the tailer name.
     * @return                           the value for the trailer if the
     *      trailer exists
     * @exception NumberFormatException  if the trailer's value is not a number
     *      or if the trailer does not exist.
     * @exception IOException            if any exception occurs on the socket.
     * @exception ModuleException        if any module encounters an exception.
     */
    public int getTrailerAsInt(String trailer)
        throws IOException, ModuleException, NumberFormatException
    {
        String val = getTrailer(trailer);
        if (val == null)
        {
            throw new NumberFormatException("null");
        }
        return Integer.parseInt(val);
    }


    /**
     * Retrieves the value for a given trailer. The value is parsed as a date;
     * if this fails it is parsed as a long representing the number of seconds
     * since 12:00 AM, Jan 1st, 1970. If this also fails an
     * IllegalArgumentException is thrown. <br>
     * Note: When sending dates use Util.httpDate().
     *
     * @param trailer                       the trailer name.
     * @return                              the value for the trailer, or null
     *      if non-existent.
     * @exception IllegalArgumentException  if the trailer's value is neither a
     *      legal date nor a number.
     * @exception IOException               if any exception occurs on the
     *      socket.
     * @exception ModuleException           if any module encounters an
     *      exception.
     */
    public Date getTrailerAsDate(String trailer)
        throws IOException, IllegalArgumentException, ModuleException
    {
        String raw_date = getTrailer(trailer);
        if (raw_date == null)
        {
            return null;
        }

        // asctime() format is missing an explicit GMT specifier
        if (raw_date.toUpperCase().indexOf("GMT") == -1 &&
                raw_date.indexOf(' ') > 0)
        {
            raw_date += " GMT";
        }

        Date date;

        try
        {
            date = Util.parseHttpDate(raw_date);
        }
        catch (IllegalArgumentException iae)
        {
            // some servers erroneously send a number, so let's try that
            long time;
            try
            {
                time = Long.parseLong(raw_date);
            }
            catch (NumberFormatException nfe)
            {
                throw iae;
            }
            // give up
            if (time < 0)
            {
                time = 0;
            }
            date = new Date(time * 1000L);
        }

        return date;
    }


    /**
     * Returns an enumeration of all the trailers available via getTrailer().
     *
     * @return                     Description of the Return Value
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public Enumeration listTrailers()
        throws IOException, ModuleException
    {
        if (!got_trailers)
        {
            getTrailers();
        }
        return Trailers.keys();
    }


    /**
     * Reads all the response data into a byte array. Note that this method
     * won't return until <em>all</em> the data has been received (so for
     * instance don't invoke this method if the server is doing a server push).
     * If <code>getInputStream()</code> had been previously invoked then this
     * method only returns any unread data remaining on the stream and then
     * closes it. <P>
     *
     * Note to the unwary: code like <PRE>
     *     System.out.println("The data: " + resp.getData())
     *</PRE> will probably not do what you want - use <PRE>
     *     System.out.println("The data: " + resp.getText())
     *</PRE> instead.
     *
     * @return                     an array containing the data (body) returned.
     *      If no data was returned then it's set to a zero-length array.
     * @exception IOException      If any io exception occured while reading the
     *      data
     * @exception ModuleException  if any module encounters an exception.
     * @see                        #getInputStream()
     */

    public byte[] getData() throws IOException, ModuleException
    {
        return getData(-1);
    }

    public byte[] getData(int max)
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }

        if (Data == null)
        {
            try
            {
                readResponseData(inp_stream, max);
            }
            catch (InterruptedIOException ie)
            {
                // don't intercept
                throw ie;
            }
            catch (IOException ioe)
            {
                Log.write(Log.RESP, "HResp: (\"" + method + " " +
                        OriginalURI.getPathAndQuery() + "\")");
                Log.write(Log.RESP, "       ", ioe);

                try
                {
                    inp_stream.close();
                }
                catch (Exception e)
                {
                }
                throw ioe;
            }

            inp_stream.close();
        }

        return Data;
    }


    /**
     * Reads all the response data into a buffer and turns it into a string
     * using the appropriate character converter. Since this uses {@link
     * #getData() getData()}, the caveats of that method apply here as well.
     *
     * @return                     the body as a String. If no data was returned
     *      then an empty string is returned.
     * @exception IOException      If any io exception occured while reading the
     *      data, or if the content is not text
     * @exception ModuleException  if any module encounters an exception.
     * @exception ParseException   if an error occured trying to parse the
     *      content-type header field
     * @see                        #getData()
     */
    public synchronized String getText()
        throws IOException, ModuleException, ParseException
    {
        String ct = getHeader("Content-Type");
        if (ct == null || !ct.toLowerCase().startsWith("text/"))
        {
            throw new IOException("Content-Type `" + ct + "' is not a text type");
        }

        String charset = Util.getParameter("charset", ct);
        if (charset == null)
        {
            charset = "ISO-8859-1";
        }

        return new String(getData(), charset);
    }


    /**
     * Gets an input stream from which the returned data can be read. Note that
     * if <code>getData()</code> had been previously invoked it will actually
     * return a ByteArrayInputStream created from that data.
     *
     * @return                     the InputStream.
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     * @see                        #getData()
     */
    public synchronized InputStream getInputStream()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            handleResponse();
        }

        if (Data == null)
        {
            return inp_stream;
        }
        else
        {
            getData();
            // ensure complete data is read
            return new ByteArrayInputStream(Data);
        }
    }


    /**
     * Should the request be retried by the application? If the application used
     * an <var>HttpOutputStream</var> in the request then various modules (such
     * as the redirection and authorization modules) are not able to resend the
     * request themselves. Instead, it becomes the application's responsibility.
     * The application can check this flag, and if it's set, resend the exact
     * same request. The modules such as the RedirectionModule or
     * AuthorizationModule will then recognize the resend and fix up or redirect
     * the request as required (i.e. they defer their normal action until the
     * resend). <P>
     *
     * If the application resends the request then it <strong>must</strong> use
     * the same <var>HttpOutputStream</var> instance. This is because the
     * modules use this to recognize the retried request and to perform the
     * necessary work on the request before it's sent. <P>
     *
     * Here is a skeleton example of usage: <PRE>
     *     OutputStream out = new HttpOutputStream(1234);
     *     do
     *     {
     *         rsp = con.Post("/cgi-bin/my_cgi", out);
     *         out.write(...);
     *         out.close();
     *     } while (rsp.retryRequest());
     *
     *     if (rsp.getStatusCode() >= 300)
     *         ...
     * </PRE> <P>
     *
     * Note that for this to ever return true, the java system property <var>
     * HTTPClient.deferStreamed</var> must be set to true at the beginning of
     * the application (before the HTTPConnection class is loaded). This
     * prevents unwary applications from causing inadvertent memory leaks. If an
     * application does set this, then it <em>must</em> resend any request whose
     * response returns true here in order to prevent memory leaks (a switch to
     * JDK 1.2 will allow us to use weak references and eliminate this problem).
     *
     * @return                     true if the request should be retried.
     * @exception IOException      If any exception occurs on the socket.
     * @exception ModuleException  if any module encounters an exception.
     */
    public boolean retryRequest()
        throws IOException, ModuleException
    {
        if (!initialized)
        {
            try
            {
                handleResponse();
            }
            catch (RetryException re)
            {
                this.retry = response.retry;
            }
        }
        return retry;
    }


    /**
     * produces a full list of headers and their values, one per line.
     *
     * @return   a string containing the headers
     */
    public String toString()
    {
        if (!initialized)
        {
            try
            {
                handleResponse();
            }
            catch (Exception e)
            {
                if (!(e instanceof InterruptedIOException))
                {
                    Log.write(Log.RESP, "HResp: (\"" + method + " " +
                            OriginalURI.getPathAndQuery() + "\")");
                    Log.write(Log.RESP, "       ", e);
                }
                return "Failed to read headers: " + e;
            }
        }

        String nl = System.getProperty("line.separator", "\n");

        StringBuffer str = new StringBuffer(Version);
        str.append(' ');
        str.append(StatusCode);
        str.append(' ');
        str.append(ReasonLine);
        str.append(nl);

        if (EffectiveURI != null)
        {
            str.append("Effective-URI: ");
            str.append(EffectiveURI);
            str.append(nl);
        }

        Enumeration hdr_list = Headers.keys();
        while (hdr_list.hasMoreElements())
        {
            String hdr = (String) hdr_list.nextElement();
            str.append(hdr);
            str.append(": ");
            str.append(Headers.get(hdr));
            str.append(nl);
        }

        return str.toString();
    }


    // Helper Methods


    /**
     * Gets the modules attribute of the HTTPResponse object
     *
     * @return   The modules value
     */
    HTTPClientModule[] getModules()
    {
        return modules;
    }


    /**
     * Processes a Response. This is done by calling the response handler in
     * each module. When all is done, the various fields of this instance are
     * intialized from the last Response.
     *
     * @return                     true if a new request was generated. This is
     *      used for internal subrequests only
     * @exception IOException      if any handler throws an IOException.
     * @exception ModuleException  if any module encounters an exception.
     */
    synchronized boolean handleResponse()
        throws IOException, ModuleException
    {
        if (initialized)
        {
            return false;
        }

        /*
         *  first get the response if necessary
         */
        if (out_stream != null)
        {
            response = out_stream.getResponse();
            response.http_resp = this;
            out_stream = null;
        }

        /*
         *  go through modules and handle them
         */
        doModules :
        while (true)
        {

            Phase1 :
            for (int idx = 0; idx < modules.length && !aborted; idx++)
            {
                try
                {
                    modules[idx].responsePhase1Handler(response, request);
                }
                catch (RetryException re)
                {
                    if (re.restart)
                    {
                        continue doModules;
                    }
                    else
                    {
                        throw re;
                    }
                }
            }

            Phase2 :
            for (int idx = 0; idx < modules.length && !aborted; idx++)
            {
                int sts = modules[idx].responsePhase2Handler(response, request);
                switch (sts)
                {
                    case RSP_CONTINUE:
                        // continue processing
                        break;
                    case RSP_RESTART:
                        // restart response processing
                        idx = -1;
                        continue doModules;
                    case RSP_SHORTCIRC:
                        // stop processing and return
                        break doModules;
                    case RSP_REQUEST:
                    // go to phase 1
                    case RSP_NEWCON_REQ:
                        // process the request using a new con
                        response.getInputStream().close();
                        if (handle_trailers)
                        {
                            invokeTrailerHandlers(true);
                        }
                        if (request.internal_subrequest)
                        {
                            return true;
                        }
                        request.getConnection().
                                handleRequest(request, this, response, true);
                        if (initialized)
                        {
                            break doModules;
                        }

                        idx = -1;
                        continue doModules;
                    case RSP_SEND:
                    // send the request immediately
                    case RSP_NEWCON_SND:
                        // send the request using a new con
                        response.getInputStream().close();
                        if (handle_trailers)
                        {
                            invokeTrailerHandlers(true);
                        }
                        if (request.internal_subrequest)
                        {
                            return true;
                        }
                        request.getConnection().
                                handleRequest(request, this, response, false);
                        idx = -1;
                        continue doModules;
                    default:
                        // not valid
                        throw new Error("HTTPClient Internal Error: invalid status" +
                                " " + sts + " returned by module " +
                                modules[idx].getClass().getName());
                }
            }

            Phase3 :
            for (int idx = 0; idx < modules.length && !aborted; idx++)
            {
                modules[idx].responsePhase3Handler(response, request);
            }

            break doModules;
        }

        /*
         *  force a read on the response in case none of the modules did
         */
        response.getStatusCode();

        /*
         *  all done, so copy data
         */
        if (!request.internal_subrequest)
        {
            init(response);
        }

        if (handle_trailers)
        {
            invokeTrailerHandlers(false);
        }

        return false;
    }


    /**
     * Copies the relevant fields from Response and marks this as initialized.
     *
     * @param resp  the Response class to copy from
     */
    void init(Response resp)
    {
        if (initialized)
        {
            return;
        }

        this.StatusCode = resp.StatusCode;
        this.ReasonLine = resp.ReasonLine;
        this.Version = resp.Version;
        this.EffectiveURI = resp.EffectiveURI;
        this.ContentLength = resp.ContentLength;
        this.Headers = resp.Headers;
        this.inp_stream = resp.inp_stream;
        this.Data = resp.Data;
        this.retry = resp.retry;
        initialized = true;
    }


    private boolean handle_trailers = false;
    private boolean trailers_handled = false;


    /**
     * This is invoked by the RespInputStream when it is close()'d. It just
     * invokes the trailer handler in each module.
     *
     * @param force                invoke the handlers even if not initialized
     *      yet?
     * @exception IOException      if thrown by any module
     * @exception ModuleException  if thrown by any module
     */
    void invokeTrailerHandlers(boolean force)
        throws IOException, ModuleException
    {
        if (trailers_handled)
        {
            return;
        }

        if (!force && !initialized)
        {
            handle_trailers = true;
            return;
        }

        for (int idx = 0; idx < modules.length && !aborted; idx++)
        {
            modules[idx].trailerHandler(response, request);
        }

        trailers_handled = true;
    }


    /**
     * Mark this request as having been aborted. It's invoked by
     * HTTPConnection.stop().
     */
    void markAborted()
    {
        aborted = true;
    }


    /**
     * Gets any trailers from the response if we haven't already done so.
     *
     * @exception IOException      Description of the Exception
     * @exception ModuleException  Description of the Exception
     */
    private synchronized void getTrailers()
        throws IOException, ModuleException
    {
        if (got_trailers)
        {
            return;
        }
        if (!initialized)
        {
            handleResponse();
        }

        response.getTrailer("Any");
        Trailers = response.Trailers;
        got_trailers = true;

        invokeTrailerHandlers(false);
    }


        /**
     * Reads the response data received. Does not return until either
     * Content-Length bytes have been read or EOF is reached.
     *
     * @param inp              Description of the Parameter
     * @exception IOException  if any read on the input stream fails
     * @inp                    the input stream from which to read the data
     */
    private void readResponseData(InputStream inp, int max)
        throws IOException, ModuleException
    {
        boolean readUnlimited = (max == -1);

        if (ContentLength == 0)
        {
            return;
        }

        if (Data == null)
        {
            Data = new byte[0];
        }

        // read response data

        int off = Data.length;

        try
        {
            // check Content-length header in case CE-Module removed it
            if (getHeader("Content-Length") != null)
            {
                int rcvd = 0;
                int total = max > 1 ? Math.min(ContentLength, max) : ContentLength;
                //System.out.println("Reading with max file size: " + total);
                Data = new byte[total];
                do
                {
                    off += rcvd;
                    rcvd = inp.read(Data, off, total - off);
                } while (rcvd != -1 && off + rcvd < total);
                // if max < ContentLength (&& max > -1): lose the rest
                /*if(total < ContentLength)
                {
                    inp.skip(ContentLength - total);
                }*/
                /*
                 *  Don't do this!
                 *  If we do, then getData() won't work after a getInputStream()
                 *  because we'll never get all the expected data. Instead, let
                 *  the underlying RespInputStream throw the EOF.
                 *  if (rcvd == -1)	// premature EOF
                 *  {
                 *  throw new EOFException("Encountered premature EOF while " +
                 *  "reading headers: received " + off +
                 *  " bytes instead of the expected " +
                 *  ContentLength + " bytes");
                 *  }
                 */
            }
            else
            {
                //System.out.println("Reading with unknown file size");
                java.util.LinkedList blocks = new java.util.LinkedList();
                //System.out.println("new LinkedList()");
                int total = 0;
                int secondBlockSize = 10*2000;
                byte[] secondBlock = new byte[secondBlockSize];
                //System.out.println("new byte[" + secondBlockSize + "]");
                int offInSecondBlock = 0;
                int rcvd = 0;
                do
                {
                    int bytesToRead = secondBlockSize - offInSecondBlock;
                    if(bytesToRead < 1)
                    {
                        // System.out.println("adding block to list...");
                        blocks.addLast(secondBlock);
                        secondBlock = new byte[secondBlockSize];
                        //System.out.println("new byte[" + secondBlockSize + "]");
                        offInSecondBlock = 0;
                        bytesToRead = secondBlockSize;
                    }
                    rcvd = inp.read(secondBlock, offInSecondBlock, bytesToRead);
                    //System.out.println("read " + rcvd);
                    // rcvd is usually << secondBlockSize
                    if(rcvd != -1)
                    {
                        offInSecondBlock += rcvd;
                        total += rcvd;
                        max -= rcvd;
                    }
                } while(rcvd != -1 && (readUnlimited || max > 0));

                // now we have: 1 x the last block as "secondBlock" + 0...n x blocks in the list
                Data = new byte[total];  // I can't see how to do it without this second buffer
                //System.out.println("new byte[" + total + "]");

                int offset = 0;
                while(blocks.size() > 0)
                {
                    byte[] block = (byte[]) blocks.removeFirst();
                    System.arraycopy(block, 0, Data, offset, block.length);
                    //System.out.println("System.arraycopy(" + block.length + ")");
                    offset += block.length;
                }
                if(offInSecondBlock > 0)
                {
                    //System.out.println("System.arraycopy(" + offInSecondBlock + ")");
                    System.arraycopy(secondBlock, 0, Data, offset, offInSecondBlock);
                }


            }
        }
        catch (IOException ioe)
        {
            Data = Util.resizeArray(Data, off);
            throw ioe;
        }
        finally
        {
            try
            {
                inp.close();
            }
            catch (IOException ioe)
            {
            }
        }
    }



    /*
     * Reads the response data received. Does not return until either
     * Content-Length bytes have been read or EOF is reached.
     *
     * @param inp                  Description of the Parameter
     * @exception IOException      if any read on the input stream fails
     * @exception ModuleException  Description of the Exception
     * @inp                        the input stream from which to read the data
     *
    private void readResponseData(InputStream inp)
        throws IOException, ModuleException
    {
        if (ContentLength == 0)
        {
            return;
        }

        if (Data == null)
        {
            Data = new byte[0];
        }

        // read response data

        int off = Data.length;

        LinkedList blocks = new java.util.LinkedList();

        // check Content-length header in case CE-Module removed it
        if (getHeader("Content-Length") != null)
        {
            try
            {
                int rcvd = 0;
                Data = new byte[ContentLength];

                do
                {
                    off += rcvd;
                    rcvd = inp.read(Data, off, ContentLength - off);
                } while (rcvd != -1 && off + rcvd < ContentLength);
                /*
                 *  Don't do this!
                 *  If we do, then getData() won't work after a getInputStream()
                 *  because we'll never get all the expected data. Instead, let
                 *  the underlying RespInputStream throw the EOF.
                 *  if (rcvd == -1)	// premature EOF
                 *  {
                 *  throw new EOFException("Encountered premature EOF while " +
                 *  "reading headers: received " + off +
                 *  " bytes instead of the expected " +
                 *  ContentLength + " bytes");
                 *  }
                 *
            }
            catch (IOException ioe)
            {
                Data = Util.resizeArray(Data, off);
                throw ioe;
            }
            finally
            {
                try
                {
                    inp.close();
                }
                catch (IOException ioe)
                {
                }
            }
        }
        else
        {
            int total = 0;
            int rcvd = 0;
            try
            {
                ByteBlock actBlock = new ByteBlock(this.readIncrement);
                // TODO: Blocks are very small (500-2000 Bytes) -> combine them
                while ((actBlock.length = inp.read(actBlock.block, 0, this.readIncrement)) != -1)
                {
                    total += actBlock.length;
                    // System.out.println(this.getOriginalURI().toExternalForm() + ": adding block with length " + actBlock.length + " complete: " + total);
                    blocks.add(actBlock);
                    actBlock = new ByteBlock(this.readIncrement);
                    //off += rcvd;
                    // Data = Util.resizeArray(Data, off + this.readIncrement);
                }
            }
            catch (IOException ioe)
            {
                throw ioe;
            }
            finally
            {
                Iterator it = blocks.iterator();
                Data = Util.resizeArray(Data, total);
                 off = 0;
                while (it.hasNext())
                {
                    ByteBlock act = (ByteBlock) it.next();
                    //System.out.println(this.getOriginalURI().toExternalForm() + ": copied " + act.length + " -> off: " + off + ", left: " + total);
                    System.arraycopy(act.block, 0, Data, off, act.length);
                    off += act.length;
                    total -= act.length;
                }
                try
                {
                    inp.close();
                }
                catch (IOException ioe)
                {
                }
            }
        }
    }
*/

    /**
     * Gets the timeout attribute of the HTTPResponse object
     *
     * @return   The timeout value
     */
    int getTimeout()
    {
        return timeout;
    }
}
