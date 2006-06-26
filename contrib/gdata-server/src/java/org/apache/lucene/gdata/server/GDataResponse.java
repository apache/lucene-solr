/** 
 * Copyright 2004 The Apache Software Foundation 
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at 
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License. 
 */

package org.apache.lucene.gdata.server;

import java.io.IOException;
import java.io.Writer;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;

import org.apache.lucene.gdata.server.GDataRequest.OutputFormat;
import org.apache.lucene.gdata.utils.DateFormater;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.util.common.xml.XmlWriter;
import com.google.gdata.util.common.xml.XmlWriter.Namespace;

/**
 * The FeedRequest Class wraps the curren HttpServletResponse. Any action on the
 * HttpServletRequest will be executed via this class. This represents an
 * abstraction on the plain {@link HttpServletResponse}. Any action which has
 * to be performed on the underlaying {@link HttpServletResponse} will be
 * executed within this class.
 * <p>
 * The GData basicly writes two different kinds ouf reponse to the output
 * stream.
 * <ol>
 * <li>update, delete or insert requests will respond with a statuscode and if
 * successful the feed entry modified or created</li>
 * <li>get requests will respond with a statuscode and if successful the
 * requested feed</li>
 * </ol>
 * 
 * For this purpose the {@link GDataResponse} class provides the overloaded
 * method
 * {@link org.apache.lucene.gdata.server.GDataResponse#sendResponse(BaseEntry, ExtensionProfile)}
 * which sends the entry e.g feed to the output stream.
 * </p>
 * <p>
 * This class will set the HTTP <tt>Last-Modified</tt> Header to enable
 * clients to send <tt>If-Modified-Since</tt> request header to avoid
 * retrieving the content again if it hasn't changed. If the content hasn't
 * changed since the If-Modified-Since time, then the GData service returns a
 * 304 (Not Modified) HTTP response.
 * </p>
 * 
 * 
 * 
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataResponse {
    private int error;

    private boolean isError = false;

    private String encoding;

    private OutputFormat outputFormat;

    private final HttpServletResponse response;

    protected static final String XMLMIME_ATOM = "text/xml";

    protected static final String XMLMIME_RSS = "text/xml";

    private static final String DEFAUL_NAMESPACE_URI = "http://www.w3.org/2005/Atom";

    private static final Namespace DEFAULT_NAMESPACE = new Namespace("",
            DEFAUL_NAMESPACE_URI);

    private static final String HEADER_LASTMODIFIED = "Last-Modified";

    /**
     * Creates a new GDataResponse
     * 
     * @param response -
     *            The underlaying {@link HttpServletResponse}
     */
    public GDataResponse(HttpServletResponse response) {
        if (response == null)
            throw new IllegalArgumentException("response must not be null");
        this.response = response;

    }

    /**
     * Sets an error code to this FeedResponse.
     * 
     * @param errorCode -
     *            {@link HttpServletResponse} error code
     */
    public void setError(int errorCode) {
        this.isError = true;
        this.error = errorCode;
    }

    /**
     * Sets the status of the underlaying response
     * 
     * @see HttpServletResponse
     * @param responseCode -
     *            the status of the response
     */
    public void setResponseCode(int responseCode) {
        this.response.setStatus(responseCode);
    }

    /**
     * This method sends the specified error to the user if set
     * 
     * @throws IOException -
     *             if an I/O Exception occures
     */
    public void sendError() throws IOException {
        if (this.isError)
            this.response.sendError(this.error);
        
    }

    /**
     * @return - the {@link HttpServletResponse} writer
     * @throws IOException -
     *             If an I/O exception occures
     */
    public Writer getWriter() throws IOException {
        return this.response.getWriter();
    }

    /**
     * Sends a response for a get e.g. query request. This method must not
     * invoked in a case of an error performing the requeste action.
     * 
     * @param feed -
     *            the feed to respond to the client
     * @param profile -
     *            the extension profil for the feed to write
     * @throws IOException -
     *             if an I/O exception accures, often caused by an already
     *             closed Writer or OutputStream
     * 
     */
    public void sendResponse(BaseFeed feed, ExtensionProfile profile)
            throws IOException {
        if (feed == null)
            throw new IllegalArgumentException("feed must not be null");
        if (profile == null)
            throw new IllegalArgumentException(
                    "extension profil must not be null");
        DateTime time = feed.getUpdated();
        if (time != null)
            setLastModifiedHeader(time.getValue());
        XmlWriter writer = createWriter();

        if (this.outputFormat.equals(OutputFormat.ATOM)) {
            this.response.setContentType(XMLMIME_ATOM);
            feed.generateAtom(writer, profile);
        } else {
            this.response.setContentType(XMLMIME_RSS);
            feed.generateRss(writer, profile);
        }

    }

    /**
     * 
     * Sends a response for an update, insert or delete request. This method
     * must not invoked in a case of an error performing the requeste action. If
     * the specified response format is ATOM the default namespace will be set
     * to ATOM.
     * 
     * @param entry -
     *            the modified / created entry to send
     * @param profile -
     *            the entries extension profile
     * @throws IOException -
     *             if an I/O exception accures, often caused by an already
     *             closed Writer or OutputStream
     */
    public void sendResponse(BaseEntry entry, ExtensionProfile profile)
            throws IOException {
        if (entry == null)
            throw new IllegalArgumentException("entry must not be null");
        if (profile == null)
            throw new IllegalArgumentException(
                    "extension profil must not be null");
        DateTime time = entry.getUpdated();
        if (time != null)
            setLastModifiedHeader(time.getValue());
        XmlWriter writer = createWriter();
        if (this.outputFormat.equals(OutputFormat.ATOM))
            entry.generateAtom(writer, profile);
        else
            entry.generateRss(writer, profile);
    }

    private XmlWriter createWriter() throws IOException {
        XmlWriter writer = new XmlWriter(getWriter(), this.encoding);
        // set the default namespace to Atom if Atom is the response format
        if (this.outputFormat.equals(OutputFormat.ATOM))
            writer.setDefaultNamespace(DEFAULT_NAMESPACE);
        return writer;
    }

    /**
     * This encoding will be used to encode the xml representation of feed or
     * entry written to the {@link HttpServletResponse} output stream.
     * 
     * @return - the entry / feed encoding
     */
    public String getEncoding() {
        return this.encoding;
    }

    /**
     * This encoding will be used to encode the xml representation of feed or
     * entry written to the {@link HttpServletResponse} output stream. <i>UTF-8</i>
     * <i>ISO-8859-1</i>
     * 
     * @param encoding -
     *            string represents the encoding
     */
    public void setEncoding(String encoding) {
        this.encoding = encoding;
    }

    /**
     * @return - the response
     *         {@link org.apache.lucene.gdata.server.GDataRequest.OutputFormat}
     */
    public OutputFormat getOutputFormat() {
        return this.outputFormat;
    }

    /**
     * @param outputFormat -
     *            the response
     *            {@link org.apache.lucene.gdata.server.GDataRequest.OutputFormat}
     */
    public void setOutputFormat(OutputFormat outputFormat) {
        this.outputFormat = outputFormat;
    }

    /**
     * @see Object#toString()
     */
    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder(" GDataResponse: ");
        builder.append("Error: ").append(this.error);
        builder.append(" outputFormat: ").append(getOutputFormat());
        builder.append(" encoding: ").append(this.encoding);

        return builder.toString();

    }

    protected void setLastModifiedHeader(long lastModified) {
        String lastMod = DateFormater.formatDate(new Date(lastModified),
                DateFormater.HTTP_HEADER_DATE_FORMAT);
        this.response.setHeader(HEADER_LASTMODIFIED, lastMod);
    }
    
    /**
     * @see HttpServletResponse#setStatus(int)
     * @param status - the request status code
     */
    public void setStatus(int status){
        this.response.setStatus(status);
    }

}
