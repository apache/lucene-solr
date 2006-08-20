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
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Date;

import javax.servlet.http.HttpServletResponse;
import javax.xml.transform.Source;
import javax.xml.transform.Templates;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.GDataRequest.OutputFormat;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.utils.DateFormater;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.ExtensionProfile;
import com.google.gdata.util.common.xml.XmlWriter;
import com.google.gdata.util.common.xml.XmlWriter.Namespace;

/**
 * The FeedRequest Class wraps the current HttpServletResponse. Any action on the
 * HttpServletRequest will be executed via this class. This represents an
 * abstraction on the plain {@link HttpServletResponse}. Any action which has
 * to be performed on the underlying {@link HttpServletResponse} will be
 * executed within this class.
 * <p>
 * The GData basically writes two different kinds of response to the output
 * stream.
 * <ol>
 * <li>update, delete or insert requests will respond with a status code and if
 * successful the feed entry modified or created</li>
 * <li>get requests will respond with a status code and if successful the
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
    /**
     * Response code bad request
     */
    public static final int BAD_REQUEST = HttpServletResponse.SC_BAD_REQUEST;
    /**
     * Response code version conflict
     */
    public static final int CONFLICT = HttpServletResponse.SC_CONFLICT;
    /**
     * Response code forbidden access
     */
    public static final int FORBIDDEN = HttpServletResponse.SC_FORBIDDEN;
    /**
     * Response code internal server error
     */
    public static final int SERVER_ERROR = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
    /**
     * Response code not found
     */
    public static final int NOT_FOUND = HttpServletResponse.SC_NOT_FOUND;
    /**
     * Response code not modified since
     */
    public static final int NOT_MODIFIED = HttpServletResponse.SC_NOT_MODIFIED;
    /**
     * Response code created
     */
    public static final int CREATED = HttpServletResponse.SC_CREATED;
    /**
     * Response code unauthorized access
     */
    public static final int UNAUTHORIZED = HttpServletResponse.SC_UNAUTHORIZED;
    
    
    static final Log LOG = LogFactory.getLog(GDataResponse.class);
    private int error;

    private boolean isError = false;

    private String encoding;

    private OutputFormat outputFormat;

    private final HttpServletResponse response;

    protected static final String XMLMIME_ATOM = "text/xml";

    protected static final String XMLMIME_RSS = "text/xml";

    private static final String HEADER_LASTMODIFIED = "Last-Modified";

    /**
     * Creates a new GDataResponse
     * 
     * @param response -
     *            The underlying {@link HttpServletResponse}
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
     * Sets the status of the underlying response
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
     *             if an I/O Exception occurs
     */
    public void sendError() throws IOException {
        if (this.isError)
            this.response.sendError(this.error);
        
    }

    /**
     * @return - the {@link HttpServletResponse} writer
     * @throws IOException -
     *             If an I/O exception occurs
     */
    public Writer getWriter() throws IOException {
        return this.response.getWriter();
    }

    /**
     * Sends a response for a get e.g. query request. This method must not
     * invoked in a case of an error performing the requested action.
     * 
     * @param feed -
     *            the feed to respond to the client
     * @param service - the service to render the feed
     * 
     * @throws IOException -
     *             if an I/O exception occurs, often caused by an already
     *             closed Writer or OutputStream
     * 
     */
    public void sendResponse(final BaseFeed feed, final ProvidedService service)
            throws IOException {
        if (feed == null)
            throw new IllegalArgumentException("feed must not be null");
        if (service == null)
            throw new IllegalArgumentException(
                    "provided service must not be null");
        DateTime time = feed.getUpdated();
        if (time != null)
            setLastModifiedHeader(time.getValue());
        FormatWriter writer = FormatWriter.getFormatWriter(this,service);
        writer.generateOutputFormat(feed,this.response);

    }

    /**
     * 
     * Sends a response for an update, insert or delete request. This method
     * must not invoked in a case of an error performing the requested action. If
     * the specified response format is ATOM the default namespace will be set
     * to ATOM.
     * 
     * @param entry -
     *            the modified / created entry to send
     * @param service - the service to render the feed
     * @throws IOException -
     *             if an I/O exception occurs, often caused by an already
     *             closed Writer or OutputStream
     */
    public void sendResponse(BaseEntry entry, ProvidedService service)
            throws IOException {
        if (entry == null)
            throw new IllegalArgumentException("entry must not be null");
        if (service == null)
            throw new IllegalArgumentException(
                    "service must not be null");
        DateTime time = entry.getUpdated();
        if (time != null)
            setLastModifiedHeader(time.getValue());
        FormatWriter writer = FormatWriter.getFormatWriter(this,service);
        writer.generateOutputFormat(entry,this.response);

        
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

    private static abstract class FormatWriter{
        
        static FormatWriter getFormatWriter(final GDataResponse response, final ProvidedService service ){
            OutputFormat format = response.getOutputFormat();
            if(format == OutputFormat.HTML){
                return new HTMLFormatWriter(service);
            }
            return new SyndicateFormatWriter(service,format,response.getEncoding());
        }
        
        abstract void generateOutputFormat(final BaseFeed feed, final HttpServletResponse response) throws IOException;
        abstract void generateOutputFormat(final BaseEntry entry, final HttpServletResponse response) throws IOException;
        
        private static class HTMLFormatWriter extends FormatWriter{
            private static final String CONTENT_TYPE = "text/html";
            private final ProvidedService service;
            
            HTMLFormatWriter(final ProvidedService service){
                this.service = service;
            }
            @Override
            void generateOutputFormat(BaseFeed feed, final HttpServletResponse response) throws IOException {
                Templates template = this.service.getTransformTemplate();
                response.setContentType(CONTENT_TYPE);
                if(template == null){
                    sendNotAvailable(response);
                    return;
                }
                StringWriter writer = new StringWriter();
                XmlWriter xmlWriter = new XmlWriter(writer);
                feed.generateAtom(xmlWriter,this.service.getExtensionProfile());
                try {
                    writeHtml(template,response.getWriter(),writer);
                } catch (TransformerException e) {
                 LOG.error("Can not transform feed for service "+this.service.getName(),e);
                 sendNotAvailable(response);
                    
                }
            }

            @Override
            void generateOutputFormat(BaseEntry entry, final HttpServletResponse response)  throws IOException{
                Templates template = this.service.getTransformTemplate();
                response.setContentType(CONTENT_TYPE);
                if(template == null){
                    sendNotAvailable(response);
                    return;
                }
                StringWriter writer = new StringWriter();
                XmlWriter xmlWriter = new XmlWriter(writer);
                entry.generateAtom(xmlWriter,this.service.getExtensionProfile());
                try {
                    writeHtml(template,response.getWriter(),writer);
                } catch (TransformerException e) {
                 LOG.error("Can not transform feed for service "+this.service.getName(),e);
                 sendNotAvailable(response);
                    
                }

            }
            
            private void writeHtml(final Templates template, final Writer writer, final StringWriter source ) throws TransformerException{
                Transformer transformer = template.newTransformer();
                Source tranformSource = new StreamSource(new StringReader(source.toString())); 
                transformer.transform(tranformSource,new StreamResult(writer));
            }
            
            private void sendNotAvailable(final HttpServletResponse response) throws IOException{
                response.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE, "No transformation stylesheet available");
            }
            
        }
        private static class SyndicateFormatWriter extends FormatWriter{
            private static final String DEFAUL_NAMESPACE_URI = "http://www.w3.org/2005/Atom";

            private static final Namespace DEFAULT_NAMESPACE = new Namespace("",
                    DEFAUL_NAMESPACE_URI);
            private final ProvidedService service;
            private final String encoding;
            private final OutputFormat format;
            
            SyndicateFormatWriter(final ProvidedService service,final OutputFormat format, String encoding){
                this.service = service;
                this.format = format;
                this.encoding = encoding;
                
            }
            @Override
            void generateOutputFormat(final BaseFeed feed, final HttpServletResponse response) throws IOException {
                XmlWriter writer = null;
                try{
                 writer = createWriter(response.getWriter());
                if (this.format == OutputFormat.ATOM) {
                    response.setContentType(XMLMIME_ATOM);
                    feed.generateAtom(writer, this.service.getExtensionProfile());
                } else {
                    response.setContentType(XMLMIME_RSS);
                    feed.generateRss(writer, this.service.getExtensionProfile());
                }
                }finally{
                    if(writer != null)
                        writer.close();
                }
            }

            @Override
            void generateOutputFormat(final BaseEntry entry, final HttpServletResponse response) throws IOException {
                XmlWriter writer = null;
                try{
                 writer = createWriter(response.getWriter());
                if (this.format == OutputFormat.ATOM) {
                    response.setContentType(XMLMIME_ATOM);
                    entry.generateAtom(writer, this.service.getExtensionProfile());
                } else {
                    response.setContentType(XMLMIME_RSS);
                    entry.generateRss(writer, this.service.getExtensionProfile());
                }
                }finally{
                    if(writer != null)
                        writer.close();
                }
            }
            private XmlWriter createWriter(final Writer target) throws IOException {
                XmlWriter writer = new XmlWriter(target, this.encoding);
                // set the default namespace to Atom if Atom is the response format
                if (this.format == OutputFormat.ATOM)
                    writer.setDefaultNamespace(DEFAULT_NAMESPACE);
                return writer;
            }
        }
    }
    
    
    
}
