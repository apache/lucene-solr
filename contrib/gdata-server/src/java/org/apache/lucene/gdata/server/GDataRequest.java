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
import java.io.Reader;
import java.util.Enumeration;
import java.util.Map;
import java.util.StringTokenizer;

import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;

/**
 * The GDataRequest Class wraps the incoming HttpServletRequest. Needed
 * information coming with the HttpServletRequest can be accessed directly. It
 * represents an abstraction on the plain HttpServletRequest. Every GData
 * specific data coming from the client will be availiable and can be accessed
 * via the GDataRequest.
 * <p>
 * GDataRequest instances will be passed to any action requested by the client.
 * This class also holds the logic to retrieve important information like
 * response format, the reqeusted feed instance and query parameters.
 * 
 * </p>
 * 
 * @author Simon Willnauer
 * 
 */
/* this class might be extracted as an interface in later development */
public class GDataRequest {

    private static final Log LOG = LogFactory.getLog(GDataRequest.class);

    private static final String RESPONSE_FORMAT_PARAMETER = "alt";

    private static final String RESPONSE_FORMAT_PARAMETER_RSS = "rss";

    private static final int DEFAULT_ITEMS_PER_PAGE = 25;

    private static final int DEFAULT_START_INDEX = 1;

    private static final String START_INDEX_NEXT_PAGE_PARAMETER = "start-index";

    private static final String ITEMS_PER_PAGE_PARAMETER = "max-results";

    private String contextPath;

    @SuppressWarnings("unused")
    private static final String RESPONSE_FORMAT_PARAMETER_ATOM = "atom";

    private static final String HTTP_HEADER_IF_MODIFIED_SINCE = "If-Modified-Since";

    private static final String HTTP_HEADER_AUTH = "Authorization";

    // Atom is the default resopnse format
    private OutputFormat responseFormat = OutputFormat.ATOM;

    private final HttpServletRequest request;

    private String feedId = null;

    private String entryId = null;

    private ProvidedService configurator = null;

    private String entryVersion = null;

    private GDataRequestType type;

    /**
     * Creates a new FeedRequest
     * 
     * @param requst -
     *            the incoming HttpServletReqeust
     * @param type -
     *            the request type
     * 
     */
    public GDataRequest(final HttpServletRequest requst,
            final GDataRequestType type) {
        if (requst == null)
            throw new IllegalArgumentException("request must not be null ");
        if (type == null)
            throw new IllegalArgumentException("request type must not be null ");
        this.request = requst;
        this.type = type;

    }

    /**
     * Initialize the GDataRequest. This will initialize all needed values /
     * attributes in this request.
     * 
     * @throws GDataRequestException
     */
    public void initializeRequest() throws GDataRequestException {
        generateIdentificationProperties();
        setOutputFormat();
        // TODO remove this dependency
        StorageController controller = GDataServerRegistry.getRegistry()
                .lookup(StorageController.class,
                        ComponentType.STORAGECONTROLLER);
        try {

            Storage storage = controller.getStorage();

            String service = storage.getServiceForFeed(this.feedId);
            /*
             * ExtensionProfile and the type is used for building the Entry /
             * Feed Instances from an inputstream or reader
             * 
             */
            this.configurator = GDataServerRegistry.getRegistry()
                    .getProvidedService(service);
            if (this.configurator == null)
                throw new GDataRequestException(
                        "feed is not registered or extension profile could not be created");

        } catch (Exception e) {
            throw new GDataRequestException(
                    "feed is not registered or extension profile could not be created");
        }

    }

    /**
     * @return - the id of the requested feed
     */
    public String getFeedId() {

        return this.feedId;
    }

    /**
     * @return - the entry id of the requested Entry if specified, otherwise
     *         <code>null</code>
     */
    public String getEntryId() {

        return this.entryId;
    }

    /**
     * @return the version Id of the requested Entry if specified, otherwise
     *         <code>null</code>
     */
    public String getEntryVersion() {
        return this.entryVersion;
    }

    /**
     * A Reader instance to read form the client input stream
     * 
     * @return - the HttpServletRequest {@link Reader}
     * @throws IOException -
     *             if an I/O Exception occures
     */
    public Reader getReader() throws IOException {
        return this.request.getReader();
    }

    /**
     * Returns the {@link HttpServletRequest} parameter map containig all <i>GET</i>
     * request parameters.
     * 
     * @return the parameter map
     */
    @SuppressWarnings("unchecked")
    public Map<String, String[]> getQueryParameter() {
        return this.request.getParameterMap();
    }

    /**
     * The {@link HttpServletRequest} request parameter names
     * 
     * @return parameter names enumeration
     */
    @SuppressWarnings("unchecked")
    public Enumeration<String> getQueryParameterNames() {
        return this.request.getParameterNames();
    }

    /**
     * Either <i>Atom</i> or <i>RSS</i>
     * 
     * @return - The output format requested by the client
     */
    public OutputFormat getRequestedResponseFormat() {

        return this.responseFormat;
    }

    private void generateIdentificationProperties()
            throws GDataRequestException {
        /* generate all needed data to identify the requested feed/entry */
        String pathInfo = this.request.getPathInfo();
        /*
         * TODO this has to be changed to support the category queries. Category
         * queries could also be rewrited in the Servlet.
         */
        if (pathInfo.length() <= 1)
            throw new GDataRequestException(
                    "No feed or entry specified for this request");
        StringTokenizer tokenizer = new StringTokenizer(pathInfo, "/");
        this.feedId = tokenizer.nextToken();
        this.entryId = tokenizer.hasMoreTokens() ? tokenizer.nextToken() : "";
        this.entryVersion = tokenizer.hasMoreTokens() ? tokenizer.nextToken()
                : "";

    }

    private void setOutputFormat() {
        String formatParameter = this.request
                .getParameter(RESPONSE_FORMAT_PARAMETER);
        if (formatParameter == null)
            return;
        if (formatParameter.equalsIgnoreCase(RESPONSE_FORMAT_PARAMETER_RSS))
            this.responseFormat = OutputFormat.RSS;

    }

    /**
     * @return - the number of returned items per page
     */
    public int getItemsPerPage() {

        if (this.request.getParameter(ITEMS_PER_PAGE_PARAMETER) == null)
            return DEFAULT_ITEMS_PER_PAGE;
        int retval = -1;
        try {
            retval = new Integer(this.request
                    .getParameter(ITEMS_PER_PAGE_PARAMETER)).intValue();
        } catch (Exception e) {
            LOG.warn("Intems per page could not be parsed - " + e.getMessage(),
                    e);
        }

        return retval < 0 ? DEFAULT_ITEMS_PER_PAGE : retval;
    }

    /**
     * Start index represents the number of the first entry of the query -
     * result. The order depends on the query. Is the query a search query the
     * this value will be assinged to the score in a common feed query the value
     * will be assigned to the update time of the entries.
     * 
     * @return - the requested start index
     */
    public int getStartIndex() {
        if (this.request.getParameter(START_INDEX_NEXT_PAGE_PARAMETER) == null)
            return DEFAULT_START_INDEX;
        int retval = -1;
        try {
            retval = new Integer(this.request
                    .getParameter(START_INDEX_NEXT_PAGE_PARAMETER)).intValue();
        } catch (Exception e) {
            LOG.warn("Start-index could not be parsed - " + e.getMessage(), e);
        }
        return retval < 0 ? DEFAULT_START_INDEX : retval;
    }

    /**
     * The selfid is <i>href</i> pointing to the requested resource
     * 
     * @return - the self id
     */
    public String getSelfId() {
        StringBuilder builder = new StringBuilder();
        builder.append(buildRequestIDString(false));
        builder.append("?");
        builder.append(getQueryString());

        return builder.toString();
    }

    /**
     * The <i>href</i> id of the next page of the requested resource.
     * 
     * @return the id of the next page
     */
    public String getNextId() {
        StringBuilder builder = new StringBuilder();
        builder.append(buildRequestIDString(false));
        builder.append("?");

        Enumeration parameters = this.request.getParameterNames();
        while (parameters.hasMoreElements()) {
            String element = (String) parameters.nextElement();
            String[] values = this.request.getParameterValues(element);
            for (int i = 0; i < values.length; i++) {

                builder.append(element).append("=");
                if (element.equals(START_INDEX_NEXT_PAGE_PARAMETER)) {
                    int tempVal = DEFAULT_START_INDEX;
                    try {
                        tempVal = Integer.parseInt(values[i]);
                    } catch (Exception e) {
                        LOG.info("Can not parse StartIndex -- use defaut");
                    }
                    builder.append(tempVal + getItemsPerPage());
                    break;
                }

                builder.append(values[i]);

            }
            if (parameters.hasMoreElements())
                builder.append("&");

        }
        if (this.request.getParameter(ITEMS_PER_PAGE_PARAMETER) == null) {
            if (builder.charAt(builder.length() - 1) != '?')
                builder.append('&');
            builder.append(ITEMS_PER_PAGE_PARAMETER).append("=").append(
                    DEFAULT_ITEMS_PER_PAGE);
        }
        if (this.request.getParameter(START_INDEX_NEXT_PAGE_PARAMETER) == null) {
            builder.append('&');
            builder.append(START_INDEX_NEXT_PAGE_PARAMETER).append("=");
            builder.append(DEFAULT_ITEMS_PER_PAGE + 1);
        }

        return builder.toString();

    }

    private String buildRequestIDString(boolean endingSlash) {
        StringBuilder builder = new StringBuilder("http://");
        builder.append(this.request.getHeader("Host"));
        builder.append(this.request.getRequestURI());
        if (endingSlash && !this.request.getRequestURI().endsWith("/"))
            builder.append("/");

        return builder.toString();
    }

    /**
     * This will return the current query string including all parameters.
     * Additionaly the <code>max-resul</code> parameter will be added if not
     * specified.
     * <p>
     * <code>max-resul</code> indicates the number of results returned to the
     * client. The default value is 25.
     * </p>
     * 
     * @return - the query string incluing all parameters
     */
    public String getQueryString() {
        String retVal = this.request.getQueryString();

        if (this.request.getParameter(ITEMS_PER_PAGE_PARAMETER) != null)
            return retVal;
        String tempString = (retVal == null ? ITEMS_PER_PAGE_PARAMETER + "="
                + DEFAULT_ITEMS_PER_PAGE : "&" + ITEMS_PER_PAGE_PARAMETER + "="
                + DEFAULT_ITEMS_PER_PAGE);

        return retVal == null ? tempString : retVal + tempString;

    }

    /**
     * This enum represents the OutputFormat of the GDATA Server
     * 
     * @author Simon Willnauer
     * 
     */
    public static enum OutputFormat {
        /**
         * Output format ATOM. ATOM is the default response format.
         */
        ATOM,
        /**
         * Output format RSS
         */
        RSS
    }

    /**
     * Returns the requested path including the domain name and the requested
     * resource <i>http://www.apache.org/path/resource/</i>
     * 
     * @return the context path
     */
    public String getContextPath() {
        if (this.contextPath == null)
            this.contextPath = buildRequestIDString(true);
        return this.contextPath;
    }

    /**
     * Indicates the request type
     * 
     * @author Simon Willnauer
     * 
     */
    public enum GDataRequestType {
        /**
         * Type FeedRequest
         */
        GET,
        /**
         * Type UpdateRequest
         */
        UPDATE,
        /**
         * Type DeleteRequest
         */
        DELETE,
        /**
         * Type InsertRequest
         */
        INSERT
    }

    /**
     * {@link GDataRequestType}
     * 
     * @return the current request type
     */
    public GDataRequestType getType() {
        return this.type;
    }

    /**
     * If the reuquest is a {@link GDataRequestType#GET} request and there is no
     * entry id specified, the requested resource is a feed.
     * 
     * @return - <code>true</code> if an only if the requested resource is a
     *         feed
     */
    public boolean isFeedRequested() {

        return (this.type.equals(GDataRequestType.GET) && (this.entryId == null
                || this.entryId.length() == 0 || (this.entryId.equals('/'))));
    }

    /**
     * * If the reuquest is a {@link GDataRequestType#GET} request and there is
     * an entry id specified, the requested resource is an entry.
     * 
     * @return - <code>true</code> if an only if the requested resource is an
     *         entry
     */
    public boolean isEntryRequested() {
        return !this.isFeedRequested();
    }

    /**
     * @return the configuration for this request
     */
    public ProvidedService getConfigurator() {
        return this.configurator;
    }

    /**
     * @return - Returns the Internet Protocol (IP) address of the client or
     *         last proxy that sent the request.
     */
    public String getRemoteAddress() {
        return this.request.getRemoteAddr();
    }

    /**
     * @return - the value for the send auth token. The auth token will be send
     *         as a request <tt>Authentication</tt> header.
     */
    public String getAuthToken() {
        String token = this.request.getHeader(HTTP_HEADER_AUTH);
        if (token == null)
            return null;
        token = token.substring(token.indexOf("=") + 1);
        return token;
    }

    /**
     * @return - Returns an array containing all of the Cookie objects the
     *         client sent with underlaying HttpServletRequest.
     */
    public Cookie[] getCookies() {
        return this.request.getCookies();
    }

    /**
     * @return - the cookie set instead of the authentication token or
     *         <code>null</code> if not auth cookie is set
     */
    public Cookie getAuthCookie() {
        Cookie[] cookies = this.request.getCookies();
        if (cookies == null)
            return null;
        for (int i = 0; i < cookies.length; i++) {
            if (cookies[i].getName().equals(AuthenticationController.TOKEN_KEY))
                return cookies[i];
        }
        return null;
    }

    /**
     * @return - the date string of the <tt>If-Modified-Since</tt> HTTP
     *         request header, or null if header is not set
     */
    public String getModifiedSince() {
        return this.request.getHeader(HTTP_HEADER_IF_MODIFIED_SINCE);
    }

    /**
     * @return - the underlaying HttpServletRequest
     */
    public HttpServletRequest getHttpServletRequest() {

        return this.request;
    }
}
