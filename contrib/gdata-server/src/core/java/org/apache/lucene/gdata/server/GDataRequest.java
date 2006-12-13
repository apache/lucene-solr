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
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.query.QueryTranslator;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;

/**
 * The GDataRequest Class wraps the incoming HttpServletRequest. Needed
 * information coming with the HttpServletRequest can be accessed directly. It
 * represents an abstraction on the plain HttpServletRequest. Every GData
 * specific data coming from the client will be available and can be accessed
 * via the GDataRequest.
 * <p>
 * GDataRequest instances will be passed to any action requested by the client.
 * This class also holds the logic to retrieve important information like
 * response format, the requested feed instance and query parameters.
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
    private static final String RESPONSE_FORMAT_PARAMETER_HTML = "html";

    private static final int DEFAULT_ITEMS_PER_PAGE = 25;

    private static final int DEFAULT_START_INDEX = 1;

    private static final String START_INDEX_NEXT_PAGE_PARAMETER = "start-index";

    private static final String ITEMS_PER_PAGE_PARAMETER = "max-results";

    private String contextPath;

    @SuppressWarnings("unused")
    private static final String RESPONSE_FORMAT_PARAMETER_ATOM = "atom";

    private static final String HTTP_HEADER_IF_MODIFIED_SINCE = "If-Modified-Since";

    private static final String HTTP_HEADER_AUTH = "Authorization";

    private static final Object CATEGORY_QUERY_INDICATOR = "-";

    // Atom is the default response format
    private OutputFormat responseFormat = OutputFormat.ATOM;

    private final HttpServletRequest request;

    private String feedId = null;

    private String entryId = null;

    private String service = null;

    private ProvidedService configurator = null;

    private boolean isSearchRequest = false;

    private String entryVersion = null;

    private GDataRequestType type;

    private String categoryQuery;
    
    private String translatedSearchQuery;

    private boolean isFeedRequest = false;

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
        
        try {
            /*
             * ExtensionProfile and the type is used for building the Entry /
             * Feed Instances from an input stream or reader
             * 
             */
            this.configurator = GDataServerRegistry.getRegistry()
                    .getProvidedService(this.service);
            
            if (this.configurator == null)
                throw new GDataRequestException(
                        "no Provided Service found for service id: "+this.service,GDataResponse.NOT_FOUND);
            applyRequestParameter();
            if(this.translatedSearchQuery != null)
                this.isSearchRequest = true;
        } catch(GDataRequestException ex){
            throw ex;
        } catch (Exception e) {
            throw new GDataRequestException(
                    "failed to initialize GDataRequest -- "
                            + e.getMessage(), e,GDataResponse.SERVER_ERROR);
        }
        
    }
    
    @SuppressWarnings("unchecked")
    private void applyRequestParameter() throws GDataRequestException{
        IndexSchema schema = this.configurator.getIndexSchema();
        try{
        this.translatedSearchQuery = QueryTranslator.translateHttpSearchRequest(schema,this.request.getParameterMap(),this.categoryQuery);
        }catch (Exception e) {
            throw new GDataRequestException("Can not translate user query to search query",e,GDataResponse.BAD_REQUEST);
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
     *             if an I/O Exception occurs
     */
    public Reader getReader() throws IOException {
        return this.request.getReader();
    }

    /**
     * Returns the {@link HttpServletRequest} parameter map containing all
     * <i>GET</i> request parameters.
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
              if (pathInfo.length() <= 1)
            throw new GDataRequestException(
                    "No feed or entry specified for this request",GDataResponse.BAD_REQUEST);
        StringTokenizer tokenizer = new StringTokenizer(pathInfo, "/");
        this.service = tokenizer.nextToken();
        if (!tokenizer.hasMoreTokens())
            throw new GDataRequestException(
                    "Can not find feed id in requested path " + pathInfo,GDataResponse.BAD_REQUEST);
        this.feedId = tokenizer.nextToken();

        String appendix = tokenizer.hasMoreTokens() ? tokenizer.nextToken()
                : null;
        if (appendix == null){
            this.isFeedRequest = true;
            return;
        }
        if (appendix.equals(CATEGORY_QUERY_INDICATOR)) {
            StringBuilder builder = new StringBuilder();
            while (tokenizer.hasMoreTokens())
                builder.append("/").append(tokenizer.nextToken());
            this.categoryQuery = builder.toString();
        } else {
            this.entryId = appendix;
            this.entryVersion = tokenizer.hasMoreTokens() ? tokenizer
                    .nextToken() : "";
        }
        this.isFeedRequest = (this.type == GDataRequestType.GET && (this.entryId == null
                || this.entryId.length() == 0 || (this.entryId.equals('/'))));
    }

    private void setOutputFormat() {
        String formatParameter = this.request
                .getParameter(RESPONSE_FORMAT_PARAMETER);
        if (formatParameter == null)
            return;
        if (formatParameter.equalsIgnoreCase(RESPONSE_FORMAT_PARAMETER_RSS))
            this.responseFormat = OutputFormat.RSS;
        if (formatParameter.equalsIgnoreCase(RESPONSE_FORMAT_PARAMETER_HTML))
            this.responseFormat = OutputFormat.HTML;

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
            LOG.warn("Items per page could not be parsed - " + e.getMessage(),
                    e);
        }

        return retval < 0 ? DEFAULT_ITEMS_PER_PAGE : retval;
    }

    /**
     * Start index represents the number of the first entry of the query -
     * result. The order depends on the query. Is the query a search query the
     * this value will be assigned to the score in a common feed query the value
     * will be assigned to the update time of the entries.
     * 
     * @return - the requested start index
     */
    public int getStartIndex() {
        String startIndex = this.request.getParameter(START_INDEX_NEXT_PAGE_PARAMETER);
        if (startIndex == null)
            return DEFAULT_START_INDEX;
        int retval = -1;
        try {
            retval = new Integer(startIndex).intValue();
        } catch (Exception e) {
            LOG.warn("Start-index could not be parsed - not an integer - " + e.getMessage());
        }
        return retval < 0 ? DEFAULT_START_INDEX : retval;
    }

    /**
     * The self id is the feeds <i>href</i> pointing to the requested resource
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
       * The previous id is the feeds <i>href</i> pointing to the previous result of the requested resource
     * 
     * @return - the self id
     */
    public String getPreviousId(){
        
        int startIndex = getStartIndex();
        if(startIndex == DEFAULT_START_INDEX )
            return null;
        StringBuilder builder = new StringBuilder();
        builder.append(buildRequestIDString(false));
        startIndex = startIndex-getItemsPerPage();
        builder.append(getPreparedQueryString(startIndex<1?DEFAULT_START_INDEX:startIndex));
        return builder.toString();
    }
  

    
    private String getPreparedQueryString(int startIndex){
        String queryString = this.request.getQueryString();
        String startIndexValue = this.request.getParameter(START_INDEX_NEXT_PAGE_PARAMETER);
        String maxResultsValue = this.request.getParameter(ITEMS_PER_PAGE_PARAMETER);
        
        StringBuilder builder = new StringBuilder("?");
        if(maxResultsValue == null){
            builder.append(ITEMS_PER_PAGE_PARAMETER).append("=").append(DEFAULT_ITEMS_PER_PAGE);
            builder.append("&");
        }
        if(startIndexValue== null){
            builder.append(START_INDEX_NEXT_PAGE_PARAMETER).append("=");
            builder.append(Integer.toString(startIndex));
            if(queryString!=null){
                builder.append("&");
                builder.append(queryString);
            }
        }else{
            builder.append(queryString.replaceAll("start-index=[\\d]*",START_INDEX_NEXT_PAGE_PARAMETER+"="+Integer.toString(startIndex)));
        }
        return builder.toString();
    }
    /**
     * The <i>href</i> id of the next page of the requested resource.
     * 
     * @return the id of the next page
     */
    public String getNextId() {
        int startIndex = getStartIndex();
        StringBuilder builder = new StringBuilder();
        builder.append(buildRequestIDString(false));
        startIndex = startIndex+getItemsPerPage();
        builder.append(getPreparedQueryString(startIndex));
        return builder.toString();

    }

    private String buildRequestIDString(boolean endingSlash) {
        StringBuilder builder = new StringBuilder("http://");
        builder.append(this.request.getHeader("Host"));
        builder.append(this.request.getRequestURI());
        if (!endingSlash && builder.charAt(builder.length() - 1) == '/')
            builder.setLength(builder.length() - 1);
        if (endingSlash && builder.charAt(builder.length() - 1) != '/')
            builder.append("/");

        return builder.toString();
    }

    /**
     * This will return the current query string including all parameters.
     * Additionally the <code>max-resul</code> parameter will be added if not
     * specified.
     * <p>
     * <code>max-resul</code> indicates the number of results returned to the
     * client. The default value is 25.
     * </p>
     * 
     * @return - the query string including all parameters
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
        RSS,
        /**
         * Output format html if user defined xsl style sheet is present 
         */
        HTML
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
     * If the request is a {@link GDataRequestType#GET} request and there is no
     * entry id specified, the requested resource is a feed.
     * 
     * @return - <code>true</code> if an only if the requested resource is a
     *         feed
     */
    public boolean isFeedRequested() {

        return this.isFeedRequest ;
    }

    /**
     * * If the request is a {@link GDataRequestType#GET} request and there is
     * an entry id specified, the requested resource is an entry.
     * 
     * @return - <code>true</code> if an only if the requested resource is an
     *         entry
     */
    public boolean isEntryRequested() {
        return !this.isFeedRequested();
    }
    /**
     * @return - <code>true</code> if an only if the user request is a search request, otherwise <code>false</code>
     */
    public boolean isSearchRequested(){
        return this.isSearchRequest;
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
     *         client sent with underlying HttpServletRequest.
     */
    public Cookie[] getCookies() {
        return this.request.getCookies();
    }

    /**
     * @return - the cookie set instead of the authentication token or
     *         <code>null</code> if no auth cookie is set
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
     * @return - the underlying HttpServletRequest
     */
    public HttpServletRequest getHttpServletRequest() {

        return this.request;
    }
    
    protected String getTranslatedQuery(){
        return this.translatedSearchQuery;
    }

 
}
