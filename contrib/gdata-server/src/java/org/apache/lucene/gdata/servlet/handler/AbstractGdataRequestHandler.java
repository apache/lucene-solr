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
 
package org.apache.lucene.gdata.servlet.handler; 
 
import java.io.IOException; 
 
import javax.servlet.ServletException; 
import javax.servlet.http.HttpServletRequest; 
import javax.servlet.http.HttpServletResponse; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.gdata.server.GDataRequest; 
import org.apache.lucene.gdata.server.GDataRequestException; 
import org.apache.lucene.gdata.server.GDataResponse; 
import org.apache.lucene.gdata.server.Service; 
import org.apache.lucene.gdata.server.ServiceFactory; 
import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType; 
 
/** 
 * @author Simon Willnauer 
 *  
 */ 
public abstract class AbstractGdataRequestHandler implements 
        GDataRequestHandler { 
    private final static Log LOG = LogFactory 
            .getLog(AbstractGdataRequestHandler.class); 
 
     
    protected GDataRequest feedRequest; 
    protected GDataResponse feedResponse; 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.GDataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest, 
     *      javax.servlet.http.HttpServletResponse) 
     */ 
    public abstract void processRequest(HttpServletRequest request, 
            HttpServletResponse response) throws ServletException, IOException; 
 
    protected void initializeRequestHandler(final HttpServletRequest request, final HttpServletResponse response, final GDataRequestType type) 
            throws GDataRequestException { 
        this.feedRequest = new GDataRequest(request, type); 
        this.feedResponse = new GDataResponse(response); 
        try {        
            this.feedRequest.initializeRequest(); 
        } catch (GDataRequestException e) { 
            this.feedResponse.setError(HttpServletResponse.SC_NOT_FOUND); 
            LOG.warn("Couldn't initialize FeedRequest - " + e.getMessage(), e); 
            throw e; 
        } 
    } 
 
     
 
    protected void sendError() throws IOException { 
        this.feedResponse.sendError(); 
         
    } 
 
    protected void setFeedResponseFormat() { 
        this.feedResponse.setOutputFormat(this.feedRequest.getRequestedResponseFormat()); 
    } 
 
    protected void setFeedResponseStatus(int status) { 
        this.feedResponse.setResponseCode(status); 
    } 
 
    protected void setError(int error) { 
        this.feedResponse.setError(error); 
    } 
 
    protected Service getService() throws ServletException { 
        ServiceFactory serviceFactory = ServiceFactory.getInstance(); 
        Service service = serviceFactory.getService(); 
        if(service == null) 
            throw new ServletException("Service not available");  
        return service; 
    } 
 
     
 
} 
