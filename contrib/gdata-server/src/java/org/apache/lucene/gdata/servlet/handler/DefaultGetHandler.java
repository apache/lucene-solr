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
import org.apache.lucene.gdata.server.GDataRequestException; 
import org.apache.lucene.gdata.server.Service; 
import org.apache.lucene.gdata.server.ServiceException; 
import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
 
/** 
 * Default Handler implementation. This handler processes the incoming 
 * {@link org.apache.lucene.gdata.server.GDataRequest} and retrieves the 
 * requested feed from the underlaying storage. 
 * <p> 
 * This hander also processes search queries and retrives the search hits from 
 * the underlaying search component. The user query will be accessed via the 
 * {@link org.apache.lucene.gdata.server.GDataRequest} instance passed to the 
 * {@link Service} class. 
 * </p> 
 *  
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class DefaultGetHandler extends AbstractGdataRequestHandler { 
    private static final Log LOG = LogFactory.getLog(DefaultGetHandler.class); 
 
    /** 
     * @see org.apache.lucene.gdata.servlet.handler.AbstractGdataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest, 
     *      javax.servlet.http.HttpServletResponse) 
     */ 
    @Override 
    public void processRequest(HttpServletRequest request, 
            HttpServletResponse response) throws IOException, ServletException { 
        try { 
            initializeRequestHandler(request, response, GDataRequestType.GET); 
        } catch (GDataRequestException e) { 
            sendError(); 
            return; 
        } 
        Service service = getService(); 
        try { 
            if (LOG.isInfoEnabled()) 
                LOG.info("Requested output formate: " 
                        + this.feedRequest.getRequestedResponseFormat()); 
            this.feedResponse.setOutputFormat(this.feedRequest 
                    .getRequestedResponseFormat()); 
            if(this.feedRequest.isFeedRequested()){ 
            BaseFeed feed = service 
                    .getFeed(this.feedRequest, this.feedResponse); 
         
            this.feedResponse.sendResponse(feed, this.feedRequest.getExtensionProfile()); 
            }else{ 
             BaseEntry entry = service.getSingleEntry(this.feedRequest,this.feedResponse); 
             if(entry == null){ 
                 this.feedResponse.setError(HttpServletResponse.SC_NOT_FOUND); 
                 sendError(); 
             } 
             this.feedResponse.sendResponse(entry, this.feedRequest.getExtensionProfile()); 
            } 
             
             
        } catch (ServiceException e) { // TODO handle exceptions to send exact 
            // response 
            LOG.error("Could not process GetFeed request - " + e.getMessage(), 
                    e); 
            this.feedResponse.setError(HttpServletResponse.SC_BAD_REQUEST); // TODO 
            // change 
            // this 
            sendError(); 
        } 
         
         
 
    } 
     
     
 
} 
