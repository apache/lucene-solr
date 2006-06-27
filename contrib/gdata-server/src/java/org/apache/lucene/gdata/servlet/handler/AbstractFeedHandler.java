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
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.GDataEntityBuilder;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.server.registry.ProvidedService;

import com.google.gdata.util.ParseException;

/**
 * 
 * @author Simon Willnauer
 *
 */
public abstract class AbstractFeedHandler extends RequestAuthenticator implements GDataRequestHandler {
    private static final Log LOG = LogFactory.getLog(AbstractFeedHandler.class);

    protected static final String PARAMETER_ACCOUNT = "account";

    protected static final String PARAMETER_SERVICE = "service";
    private int error;
    protected boolean authenticated = false;
    
      private String errorMessage = "";
      private boolean isError = false;
      
    /**
     * @see org.apache.lucene.gdata.servlet.handler.GDataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @SuppressWarnings("unused")
    public void processRequest(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
            this.authenticated = authenticateAccount(request,AccountRole.FEEDAMINISTRATOR);
            if(!this.authenticated)
                setError(HttpServletResponse.SC_UNAUTHORIZED,"Authorization failed");
        
    }
    
    protected ServerBaseFeed createFeedFromRequest(HttpServletRequest request) throws ParseException, IOException, FeedHandlerException{
        GDataServerRegistry registry = GDataServerRegistry.getRegistry();
        String providedService = request.getParameter(PARAMETER_SERVICE);
        if(!registry.isServiceRegistered(providedService)){
            setError(HttpServletResponse.SC_NOT_FOUND,"no such service");
            throw new FeedHandlerException("ProvicdedService is not registered -- Name: "+providedService);
         }
        ProvidedService provServiceInstance = registry.getProvidedService(providedService);  
        if(providedService == null){
            setError(HttpServletResponse.SC_BAD_REQUEST,"no such service");
            throw new FeedHandlerException("no such service registered -- "+providedService);
        }
        try{
            ServerBaseFeed retVal = new ServerBaseFeed(GDataEntityBuilder.buildFeed(request.getReader(),provServiceInstance));
            retVal.setServiceConfig(provServiceInstance);
        return retVal;
        }catch (IOException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not read from input stream - ",e);
            setError(HttpServletResponse.SC_BAD_REQUEST,"Can not read from input stream");
            throw e;
        }catch (ParseException e) {
            if(LOG.isInfoEnabled())
                LOG.info("feed can not be parsed - ",e);
            setError(HttpServletResponse.SC_BAD_REQUEST,"incoming feed can not be parsed");
            throw e;
        }
        
    }
    
    
    protected GDataAccount createRequestedAccount(HttpServletRequest request) throws FeedHandlerException{
        GDataServerRegistry registry = GDataServerRegistry.getRegistry();
           ServiceFactory serviceFactory = registry.lookup(ServiceFactory.class,ComponentType.SERVICEFACTORY);
        
        if(serviceFactory == null){
            setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Required server component not available");
            throw new FeedHandlerException("Required server component not available -- "+ServiceFactory.class.getName());
        }
        AdminService service = serviceFactory.getAdminService();
        String account = request.getParameter(PARAMETER_ACCOUNT);
        try{
        return service.getAccount(account);
        }catch (ServiceException e) {
            if(LOG.isInfoEnabled())
                LOG.info("no account for requested account - "+account,e);
            setError(HttpServletResponse.SC_BAD_REQUEST,"no such account");
            throw new FeedHandlerException(e.getMessage(),e);
        }
    }
    
    protected void sendResponse(HttpServletResponse response){
        
        if(!this.isError)
            return;
        try{
        response.sendError(this.error,this.errorMessage);
        }catch (IOException e) {
            LOG.warn("can send error in RequestHandler ",e);
        }
    }
    
    protected void setError(int error, String message){
        this.error = error;
        this.errorMessage = message;
        this.isError = true;
    }
    protected int getErrorCode(){
        return this.error;
    }
    
    protected String getErrorMessage(){
        return this.errorMessage;
    }
    
    class FeedHandlerException extends Exception{

        /**
         * 
         */
        private static final long serialVersionUID = 1L;

        /**
         * Creates a new FeedHandlerException with a exception message and the exception cause this ex.
         * @param arg0 - the message
         * @param arg1 - the cause
         */
        public FeedHandlerException(String arg0, Throwable arg1) {
            super(arg0, arg1);
       
        }

        /**
         * Creates a new FeedHandlerException with a exception message.
         * @param arg0 - message
         */
        public FeedHandlerException(String arg0) {
            super(arg0 );
            
        }
        
    }
    

}
