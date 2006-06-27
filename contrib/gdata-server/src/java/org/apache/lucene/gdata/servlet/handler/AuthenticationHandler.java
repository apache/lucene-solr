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
import java.io.Writer;

import javax.servlet.ServletException;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.administration.AdminService;
import org.apache.lucene.gdata.server.authentication.AuthenticationController;
import org.apache.lucene.gdata.server.authentication.AuthenticationException;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;



/**
 * @author Simon Willnauer
 *
 */
public class AuthenticationHandler implements GDataRequestHandler {
    private static final Log LOG = LogFactory.getLog(AuthenticationHandler.class);
    private final AuthenticationController controller;
    private final static String errorKey = "Error";
    private final static char seperatory = '=';
    private final ServiceFactory serviceFactory;
    private final GDataServerRegistry registry;
    /**
     * 
     */
    public AuthenticationHandler() {
        this.registry = GDataServerRegistry.getRegistry();
        this.controller = this.registry.lookup(AuthenticationController.class, ComponentType.AUTHENTICATIONCONTROLLER);
        this.serviceFactory = this.registry.lookup(ServiceFactory.class, ComponentType.SERVICEFACTORY);
    }

    /**
     * @see org.apache.lucene.gdata.servlet.handler.GDataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest, javax.servlet.http.HttpServletResponse)
     */
    @SuppressWarnings("unused")
    public void processRequest(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        
        try {
        String serviceName = request.getParameter(AuthenticationController.SERVICE_PARAMETER);
        if(LOG.isInfoEnabled()){
            String application = request.getParameter(AuthenticationController.APPLICATION_PARAMETER);
            LOG.info("Authentication request for service: "+serviceName+"; Application name: "+application);
        }
        
        if(!this.registry.isServiceRegistered(serviceName))
            throw new AuthenticationException("requested Service "+serviceName+"is not registered");
        String password = request.getParameter(AuthenticationController.PASSWORD_PARAMETER);
        String accountName = request.getParameter(AuthenticationController.ACCOUNT_PARAMETER);
        String clientIp = request.getRemoteHost();
        
       
        
        GDataAccount  account = getAccount(accountName);
        if(account == null || !account.getPassword().equals(password))
            throw new AuthenticationException("Account is null or password does not match");
        
        String token = this.controller.authenticatAccount(account,clientIp);
        sendToken(response,token);
        if(LOG.isInfoEnabled()){
            
            LOG.info("Account authenticated -- "+account);
        }
        } catch (AuthenticationException e){
            LOG.error("BadAuthentication -- "+e.getMessage(),e);
            sendError(response, HttpServletResponse.SC_FORBIDDEN,"BadAuthentication");
        }catch (Exception e) {
            LOG.error("Unexpected Exception -- SERVERERROR -- "+e.getMessage(),e);
            sendError(response,HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Service not available");
        }
    }
    
    
   private GDataAccount getAccount(String accountName) throws ServiceException{
       AdminService service = this.serviceFactory.getAdminService();
       return service.getAccount(accountName);
        
    }
   private void sendError(HttpServletResponse response, int code, String message)throws IOException{
       Writer writer = response.getWriter();
       writer.write(errorKey);
       writer.write(seperatory);
       writer.write(message);
       response.sendError(code);
   }
   
   private void sendToken(HttpServletResponse response, String token) throws IOException{
       Writer responseWriter = response.getWriter();
       Cookie cookie = new Cookie(AuthenticationController.TOKEN_KEY,token);
       response.addCookie(cookie);
       responseWriter.write(AuthenticationController.TOKEN_KEY);
       responseWriter.write(seperatory);
       responseWriter.write(token);
       responseWriter.close();
   }
}
