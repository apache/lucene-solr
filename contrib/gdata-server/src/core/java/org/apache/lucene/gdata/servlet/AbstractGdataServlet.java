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
 
package org.apache.lucene.gdata.servlet; 
 
import java.io.IOException; 
 
import javax.servlet.ServletConfig;
import javax.servlet.ServletException; 
import javax.servlet.http.HttpServlet; 
import javax.servlet.http.HttpServletRequest; 
import javax.servlet.http.HttpServletResponse; 

import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.servlet.handler.RequestHandlerFactory;
 
/** 
 *  
 * Provides an abstract class to be subclassed to create an GDATA servlet 
 * suitable for a GDATA serverside implementation. 
 *  
 * @see javax.servlet.http.HttpServlet 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public abstract class AbstractGdataServlet extends HttpServlet { 
    private static final String METHOD_HEADER_NAME = "x-http-method-override"; 
 
    private static final String METHOD_DELETE = "DELETE"; 
 
    private static final String METHOD_GET = "GET"; 
 
    private static final String METHOD_POST = "POST"; 
 
    private static final String METHOD_PUT = "PUT";

    protected static RequestHandlerFactory HANDLER_FACTORY = null; 
 
    /** 
     * This overwrites the protected <code>service</code> method to dispatch 
     * the request to the correponding <code>do</code> method. There is 
     * ususaly no need for overwriting this method. The GData protool and the 
     * Google GData API uses the <code>x-http-method-override</code> header to 
     * get through firewalls. The http method will be overritten by the 
     * <code>x-http-method-override</code> and dispatched to the 
     * <code>do</code><i>Xxx</i> methods defined in this class. This method 
     * is an GDATA-specific version of the {@link javax.servlet.Servlet#service} 
     * method. 
     *  
     * @see HttpServlet#service(javax.servlet.http.HttpServletRequest, 
     *      javax.servlet.http.HttpServletResponse) 
     */ 
    @Override 
    protected void service(HttpServletRequest arg0, HttpServletResponse arg1) 
            throws ServletException, IOException { 
        if (arg0.getHeader(METHOD_HEADER_NAME) == null) { 
            super.service(arg0, arg1); 
            return; 
        } 
        overrideMethod(arg0, arg1); 
 
    } 
 
    private void overrideMethod(HttpServletRequest arg0, 
            HttpServletResponse arg1) throws ServletException, IOException { 
        final String method = arg0.getMethod(); 
        final String overrideHeaderMethod = arg0.getHeader(METHOD_HEADER_NAME); 
        if (overrideHeaderMethod.equals(method)) { 
            super.service(arg0, arg1); 
            return; 
        } 
        // These methodes are use by GDATA Client APIs 
        if (overrideHeaderMethod.equals(METHOD_DELETE)) { 
            doDelete(arg0, arg1); 
        } else if (overrideHeaderMethod.equals(METHOD_GET)) { 
            doGet(arg0, arg1); 
        } else if (overrideHeaderMethod.equals(METHOD_POST)) { 
            doPost(arg0, arg1); 
        } else if (overrideHeaderMethod.equals(METHOD_PUT)) { 
            doPut(arg0, arg1); 
        } else { 
            // if another method has been overwritten follow the HttpServlet 
            // implementation 
            super.service(arg0, arg1); 
        } 
 
    }

    /**
     * 
     * @see javax.servlet.GenericServlet#init(javax.servlet.ServletConfig)
     */
    public void init(ServletConfig arg0) throws ServletException {
        HANDLER_FACTORY = GDataServerRegistry.getRegistry().lookup(RequestHandlerFactory.class,ComponentType.REQUESTHANDLERFACTORY);
        if(HANDLER_FACTORY == null)
            throw new ServletException("service not available");
        
    } 
 
} 
