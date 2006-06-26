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

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.servlet.handler.GDataRequestHandler;

/**
 * Provides a clean basic interface for GDATA Client API and requests to the
 * GDATA Server. This Servlet dispatches the incoming requests to defined GDATA
 * request handlers. Each of the handler processes the incoming request and
 * responds according to the requested action.
 * 
 * @author Simon Willnauer
 * 
 */
public class RequestControllerServlet extends AbstractGdataServlet {
    private static final Log LOGGER = LogFactory.getLog(RequestControllerServlet.class);

    /**
     * Version ID since this class implements
     * 
     * @see java.io.Serializable
     */
    private static final long serialVersionUID = 7540810742476175576L;

    /**
     * @see javax.servlet.http.HttpServlet#doDelete(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @Override
	protected void doDelete(HttpServletRequest arg0, HttpServletResponse arg1)
            throws ServletException, IOException {
        GDataRequestHandler hanlder = HANDLER_FACTORY.getEntryDeleteHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process DELETE request");
        
        hanlder.processRequest(arg0, arg1);
    }

    /**
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @Override
	protected void doGet(HttpServletRequest arg0, HttpServletResponse arg1)
            throws ServletException, IOException {
        GDataRequestHandler hanlder = HANDLER_FACTORY.getFeedQueryHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process GET request");
        hanlder.processRequest(arg0, arg1);
    }

    /**
     * @see javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @Override
	protected void doPost(HttpServletRequest arg0, HttpServletResponse arg1)
            throws ServletException, IOException {
        GDataRequestHandler hanlder = HANDLER_FACTORY.getEntryInsertHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process POST request");
        hanlder.processRequest(arg0, arg1);
    }

    /**
     * @see javax.servlet.http.HttpServlet#doPut(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @Override
	protected void doPut(HttpServletRequest arg0, HttpServletResponse arg1)
            throws ServletException, IOException {
        GDataRequestHandler hanlder = HANDLER_FACTORY.getEntryUpdateHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process PUT request");
        hanlder.processRequest(arg0, arg1);
    }
    
  
}
