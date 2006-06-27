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
 * This Servlet provides an REST interface to create / update and delete Feed instances.
 *  
 * @author Simon Willnauer
 *
 */
public class FeedAdministrationServlet extends AbstractGdataServlet {
    private static final Log LOGGER = LogFactory.getLog(FeedAdministrationServlet.class);
    /**
     * 
     */
    private static final long serialVersionUID = -905586350743277032L;

    @Override
    protected void doDelete(HttpServletRequest arg0, HttpServletResponse arg1) throws ServletException, IOException {
        GDataRequestHandler handler = HANDLER_FACTORY.getDeleteFeedHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process delete feed request");
        handler.processRequest(arg0,arg1);
     
    }

    @Override
    protected void doPost(HttpServletRequest arg0, HttpServletResponse arg1) throws ServletException, IOException {
        GDataRequestHandler handler = HANDLER_FACTORY.getInsertFeedHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process insert feed request");
        handler.processRequest(arg0,arg1);
     
    }

    @Override
    protected void doPut(HttpServletRequest arg0, HttpServletResponse arg1) throws ServletException, IOException {
        GDataRequestHandler handler = HANDLER_FACTORY.getUpdateFeedHandler();
        if(LOGGER.isInfoEnabled())
            LOGGER.info("Process update feed request");
        handler.processRequest(arg0,arg1);
     
    }

  

}
