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
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.ServiceFactory;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;

/**
 * @author Simon Willnauer
 * 
 */
public class InsertFeedHandler extends AbstractFeedHandler {
    private static final Log LOG = LogFactory.getLog(InsertFeedHandler.class);

    /**
     * @see org.apache.lucene.gdata.servlet.handler.GDataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     */
    @SuppressWarnings("unused")
    public void processRequest(HttpServletRequest request,
            HttpServletResponse response) throws ServletException, IOException {
        super.processRequest(request, response);
        if (this.authenticated) {
            try {
                ServerBaseFeed feed = createFeedFromRequest(request);
                GDataAccount account = createRequestedAccount(request);

                GDataServerRegistry registry = GDataServerRegistry
                        .getRegistry();
                ServiceFactory serviceFactory = registry.lookup(
                        ServiceFactory.class, ComponentType.SERVICEFACTORY);
                if (serviceFactory == null) {
                    setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                            "required component is not available");
                    throw new FeedHandlerException(
                            "Can't save feed - ServiceFactory is null");
                }
                serviceFactory.getAdminService().createFeed(feed, account);
            } catch (ServiceException e) {
                setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR,
                        "can not create feed");
                LOG.error("Can not create feed -- " + e.getMessage(), e);
            } catch (Exception e) {
                LOG.error("Can not create feed -- " + e.getMessage(), e);

            }

        }
        sendResponse(response);
    }

}
