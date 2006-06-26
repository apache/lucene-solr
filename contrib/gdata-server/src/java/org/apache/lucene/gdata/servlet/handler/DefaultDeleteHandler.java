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
import org.apache.lucene.gdata.data.GDataAccount.AccountRole;
import org.apache.lucene.gdata.server.GDataRequestException;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType;

/**
 * Default Handler implementation. This handler processes the incoming
 * {@link org.apache.lucene.gdata.server.GDataRequest} and deletes the requested
 * feed entry from the storage and the search component.
 * <p>
 * The handler sends following response to the client:
 * </p>
 * <ol>
 * <li>if the entry could be deleted - HTTP status code <i>200 OK</i></li>
 * <li>if an error occures - HTTP status code <i>500 INTERNAL SERVER ERROR</i></li>
 * <li>if the resource could not found - HTTP status code <i>404 NOT FOUND</i></li>
 * </ol>
 *  
 * @author Simon Willnauer
 * 
 */
public class DefaultDeleteHandler extends AbstractGdataRequestHandler {
	private static final Log LOG = LogFactory
			.getLog(DefaultDeleteHandler.class);

	/**
	 * @throws ServletException 
	 * @see org.apache.lucene.gdata.servlet.handler.AbstractGdataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws IOException, ServletException {
		try {
			initializeRequestHandler(request, response,GDataRequestType.DELETE);
		} catch (GDataRequestException e) {
			sendError();
			return;
		}
        if(!authenticateAccount(request,AccountRole.ENTRYAMINISTRATOR)){
            setError(HttpServletResponse.SC_UNAUTHORIZED);
            sendError();
            return;
        }
		
		try {
			this.service.deleteEntry(this.feedRequest, this.feedResponse);
		
		} catch (ServiceException e) {
			LOG.error("Could not process DeleteFeed request - "
					+ e.getMessage(), e);
			sendError();
		}
        closeService();

	}

}
