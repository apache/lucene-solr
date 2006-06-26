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

import com.google.gdata.data.BaseEntry;

/**
 * Default Handler implementation. This handler processes the incoming
 * {@link org.apache.lucene.gdata.server.GDataRequest} and updates the requested
 * feed entry in the storage and the search component.
 * <p>
 * The handler sends following response to the client:
 * </p>
 * <ol>
 * <li>if the entry was successfully updated - HTTP status code <i>200 OK</i></li>
 * <li>if an error occures - HTTP status code <i>500 INTERNAL SERVER ERROR</i></li>
 * <li>if the resource could not found - HTTP status code <i>404 NOT FOUND</i></li>
 * </ol>
 * 
 * @author Simon Willnauer
 *  
 */
public class DefaultUpdateHandler extends AbstractGdataRequestHandler {
	private static final Log LOG = LogFactory
			.getLog(DefaultUpdateHandler.class);

	/**
	 * @throws ServletException 
	 * @see org.apache.lucene.gdata.servlet.handler.AbstractGdataRequestHandler#processRequest(javax.servlet.http.HttpServletRequest,
	 *      javax.servlet.http.HttpServletResponse)
	 */
	@Override
	public void processRequest(HttpServletRequest request,
			HttpServletResponse response) throws  IOException, ServletException {
		try {
			initializeRequestHandler(request, response,GDataRequestType.UPDATE);
		} catch (GDataRequestException e) {
            setError(HttpServletResponse.SC_UNAUTHORIZED);
			sendError();
			return;
		}
        if(!authenticateAccount(request,AccountRole.ENTRYAMINISTRATOR)){
            sendError();
            return;
        }
        
		try {
			BaseEntry entry = this.service.updateEntry(this.feedRequest,
					this.feedResponse);
			setFeedResponseFormat();
			setFeedResponseStatus(HttpServletResponse.SC_OK);
			this.feedResponse.sendResponse(entry, this.feedRequest.getConfigurator().getExtensionProfile());
		
		}
		catch (ServiceException e) {
			LOG.error("Could not process UpdateFeed request - "
					+ e.getMessage(), e);
			sendError();
		}
        closeService();
	}

}
