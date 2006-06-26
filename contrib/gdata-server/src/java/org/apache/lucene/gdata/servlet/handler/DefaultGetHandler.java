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
import java.util.Date;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.server.GDataRequestException;
import org.apache.lucene.gdata.server.Service;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.server.GDataRequest.GDataRequestType;
import org.apache.lucene.gdata.utils.DateFormater;

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
 * <p>
 * The DefaultGetHandler supports HTTP Conditional GET. It set the Last-Modified
 * response header based upon the value of the <atom:updated> element in the
 * returned feed or entry. A client can send this value back as the value of the
 * If-Modified-Since request header to avoid retrieving the content again if it
 * hasn't changed. If the content hasn't changed since the If-Modified-Since
 * time, then the GData service returns a 304 (Not Modified) HTTP response.</p>
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

        try {
            String modifiedSince = this.feedRequest.getModifiedSince();
            if (!checkIsModified(modifiedSince)) {
                this.feedResponse
                        .setStatus(HttpServletResponse.SC_NOT_MODIFIED);
                return;
            }
            if (LOG.isInfoEnabled())
                LOG.info("Requested output formate: "
                        + this.feedRequest.getRequestedResponseFormat());
            this.feedResponse.setOutputFormat(this.feedRequest
                    .getRequestedResponseFormat());
            if (this.feedRequest.isFeedRequested()) {
                BaseFeed feed = this.service.getFeed(this.feedRequest,
                        this.feedResponse);

                this.feedResponse.sendResponse(feed, this.feedRequest
                        .getConfigurator().getExtensionProfile());
            } else {
                BaseEntry entry = this.service.getSingleEntry(this.feedRequest,
                        this.feedResponse);
                this.feedResponse.sendResponse(entry, this.feedRequest
                        .getConfigurator().getExtensionProfile());
            }

        } catch (ServiceException e) {
            LOG.error("Could not process GetFeed request - " + e.getMessage(),
                    e);
            sendError();
        }
        closeService();

    }

    /**
     * 
     * returns true if the resource has been modified since the specified
     * reqeust header value
     */
    private boolean checkIsModified(String lastModified)
            throws ServiceException {
        if (lastModified == null)
            return true;
        try {
            Date clientDate = DateFormater.parseDate(lastModified,DateFormater.HTTP_HEADER_DATE_FORMAT,DateFormater.HTTP_HEADER_DATE_FORMAT_TIME_OFFSET);
            Date entityDate;
            if (this.feedRequest.isFeedRequested())
                entityDate = this.service.getFeedLastModified(this.feedRequest
                        .getFeedId());
            else
                entityDate = this.service.getEntryLastModified(this.feedRequest
                        .getEntryId(),this.feedRequest.getFeedId());
            if(LOG.isInfoEnabled())
                LOG.info("comparing date clientDate: "+clientDate+"; lastmodified: "+entityDate);
            return (entityDate.getTime()-clientDate.getTime() > 1000);
        } catch (java.text.ParseException e) {
            LOG.info("Couldn't parse Last-Modified header -- "+lastModified,e);

        }
        return true;
    }

}
