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
package org.apache.lucene.gdata.server;

import java.io.IOException;
import java.util.Date;
import java.util.List;

import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.storage.ResourceNotFoundException;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;
import com.google.gdata.data.Generator;
import com.google.gdata.data.Link;
import com.google.gdata.util.ParseException;

/**
 * default implementation of the {@link org.apache.lucene.gdata.server.Service}
 * interface.
 * 
 * @author Simon Willnauer
 * 
 */
public class GDataService implements Service {
    private static final Log LOGGER = LogFactory.getLog(GDataService.class);

    protected Storage storage;

    protected GDataServerRegistry registry = GDataServerRegistry.getRegistry();

    private static final Generator generator;

    private static final String generatorName = "Lucene GData-Server";

    private static final String generatorURI = "http://lucene.apache.org";

    private static final String XMLMIME = "application/atom+xml";
    static {
        generator = new Generator();
        generator.setName(generatorName);
        generator.setUri(generatorURI);
        generator.setVersion("0.1");
    }

    protected GDataService() throws ServiceException {
        try {
            StorageController controller = GDataServerRegistry.getRegistry()
                    .lookup(StorageController.class,
                            ComponentType.STORAGECONTROLLER);
            if (controller == null)
                throw new StorageException(
                        "StorageController is not registered");
            this.storage = controller.getStorage();

        } catch (StorageException e) {
            LOGGER
                    .fatal(
                            "Can't get Storage Instance -- can't serve any requests",
                            e);
            ServiceException ex = new ServiceException(
                    "Can't get Storage instance" + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#createEntry(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.server.GDataResponse)
     */

    public BaseEntry createEntry(GDataRequest request, GDataResponse response)
            throws ServiceException {

        if (LOGGER.isInfoEnabled())
            LOGGER.info("create Entry for feedId: " + request.getFeedId());

        ServerBaseEntry entry = buildEntry(request, response);
        entry.setFeedId(request.getFeedId());
        entry.setServiceConfig(request.getConfigurator());
        setTimeStamps(entry.getEntry());
        BaseEntry retVal = null;
        try {
            retVal = this.storage.storeEntry(entry);
        } catch (Exception e) {
            response.setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ServiceException ex = new ServiceException("Could not store entry",
                    e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
        return retVal;
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#deleteEntry(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.server.GDataResponse)
     */

    public BaseEntry deleteEntry(GDataRequest request, GDataResponse response)
            throws ServiceException {

        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setServiceConfig(request.getConfigurator());
        entry.setFeedId(request.getFeedId());
        entry.setId(request.getEntryId());
        if (entry.getId() == null)
            throw new ServiceException(
                    "entry id is null -- can not delete null entry");
        try {
            this.storage.deleteEntry(entry);
        } catch (ResourceNotFoundException e) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            ServiceException ex = new ServiceException(
                    "Could not delete entry", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        } catch (Exception e) {
            response.setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ServiceException ex = new ServiceException(
                    "Could not delete entry", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#updateEntry(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.server.GDataResponse)
     */

    public BaseEntry updateEntry(GDataRequest request, GDataResponse response)
            throws ServiceException {

        ServerBaseEntry entry = buildEntry(request, response);
        entry.setFeedId(request.getFeedId());

        entry.setServiceConfig(request.getConfigurator());
        if (LOGGER.isInfoEnabled())
            LOGGER.info("update Entry" + entry.getId() + " for feedId: "
                    + request.getFeedId());
        if (entry.getId() == null) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            throw new ServiceException("Entry id is null can not update entry");
        }
        if (!entry.getId().equals(request.getEntryId())) {
            if (LOGGER.isInfoEnabled())
                LOGGER
                        .info("Entry id in the entry xml does not match the requested resource -- XML-ID:"
                                + entry.getId()
                                + "; Requested resource: "
                                + request.getEntryId());
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            throw new ServiceException(
                    "Entry id in the entry xml does not match the requested resource");
        }
        setTimeStamps(entry.getEntry());
        BaseEntry retVal = null;
        try {
            retVal = this.storage.updateEntry(entry);
        } catch (ResourceNotFoundException e) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            ServiceException ex = new ServiceException(
                    "Could not update entry", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        } catch (StorageException e) {
            response.setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ServiceException ex = new ServiceException(
                    "Could not update entry", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
        return retVal;
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#getFeed(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.server.GDataResponse)
     */
    @SuppressWarnings("unchecked")
    public BaseFeed getFeed(GDataRequest request, GDataResponse response)
            throws ServiceException {

        ServerBaseFeed feed = new ServerBaseFeed();
        feed.setId(request.getFeedId());
        feed.setStartIndex(request.getStartIndex());
        feed.setItemsPerPage(request.getItemsPerPage());
        feed.setServiceConfig(request.getConfigurator());
        try {
            BaseFeed retVal = this.storage.getFeed(feed);
            dynamicElementFeedStragey(retVal, request);

            return retVal;
            /*
             * resouce not found will be detected in Gdata request.
             * the request queries the storage for the feed to get the serivce for the feed
             */
        } catch (StorageException e) {
            response.setError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
            ServiceException ex = new ServiceException("Could not get feed", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }

    }

    private Link buildLink(String rel, String type, String href) {
        Link retVal = new Link();
        retVal.setHref(href);
        retVal.setRel(rel);
        retVal.setType(type);
        return retVal;
    }

    private ServerBaseEntry buildEntry(final GDataRequest request,
            final GDataResponse response) throws ServiceException {
        try {
            ServerBaseEntry entry = new ServerBaseEntry(GDataEntityBuilder
                    .buildEntry(request));
            return entry;

        } catch (ParseException e) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            ServiceException ex = new ServiceException(
                    "Could not parse entry from incoming request", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        } catch (IOException e) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            ServiceException ex = new ServiceException(
                    "Could not read or open input stream", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
    }

    private BaseEntry setTimeStamps(final BaseEntry entry) {
        if (entry.getUpdated() == null)
            entry.setUpdated(DateTime.now());
        if (entry.getPublished() == null)
            entry.setPublished(DateTime.now());
        return entry;
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#getSingleEntry(org.apache.lucene.gdata.server.GDataRequest,
     *      org.apache.lucene.gdata.server.GDataResponse)
     */

    public BaseEntry getSingleEntry(GDataRequest request, GDataResponse response)
            throws ServiceException {

        try {
            ServerBaseEntry entry = new ServerBaseEntry();
            entry.setServiceConfig(request.getConfigurator());
            entry.setFeedId(request.getFeedId());
            entry.setId(request.getEntryId());
            if(entry.getId() == null){
                response.setError(HttpServletResponse.SC_BAD_REQUEST);
                throw new ServiceException("entry is null can't get entry");
            }
                
            BaseEntry retVal = null;
            retVal = this.storage.getEntry(entry);
            dynamicElementEntryStragey(retVal, request);
            return retVal;
        } catch (ResourceNotFoundException e) {
            response.setError(HttpServletResponse.SC_BAD_REQUEST);
            ServiceException ex = new ServiceException(
                    "Could not get entry", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        } catch (StorageException e) {
            ServiceException ex = new ServiceException("Could not get feed", e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }
    }

    /*
     * adds all dynamic element to the entry
     */
    private void dynamicElementEntryStragey(final BaseEntry entry,
            final GDataRequest request) {
        setSelfLink(entry, request.getContextPath());
    }

    /*
     * adds all dynamic element to the feed entries
     */
    @SuppressWarnings("unchecked")
    private void dynamicElementFeedStragey(final BaseFeed feed,
            final GDataRequest request) {
        buildDynamicFeedElements(request, feed);
        List<BaseEntry> entryList = feed.getEntries();
        for (BaseEntry entry : entryList) {
            String id = request.getContextPath() + entry.getId();
            setSelfLink(entry, id);
        }

    }

    /*
     * The selfLink is build from a prefix and the entry id. The prefix is the
     * context path of the requested feed. This will be used to request the
     * entry directly
     */@SuppressWarnings("unchecked")
    private BaseEntry setSelfLink(final BaseEntry entry, String id) {
        Link self = buildLink(Link.Rel.SELF, XMLMIME, id);
        entry.getLinks().add(self);
        return entry;
    }

    /*
     * build the dynamic elements like self link and next link
     */
    private void buildDynamicFeedElements(final GDataRequest request,
            final BaseFeed feed) {
        feed.setGenerator(generator);
        feed.setItemsPerPage(request.getItemsPerPage());
        feed.setStartIndex(request.getStartIndex());
        feed.setId(request.getContextPath());
        feed.getLinks().add(
                buildLink(Link.Rel.SELF, Link.Type.ATOM, request.getSelfId()));
        feed.getLinks().add(
                buildLink(Link.Rel.NEXT, XMLMIME, request.getNextId()));

    }

    /**
     * @see org.apache.lucene.gdata.server.Service#close()
     */
    public void close() {
        this.storage.close();
    }

    /**
     * @see org.apache.lucene.gdata.server.Service#getFeedLastModified(java.lang.String)
     */
    public Date getFeedLastModified(final String feedId) throws ServiceException {
        try {
            return new Date(this.storage.getFeedLastModified(feedId));
           
        } catch (StorageException e) {
            ServiceException ex = new ServiceException(
                    "Could not get Last update for feed -- "+feedId, e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;
        }

    }

    /**
     * @see org.apache.lucene.gdata.server.Service#getEntryLastModified(java.lang.String, java.lang.String)
     */
    public Date getEntryLastModified(final String entryId,final String feedId) throws ServiceException {
            try {
                return new Date(this.storage.getEntryLastModified(entryId, feedId));
                
               
                
            } catch (StorageException e) {
                ServiceException ex = new ServiceException(
                        "Could not get Last update for entry  -- "+entryId, e);
                ex.setStackTrace(e.getStackTrace());
                throw ex;
            }
        
    }

}
