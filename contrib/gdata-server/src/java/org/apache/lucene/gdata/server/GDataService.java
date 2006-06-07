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
import java.util.List; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.gdata.server.registry.GDataServerRegistry; 
import org.apache.lucene.gdata.storage.Storage; 
import org.apache.lucene.gdata.storage.StorageException; 
import org.apache.lucene.gdata.storage.StorageFactory; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
import com.google.gdata.data.DateTime; 
import com.google.gdata.data.Generator; 
import com.google.gdata.data.Link; 
import com.google.gdata.util.ParseException; 
 
/** 
 * @author Simon Willnauer 
 *  
 */ 
public class GDataService extends Service { 
    private static final Log LOGGER = LogFactory.getLog(GDataService.class); 
 
    private Storage storage; 
 
    private GDataServerRegistry registry = GDataServerRegistry.getRegistry(); 
 
    private static final Generator generator; 
 
    private static final String generatorName = "Lucene GData-Server"; 
 
    private static final String generatorURI = "http://lucene.apache.org"; 
    static { 
        generator = new Generator(); 
        generator.setName(generatorName); 
        generator.setUri(generatorURI); 
        generator.setVersion("0.1"); 
    } 
 
    protected GDataService() throws ServiceException { 
        try { 
            this.storage = StorageFactory.getStorage(); 
             
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
    @Override 
    public BaseEntry createEntry(GDataRequest request, GDataResponse response) 
            throws ServiceException { 
 
        checkFeedIsRegisterd(request); 
        if (LOGGER.isInfoEnabled()) 
            LOGGER.info("create Entry for feedId: " + request.getFeedId()); 
        BaseEntry entry = buildEntry(request); 
        setUpdateTime(entry); 
        try { 
 
            this.storage.storeEntry(entry, request.getFeedId()); 
        } catch (Exception e) { 
            ServiceException ex = new ServiceException("Could not store entry", 
                    e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
        return entry; 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.server.Service#deleteEntry(org.apache.lucene.gdata.server.GDataRequest, 
     *      org.apache.lucene.gdata.server.GDataResponse) 
     */ 
    @Override 
    public BaseEntry deleteEntry(GDataRequest request, GDataResponse response) 
            throws ServiceException { 
        checkFeedIsRegisterd(request); 
        String entryid = request.getEntryId(); 
        String feedid = request.getFeedId(); 
        try { 
            this.storage.deleteEntry(entryid, feedid); 
        } catch (Exception e) { 
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
    @Override 
    public BaseEntry updateEntry(GDataRequest request, GDataResponse response) 
            throws ServiceException { 
        checkFeedIsRegisterd(request); 
 
        BaseEntry entry = buildEntry(request); 
        String feedid = request.getFeedId(); 
        if (LOGGER.isInfoEnabled()) 
            LOGGER.info("update Entry" + entry.getId() + " for feedId: " 
                    + feedid); 
        setUpdateTime(entry); 
        try { 
            this.storage.updateEntry(entry, feedid); 
        } catch (StorageException e) { 
            ServiceException ex = new ServiceException( 
                    "Could not update entry", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
        return entry; 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.server.Service#getFeed(org.apache.lucene.gdata.server.GDataRequest, 
     *      org.apache.lucene.gdata.server.GDataResponse) 
     */ 
    @SuppressWarnings("unchecked") 
    @Override 
    public BaseFeed getFeed(GDataRequest request, GDataResponse response) 
            throws ServiceException { 
        checkFeedIsRegisterd(request); 
 
        try { 
            // TODO remove when storing feeds is implemented just for 
            // development 
            BaseFeed feed = this.storage.getFeed(request.getFeedId(), request 
                    .getStartIndex(), request.getItemsPerPage()); 
            buildDynamicFeedElements(request, feed); 
            List<BaseEntry> list = feed.getEntries(); 
            addContextPath(list, request.getContextPath()); 
            return feed; 
        } catch (StorageException e) { 
            ServiceException ex = new ServiceException("Could not get feed", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
 
    } 
 
    /* 
     * build the dynamic elements like self link and next link 
     */ 
    private void buildDynamicFeedElements(final GDataRequest request, 
            final BaseFeed feed) { 
        feed.setGenerator(generator); 
        feed.setItemsPerPage(request.getItemsPerPage()); 
        feed.getLinks().add( 
                buildLink(Link.Rel.SELF, Link.Type.ATOM, request.getSelfId())); 
        // TODO add next link 
    } 
 
    private Link buildLink(String rel, String type, String href) { 
        Link retVal = new Link(); 
        retVal.setHref(href); 
        retVal.setRel(rel); 
        retVal.setType(type); 
        return retVal; 
    } 
 
    /* 
     * every entry has an ID which has to have a prefix. The prefix is the 
     * context path of the requested feed. This will be used to request the 
     * entry directly 
     */ 
    private void addContextPath(List<BaseEntry> list, final String contextPath) { 
        for (BaseEntry entry : list) { 
            addcontextPath(entry, contextPath); 
        } 
    } 
 
    @SuppressWarnings("unchecked") 
    private BaseEntry addcontextPath(final BaseEntry entry, 
            final String contextPath) { 
        String id = contextPath + entry.getId(); 
        entry.setId(id); 
        Link self = new Link(); 
        self.setRel("self"); 
        self.setHref(id); 
        self.setType("application/atom+xml"); 
        entry.getLinks().add(self); 
        return entry; 
    } 
 
    private BaseEntry buildEntry(final GDataRequest request) 
            throws ServiceException { 
        try { 
            return GDataEntityBuilder.buildEntry(request); 
 
        } catch (ParseException e) { 
            ServiceException ex = new ServiceException( 
                    "Could not parse entry from incoming request", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } catch (IOException e) { 
            ServiceException ex = new ServiceException( 
                    "Could not read or open input stream", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
    } 
 
    /* 
     * checks whether the reqeuested feed is registered 
     */ 
    private void checkFeedIsRegisterd(final GDataRequest request) 
            throws FeedNotFoundException { 
        if (!this.registry.isFeedRegistered(request.getFeedId())) 
            throw new FeedNotFoundException( 
                    "Feed could not be found - is not registed - Feed ID:" 
                            + request.getFeedId()); 
        this.storage.setExtensionProfile(request.getExtensionProfile()); 
    } 
 
    private BaseEntry setUpdateTime(final BaseEntry entry) { 
        entry.setUpdated(DateTime.now()); 
        return entry; 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.server.Service#getSingleEntry(org.apache.lucene.gdata.server.GDataRequest, 
     *      org.apache.lucene.gdata.server.GDataResponse) 
     */ 
    @Override 
    public BaseEntry getSingleEntry(GDataRequest request, GDataResponse response) 
            throws ServiceException { 
        checkFeedIsRegisterd(request); 
 
        try { 
            BaseEntry entry = this.storage.getEntry(request.getEntryId(), 
                    request.getFeedId()); 
            if(entry == null) 
                return null; 
            addcontextPath(entry, request.getContextPath()); 
            return entry; 
        } catch (StorageException e) { 
            ServiceException ex = new ServiceException("Could not get feed", e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
        } 
    } 
 
} 
