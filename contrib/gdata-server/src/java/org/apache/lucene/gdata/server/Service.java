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
 
import java.util.Date;

import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
 
 
/** 
 * The Service class represents an interface to access the GData service 
 * componentes of the GData-Server. It encapsulates all interactions with the 
 * GData client. 
 * <p> 
 * This class provides the base level common functionality required to access 
 * the GData components. It is also designed to act as a base class that can be 
 * extended for specific types of underlaying server components as different 
 * indexing or storage components. 
 * </p> 
 * <p> 
 * It could also encapsulate caching mechanismn build on top of the storage to 
 * reduce load on the storage component 
 * </p> 
 *  
 * @author Simon Willnauer 
 *  
 *  
 */ 
public interface Service { 
 
    /** 
     * Service method to create an entry in an already created and existing 
     * feed. This method will create the entry and passes the entry to the 
     * indexing component to make the new entry accessable via <i>get-queries</i>. 
     * The response and the corresponding http status code will be added to the 
     * given <code>FeedResponse</code>. 
     *  
     * @param request - 
     *            the current FeedRequest 
     * @param response - 
     *            the current FeedResponse 
     * @return - the entry which has been created  
     * @throws ServiceException - 
     *             if the corresponding feed does not exist or the storage can 
     *             not be accessed 
     */ 
    public abstract BaseEntry createEntry(final GDataRequest request, 
            final GDataResponse response) throws ServiceException; 
 
    /** 
     * Service Method to delete an entry specified in the given FeedRequest. 
     * This method will remove the entry permanently. There will be no 
     * possiblity to restore the entry. The response and the corresponding http 
     * status code will be added to the given <code>FeedResponse</code>. 
     *  
     * @param request - 
     *            the current FeedRequest 
     * @param response - 
     *            the current FeedResponse 
     * @return - the entry wich has been deleted 
     * @throws ServiceException - 
     *             if the entry does not exist or the storage can not be 
     *             accessed 
     */ 
    public abstract BaseEntry deleteEntry(GDataRequest request, final GDataResponse response) 
            throws ServiceException; 
 
    /** 
     * Service method to update an existing entry in a existing feed context. 
     * The entry version will be checked and a <code>ServiceException</code> 
     * will be thrown if the version to update is outdated. The new entry will 
     * be passed to the indexing component to make the version accessable via 
     * <i>get-queries</i>. 
     *  
     * @param request - 
     *            the current FeedRequest 
     * @param response - 
     *            the current FeedResponse 
     * @return - the entry wich has been updated 
     * @throws ServiceException - 
     *             if the corresponding feed does not exist, the storage can not 
     *             be accessed or the version to update is out of date. 
     */ 
    public abstract BaseEntry  updateEntry(final GDataRequest request, 
            final GDataResponse response) throws ServiceException; 
 
    /** 
     * Service method to retrieve a requested Feed. The feed will also be added to 
     * the given <code>FeedResponse</code> instance and can also be accessed 
     * via the <code>FeedResponse</code> object. 
     *  
     * @param request - 
     *            the current FeedRequest 
     * @param response - 
     *            the current FeedResponse 
     * @return - the requested feed 
     *  
     * @throws ServiceException - 
     *             If the storage can not be accessed or the requested feed does 
     *             not exist. 
     */ 
    public abstract BaseFeed getFeed(final GDataRequest request, final GDataResponse response) 
            throws ServiceException; 
 
    /** 
     * Service method to retrieve a requested entry. The entry will also be added to 
     * the given <code>FeedResponse</code> instance and can also be accessed 
     * via the <code>FeedResponse</code> object. 
     *  
     * @param request - 
     *            the current FeedRequest 
     * @param response - 
     *            the current FeedResponse 
     * @return - the requested entry 
     *  
     * @throws ServiceException - 
     *             If the storage can not be accessed or the requested entry does 
     *             not exist. 
     */ 
    public abstract BaseEntry getSingleEntry(final GDataRequest request, final GDataResponse response) 
            throws ServiceException; 
     
    /**
     * will close the Service - service should not be used after this method has been called
     */
    public void close();

    /**
     * Retruns the date of the last modification for the given feed id
     * @param feedId - the id of the feed 
     * @return - the last modified date or the current date if the date can not be retrieved
     * @throws ServiceException - if the storage can not be accessed
     */
    public abstract Date getFeedLastModified(String feedId)throws ServiceException;

    /**
     * Retruns the date of the last modification for the given entry id
     * @param entryId - the id of the entry
     * @param feedId  - the feed id this entry belongs to
     * @return - the last modified date or the current date if the date can not be retrieved
     * @throws ServiceException - if the storage can not be accessed 
     */
    public abstract Date getEntryLastModified(String entryId, String feedId)throws ServiceException;
     
 
 
} 
