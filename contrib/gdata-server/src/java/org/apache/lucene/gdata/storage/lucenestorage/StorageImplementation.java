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
 
package org.apache.lucene.gdata.storage.lucenestorage; 
 
import java.io.IOException; 
import java.util.List; 
 
import org.apache.commons.logging.Log; 
import org.apache.commons.logging.LogFactory; 
import org.apache.lucene.gdata.storage.Storage; 
import org.apache.lucene.gdata.storage.StorageException; 
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation; 
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.data.Feed; 
 
/** 
 * This is an implementation of the 
 * {@link org.apache.lucene.gdata.storage.Storage} interface. The 
 * StorageImplementation provides access to the 
 * {@link org.apache.lucene.gdata.storage.lucenestorage.StorageQuery} and the 
 * {@link org.apache.lucene.gdata.storage.lucenestorage.StorageModifier}. This 
 * class will be instanciated per client request. 
 *  
 *  
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class StorageImplementation implements Storage { 
    private final StorageCoreController controller; 
 
    private ExtensionProfile profile; 
 
    private static final Log LOG = LogFactory 
            .getLog(StorageImplementation.class); 
 
    /** 
     * Creates a new StorageImplementation 
     *  
     * @throws StorageException - 
     *             if the 
     *             {@link org.apache.lucene.gdata.storage.StorageController} can 
     *             not be created 
     * @throws IOException - 
     *             if the 
     *             {@link org.apache.lucene.gdata.storage.StorageController} can 
     *             not be created 
     * @see StorageCoreController#getStorageCoreController() 
     *  
     */ 
    public StorageImplementation() throws IOException, StorageException { 
        this.controller = StorageCoreController.getStorageCoreController(); 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#storeEntry(com.google.gdata.data.BaseEntry, 
     *      java.lang.String) 
     */ 
    public BaseEntry storeEntry(BaseEntry entry, String feedId) 
            throws StorageException { 
        if (this.profile == null) 
            throw new StorageException( 
                    "Can process ExtensionProfile not set -- is null"); 
        if (feedId == null) 
            throw new StorageException("No feed ID specified -- is null"); 
        StorageModifier modifier = this.controller.getStorageModifier(); 
        String id = this.controller.releaseID(); 
        entry.setId(feedId + id); 
        if (LOG.isInfoEnabled()) 
            LOG.info("Store entry " + id + " -- feed: " + feedId); 
 
        try { 
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry, 
                    feedId, StorageOperation.INSERT, this.profile); 
            modifier.insertEntry(wrapper); 
        } catch (IOException e) { 
            StorageException ex = new StorageException("Can't create Entry -- " 
                    + e.getMessage(), e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
 
        } 
 
        return entry; 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#deleteEntry(java.lang.String, 
     *      java.lang.String) 
     */ 
    public void deleteEntry(String entryId, String feedId) 
            throws StorageException { 
        if (this.profile == null) 
            throw new StorageException( 
                    "Can process ExtensionProfile not set -- is null"); 
        if (feedId == null) 
            throw new StorageException("No feed ID specified -- is null"); 
        if (entryId == null) 
            throw new StorageException("No entry ID specified -- is null"); 
        if (LOG.isInfoEnabled()) 
            LOG.info("delete entry " + entryId + " -- feed: " + feedId); 
        StorageModifier modifier = this.controller.getStorageModifier(); 
        modifier.deleteEntry(entryId, feedId); 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#updateEntry(com.google.gdata.data.BaseEntry, 
     *      java.lang.String) 
     */ 
    public BaseEntry updateEntry(BaseEntry entry, String feedId) 
            throws StorageException { 
        if (this.profile == null) 
            throw new StorageException( 
                    "Can process ExtensionProfile not set -- is null"); 
        if (feedId == null) 
            throw new StorageException("No feed ID specified -- is null"); 
        if (entry == null) 
            throw new StorageException("enrty is null"); 
        if (entry.getId() == null) 
            throw new StorageException("No entry ID specified -- is null"); 
        if (LOG.isInfoEnabled()) 
            LOG.info("update entry " + entry.getId() + " -- feed: " + feedId); 
        StorageModifier modifier = this.controller.getStorageModifier(); 
 
        try { 
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry, 
                    feedId, StorageOperation.UPDATE, this.profile); 
            modifier.updateEntry(wrapper); 
        } catch (IOException e) { 
            LOG.error("Can't update entry for feedID: " + feedId 
                    + "; entryId: " + entry.getId() + " -- " + e.getMessage(), 
                    e); 
            StorageException ex = new StorageException("Can't create Entry -- " 
                    + e.getMessage(), e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
 
        } 
 
        return entry; 
 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#getFeed(java.lang.String, 
     *      int, int) 
     */ 
    @SuppressWarnings("unchecked") 
    public BaseFeed getFeed(String feedId, int startIndex, int resultCount) 
            throws StorageException { 
        if (this.profile == null) 
            throw new StorageException( 
                    "Can process ExtensionProfile not set -- is null"); 
        if (feedId == null) 
            throw new StorageException("No feed ID specified -- is null"); 
        if (LOG.isInfoEnabled()) 
            LOG.info("get feed: " + feedId + " startindex: " + startIndex 
                    + " resultCount: " + resultCount); 
        ReferenceCounter<StorageQuery> query = null; 
        try { 
            query = this.controller.getStorageQuery(); 
            List<BaseEntry> resultList = query.get().getLatestFeedQuery(feedId, 
                    resultCount, startIndex, this.profile); 
            BaseFeed feed = new Feed(); 
            feed.getEntries().addAll(resultList); 
            return feed; 
        } catch (Exception e) { 
            LOG.error("Can't get latest feed for feedID: " + feedId + " -- " 
                    + e.getMessage(), e); 
            StorageException ex = new StorageException("Can't create Entry -- " 
                    + e.getMessage(), e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
 
        } finally { 
            if (query != null) 
                query.decrementRef(); 
        } 
 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#getEntry(java.lang.String, 
     *      java.lang.String) 
     */ 
    public BaseEntry getEntry(String entryId, String feedId) 
            throws StorageException { 
        if (this.profile == null) 
            throw new StorageException( 
                    "Can process ExtensionProfile not set -- is null"); 
        if (feedId == null) 
            throw new StorageException("No feed ID specified -- is null"); 
        if (entryId == null) 
            throw new StorageException("No entry ID specified -- is null"); 
        if (LOG.isInfoEnabled()) 
            LOG.info("get entry " + entryId + " -- feed: " + feedId); 
        ReferenceCounter<StorageQuery> query = null; 
        try { 
            query = this.controller.getStorageQuery(); 
            return query.get().singleEntryQuery(entryId, feedId, this.profile); 
        } catch (Exception e) { 
            LOG.error("Can't get entry for feedID: " + feedId + "; entryId: " 
                    + entryId + " -- " + e.getMessage(), e); 
            StorageException ex = new StorageException("Can't create Entry -- " 
                    + e.getMessage(), e); 
            ex.setStackTrace(e.getStackTrace()); 
            throw ex; 
 
        } finally { 
            if (query != null) 
                query.decrementRef(); 
        } 
 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#getEntries(java.util.List, 
     *      java.lang.String) 
     */ 
    public List<BaseEntry> getEntries(List<String> entryIdList, String feedId) 
            throws StorageException { 
        throw new StorageException("not implemented yet"); 
        // TODO implement this 
     
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#close() 
     */ 
    public void close() { 
        // 
    } 
 
    /** 
     * @see org.apache.lucene.gdata.storage.Storage#setExtensionProfile(com.google.gdata.data.ExtensionProfile) 
     */ 
    public void setExtensionProfile(ExtensionProfile profile) { 
        this.profile = profile; 
    } 
 
} 
