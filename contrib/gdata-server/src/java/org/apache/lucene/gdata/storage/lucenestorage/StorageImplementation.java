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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.storage.ResourceNotFoundException;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper.StorageOperation;
import org.apache.lucene.gdata.storage.lucenestorage.util.ReferenceCounter;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;

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

    private static final Log LOG = LogFactory
            .getLog(StorageImplementation.class);

    /**
     * Creates a new StorageImplementation
     * 
     * @throws StorageException -
     *             if the storage controller can not be obtained
     * 
     * 
     * 
     */
    public StorageImplementation() throws StorageException {
        this.controller = (StorageCoreController) GDataServerRegistry
                .getRegistry().lookup(StorageController.class,
                        ComponentType.STORAGECONTROLLER);
        if (this.controller == null)
            throw new StorageException("Can't get registered StorageController");
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry storeEntry(final ServerBaseEntry entry)
            throws StorageException {

        if (entry == null)
            throw new StorageException("entry is null");
        StorageModifier modifier = this.controller.getStorageModifier();
        String id = this.controller.releaseID();
        entry.setId(entry.getFeedId() + id);
        if (LOG.isInfoEnabled())
            LOG.info("Store entry " + id + " -- feed: " + entry.getFeedId());

        try {
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,
                    StorageOperation.INSERT);
            modifier.insertEntry(wrapper);
        } catch (IOException e) {
            StorageException ex = new StorageException("Can't create Entry -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        }

        return entry.getEntry();
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void deleteEntry(final ServerBaseEntry entry)
            throws StorageException {

        if (entry == null)
            throw new StorageException("Entry is null");

        if (LOG.isInfoEnabled())
            LOG.info("delete entry " + entry.getId() + " -- feed: "
                    + entry.getFeedId());
        StorageModifier modifier = this.controller.getStorageModifier();
        ReferenceCounter<StorageQuery> query = this.controller.getStorageQuery();
        try{
        if(query.get().isEntryStored(entry.getId(),entry.getFeedId())){
            
            modifier.deleteEntry(new StorageEntryWrapper(entry,StorageOperation.DELETE));
        }
        else
            throw new ResourceNotFoundException("Entry for entry id: "+entry.getId()+" is not stored");
        }catch (IOException e) {
            throw new StorageException("Can not access storage");
        }finally{
            query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry updateEntry(ServerBaseEntry entry) throws StorageException {

        if (entry == null)
            throw new StorageException("entry is null");
        if(entry.getId() == null)
            throw new StorageException("entry id is null");
        if(entry.getFeedId() == null)
            throw new StorageException("feed id is null");
        if (LOG.isInfoEnabled())
            LOG.info("update entry " + entry.getId() + " -- feed: "
                    + entry.getFeedId());
        StorageModifier modifier = this.controller.getStorageModifier();
        ReferenceCounter<StorageQuery> query = this.controller.getStorageQuery();
        try {
            StorageEntryWrapper wrapper = new StorageEntryWrapper(entry,
                    StorageOperation.UPDATE);
            if(query.get().isEntryStored(entry.getId(),entry.getFeedId()))
                modifier.updateEntry(wrapper);
            else
                throw new ResourceNotFoundException("Entry for entry id: "+entry.getId()+" is not stored");
            
        } catch (IOException e) {
            LOG.error("Can't update entry for feedID: " + entry.getFeedId()
                    + "; entryId: " + entry.getId() + " -- " + e.getMessage(),
                    e);
            StorageException ex = new StorageException("Can't create Entry -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        }

        return entry.getEntry();

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getFeed(org.apache.lucene.gdata.data.ServerBaseFeed)
     */
    @SuppressWarnings("unchecked")
    public BaseFeed getFeed(final ServerBaseFeed feed) throws StorageException {

        if (feed == null)
            throw new StorageException("feed is null");
        if (LOG.isInfoEnabled())
            LOG.info("get feed: " + feed.getId() + " startindex: "
                    + feed.getStartIndex() + " resultCount: "
                    + feed.getItemsPerPage());
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            BaseFeed retVal = query.get().getLatestFeedQuery(feed.getId(),
                    feed.getItemsPerPage(), feed.getStartIndex(),
                    feed.getServiceConfig());
            return retVal;
        } catch (Exception e) {
            LOG.error("Can't get latest feed for feedID: " + feed.getId()
                    + " -- " + e.getMessage(), e);
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
     * @see org.apache.lucene.gdata.storage.Storage#getEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry getEntry(final ServerBaseEntry entry)
            throws StorageException {

        if (entry == null)
            throw new StorageException("No entry  specified -- is null");
        if (LOG.isInfoEnabled())
            LOG.info("get entry " + entry.getId() + " -- feed: "
                    + entry.getFeedId());
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            BaseEntry retVal = query.get().singleEntryQuery(entry.getId(),
                    entry.getFeedId(), entry.getServiceConfig());
            if(retVal == null)
                throw new ResourceNotFoundException("can not get entry for entry ID "+entry.getId());
            return retVal;
        } catch (Exception e) {
            LOG.error("Can't get entry for feedID: " + entry.getFeedId()
                    + "; entryId: " + entry.getId() + " -- " + e.getMessage(),
                    e);
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
     * @see org.apache.lucene.gdata.storage.Storage#close()
     */
    public void close() {
        //
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void storeAccount(GDataAccount Account) throws StorageException {
        if (Account == null)
            throw new StorageException("Can not save null Account");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (query.get().getUser(Account.getName()) != null)
                throw new StorageException("Account already exists");
            StorageModifier modifier = this.controller.getStorageModifier();
            StorageAccountWrapper wrapper = new StorageAccountWrapper(Account);
            modifier.createAccount(wrapper);
        } catch (Exception e) {
            LOG.error("Can't save Account -- " + e.getMessage(), e);
            StorageException ex = new StorageException("Can't save Account -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query != null)
                query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void updateAccount(GDataAccount Account) throws StorageException {
        if (Account == null)
            throw new StorageException("Can not update null Account");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (query.get().getUser(Account.getName()) == null)
                throw new StorageException("Account does not exist");
            StorageModifier modifier = this.controller.getStorageModifier();
            StorageAccountWrapper wrapper = new StorageAccountWrapper(Account);
            modifier.updateAccount(wrapper);
        } catch (Exception e) {
            LOG.error("Can't update Account -- " + e.getMessage(), e);
            StorageException ex = new StorageException(
                    "Can't update Account -- " + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query != null)
                query.decrementRef();
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteAccount(java.lang.String)
     */
    public void deleteAccount(String Accountname) throws StorageException {
        if (Accountname == null)
            throw new StorageException("can not delete null Account");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (query.get().getUser(Accountname) == null)
                throw new StorageException("Account does not exist");
            StorageModifier modifier = this.controller.getStorageModifier();
            modifier.deleteAccount(Accountname);
        } catch (Exception e) {
            LOG.error("Can't update Account -- " + e.getMessage(), e);
            StorageException ex = new StorageException(
                    "Can't update Account -- " + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query != null)
                query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeFeed(org.apache.lucene.gdata.data.ServerBaseFeed,
     *      java.lang.String)
     */
    public void storeFeed(ServerBaseFeed feed, String accountName)
            throws StorageException {
        if (feed == null)
            throw new StorageException("can not insert null feed");
        if (accountName == null)
            throw new StorageException("accountName must not be null");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (query.get().isFeedStored(feed.getId()))
                throw new StorageException("feed with feedID " + feed.getId()
                        + " is already stored");
            StorageModifier modifier = this.controller.getStorageModifier();
            StorageFeedWrapper wrapper = new StorageFeedWrapper(feed,
                    accountName);
            modifier.createFeed(wrapper);

        } catch (Exception e) {
            LOG.error("Can't create feed -- " + e.getMessage(), e);
            StorageException ex = new StorageException("Can't create feed -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query != null)
                query.decrementRef();
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteFeed(java.lang.String)
     */
    public void deleteFeed(String feedId) throws StorageException {
        if (feedId == null)
            throw new StorageException("can not delete feed id is null ");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (!query.get().isFeedStored(feedId))
                throw new StorageException("Account does not exist");
            StorageModifier modifier = this.controller.getStorageModifier();

            modifier.deleteFeed(feedId);

        } catch (Exception e) {
            LOG.error("Can't delete feed -- " + e.getMessage(), e);
            StorageException ex = new StorageException("Can't create feed -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query == null)
                query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateFeed(org.apache.lucene.gdata.data.ServerBaseFeed,
     *      java.lang.String)
     */
    public void updateFeed(ServerBaseFeed feed, String accountName)
            throws StorageException {
        if (feed == null)
            throw new StorageException("can not update null feed");
        if (accountName == null)
            throw new StorageException("accountName must not be null");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            if (!query.get().isFeedStored(feed.getId()))
                throw new StorageException("Account does not exist");
            StorageModifier modifier = this.controller.getStorageModifier();
            StorageFeedWrapper wrapper = new StorageFeedWrapper(feed,
                    accountName);
            modifier.updateFeed(wrapper);

        } catch (Exception e) {
            LOG.error("Can't create feed -- " + e.getMessage(), e);
            StorageException ex = new StorageException("Can't create feed -- "
                    + e.getMessage(), e);
            ex.setStackTrace(e.getStackTrace());
            throw ex;

        } finally {
            if (query == null)
                query.decrementRef();
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getServiceForFeed(java.lang.String)
     */
    public String getServiceForFeed(String feedId) throws StorageException {
        if (feedId == null)
            throw new StorageException("no feed for the feedID == null");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            String type = query.get().getService(feedId);
            if (type == null)
                throw new StorageException("no feed for the feedID == "
                        + feedId + " found");
            return type;
        } catch (Exception e) {
            throw new StorageException("Can not access storage", e);
        } finally {
            if (query != null)
                query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getAccount(java.lang.String)
     */
    public GDataAccount getAccount(String accountName) throws StorageException {
        if (accountName == null)
            throw new StorageException("account name must not be null");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            return query.get().getUser(accountName);
        } catch (Exception e) {
            throw new StorageException("Can not access storage", e);
        } finally {
            if (query != null)
                query.decrementRef();
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getAccountNameForFeedId(java.lang.String)
     */
    public String getAccountNameForFeedId(String feedId)
            throws StorageException {
        if (feedId == null)
            throw new StorageException("feedid must not be null");
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            String accountName = query.get().getAccountNameForFeedId(feedId);
            if (accountName == null)
                throw new StorageException("no feed for feedId " + feedId
                        + " found");
            return accountName;
        } catch (IOException e) {
            throw new StorageException("Can not access storage - "
                    + e.getMessage(), e);
        } finally {
            if (query != null)
                query.decrementRef();
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getEntryLastModified(java.lang.String, java.lang.String)
     */
    public Long getEntryLastModified(String entryId,String feedId) throws StorageException {
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            return new Long(query.get().getEntryLastModified(entryId,feedId));
        } catch (IOException e) {
            throw new StorageException("Can not access storage - "
                    + e.getMessage(), e);
        } finally {
            if (query != null)
                query.decrementRef();
        }

        
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getFeedLastModified(java.lang.String)
     */
    public Long getFeedLastModified(String feedId) throws StorageException {
        ReferenceCounter<StorageQuery> query = null;
        try {
            query = this.controller.getStorageQuery();
            return new Long(query.get().getFeedLastModified(feedId));
        } catch (IOException e) {
            throw new StorageException("Can not access storage - "
                    + e.getMessage(), e);
        } finally {
            if (query != null)
                query.decrementRef();
        }

            }

}
