/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.lucene.gdata.storage.db4o;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.storage.ModificationConflictException;
import org.apache.lucene.gdata.storage.ResourceNotFoundException;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;

import com.db4o.ObjectContainer;
import com.db4o.ObjectSet;
import com.db4o.query.Query;
import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.data.DateTime;

/**
 * 
 * Storage implementation for the DB4o storage component
 * @author Simon Willnauer
 * 
 */
public class DB4oStorage implements Storage {
    private static final Log LOG = LogFactory.getLog(DB4oStorage.class);

    private static final int RENDER_ACTIVATION_DEPTH = 100;

    private final ObjectContainer container;

    private final StorageController controller;

    private final List<String> semaphore = new ArrayList<String>();

    
    protected DB4oStorage(final ObjectContainer container,
            StorageController controller) {
        this.container = container;
        this.controller = controller;
    }

    private void createSemaphore(String key)
            throws ModificationConflictException {
        this.semaphore.add(key);
        if (this.container.ext().setSemaphore(key, 0))
            return;
        throw new ModificationConflictException(
                "can not create semaphore for key -- " + key);
    }

    private void releaseAllSemaphore() {
        for (String key : this.semaphore) {
            this.container.ext().releaseSemaphore(key);
        }
        this.semaphore.clear();
    }

    private void releaseSemaphore(String key) {
        if (this.semaphore.contains(key)) {
            this.container.ext().releaseSemaphore(key);
            this.semaphore.remove(key);
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry storeEntry(ServerBaseEntry entry) throws StorageException {
        if (entry == null)
            throw new StorageException("Can not store entry -- is null");

        if (entry.getFeedId() == null)
            throw new StorageException("can not store entry -- feed id is null");
        if (LOG.isDebugEnabled())
            LOG.debug("Storing entry for feed: " + entry.getFeedId());
        BaseFeed<BaseFeed, BaseEntry> feed = getFeedOnly(entry.getFeedId(),entry.getServiceType());
       refreshPersistentObject(feed);
        try {
            StringBuilder idBuilder = new StringBuilder(entry.getFeedId());
            idBuilder.append(this.controller.releaseId());
            entry.setId(idBuilder.toString());
        } catch (StorageException e) {
            LOG.error("Can not create uid for entry -- " + e.getMessage(), e);
            throw new StorageException("Can not create uid for entry -- "
                    + e.getMessage(), e);

        }
        setUpdated(entry, feed);
        DB4oEntry intEntry = new DB4oEntry();
        intEntry.setEntry(entry.getEntry());
        intEntry.setUpdateTime(entry.getUpdated().getValue());
        intEntry.setFeedId(feed.getId());
        intEntry.setVersion(entry.getVersion());

       
        try {
            this.container.set(feed);
            this.container.set(intEntry);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
        if (LOG.isInfoEnabled())
            LOG.info("Stored Entry for entryID: " + entry.getId()
                    + " -- feedID: " + entry.getFeedId());
        return entry.getEntry();
    }

    private void setUpdated(ServerBaseEntry entry, DB4oEntry intEntry) {
        if (entry.getUpdated().compareTo(intEntry.getEntry().getUpdated()) <= 0) {
            if (LOG.isDebugEnabled())
                LOG
                        .debug("Set new UpdateTime to entry new entry time is less or equal the time of the stored entry -- old Entry: "
                                + intEntry.getEntry().getUpdated()
                                + "; new Entry: " + entry.getUpdated());
            entry.setUpdated(new DateTime(System.currentTimeMillis(), entry
                    .getUpdated().getTzShift()));
        }

    }

    private void setUpdated(ServerBaseEntry entry,
            BaseFeed<BaseFeed, BaseEntry> feed) {
        if (entry.getUpdated() != null){
            long timeInMilli = entry.getUpdated().getValue();
            int tzShift = entry.getUpdated().getTzShift();
            feed.setUpdated(new DateTime(timeInMilli, tzShift));
        }
        else{
            int timezone = 0;
            if(feed.getUpdated() != null){
                 timezone = feed.getUpdated().getTzShift();
            }
            long timeInMilli = System.currentTimeMillis();
            entry.setUpdated(new DateTime(timeInMilli,timezone));
            feed.setUpdated(new DateTime(timeInMilli,timezone));
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void deleteEntry(ServerBaseEntry entry) throws StorageException {
        if (entry == null)
            throw new StorageException("Can not delete entry -- is null");
        if (entry.getFeedId() == null)
            throw new StorageException(
                    "can not delete entry -- feed id is null");
        if (entry.getId() == null)
            throw new StorageException("Can not delete entry -- id is null");
        if (LOG.isDebugEnabled())
            LOG.debug("delete entry for feed: " + entry.getFeedId()
                    + " entry ID: " + entry.getId());
        DB4oEntry persistentEntry = getInternalEntry(entry.getId());
        // lock the entry to prevent concurrent access
        createSemaphore(entry.getId());
        refreshPersistentObject(persistentEntry);
        if(persistentEntry.getVersion() != entry.getVersion())
            throw new ModificationConflictException(
                    "Current version does not match given version  -- currentVersion: "+persistentEntry.getVersion()+"; given Version: "+entry.getVersion() );
        BaseFeed<BaseFeed, BaseEntry> feed = getFeedOnly(entry.getFeedId(),entry.getServiceType());
        refreshPersistentObject(feed);
        DateTime time = DateTime.now();
        if (persistentEntry.getEntry().getUpdated() != null)
            time.setTzShift(persistentEntry.getEntry().getUpdated().getTzShift());
        feed.setUpdated(time);
        try {
            //delete the entry
            this.container.delete(persistentEntry.getEntry());
            this.container.delete(persistentEntry);
            this.container.set(feed);
            this.container.commit();
            
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        } finally {
            releaseSemaphore(entry.getId());
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry updateEntry(ServerBaseEntry entry) throws StorageException {
        if (entry == null)
            throw new StorageException("Can not update entry -- is null");
        if (entry.getFeedId() == null)
            throw new StorageException(
                    "can not delete entry -- feed id is null");
        if (entry.getId() == null)
            throw new StorageException("Can not delete entry -- id is null");

        DB4oEntry persistentEntry = getInternalEntry(entry.getId());
        // lock the entry to prevent concurrent access
        createSemaphore(entry.getId());
        refreshPersistentObject(persistentEntry);
        if(persistentEntry.getVersion() != entry.getVersion())
            throw new ModificationConflictException(
                    "Current version does not match given version  -- currentVersion: "+persistentEntry.getVersion()+"; given Version: "+entry.getVersion() );
        
        setUpdated(entry, persistentEntry);
        BaseFeed<BaseFeed, BaseEntry> feed = getFeedOnly(entry.getFeedId(),entry.getServiceType());
        refreshPersistentObject(feed);
        BaseEntry retVal = entry.getEntry(); 
        DB4oEntry newEntry = new DB4oEntry();
        newEntry.setEntry(retVal);
        newEntry.setUpdateTime(entry.getUpdated().getValue());
        newEntry.setFeedId(feed.getId());
        // increment Version
        newEntry.setVersion((entry.getVersion())+1);

        setUpdated(entry, feed);
        try {
            this.container.set(feed);
            this.container.set(newEntry);
            this.container.delete(persistentEntry.getEntry());
            this.container.delete(persistentEntry);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        } finally {
            releaseSemaphore(entry.getId());
        }
        return retVal;

    }
    


    /**
     * @see org.apache.lucene.gdata.storage.Storage#getFeed(org.apache.lucene.gdata.data.ServerBaseFeed)
     */
    @SuppressWarnings("unchecked")
    public BaseFeed getFeed(ServerBaseFeed feed) throws StorageException {
        if (feed.getId() == null)
            throw new StorageException("can not get feed -- feed id is null");
        if (feed.getStartIndex() < 1)
            feed.setStartIndex(1);
        if (feed.getItemsPerPage() < 0)
            feed.setItemsPerPage(25);

        if (LOG.isInfoEnabled())
            LOG.info("Fetching feed for feedID: " + feed.getId()
                    + "; start-index: " + feed.getStartIndex()
                    + "; items per page: " + feed.getItemsPerPage());

       BaseFeed<BaseFeed, BaseEntry> persistentFeed = getFeedOnly(feed.getId(),feed.getServiceType());
       /*
        * prevent previously added entries in long running storage instances
        */
       clearDynamicElements(persistentFeed);
        Query query = this.container.query();
        query.constrain(DB4oEntry.class);
        query.descend("feedId").constrain(feed.getId()).equal();
        query.descend("updateTime").orderDescending();

        ObjectSet<DB4oEntry> set = query.execute();
       
        int size = set.size();
        
        if (size < feed.getStartIndex()) {
            if (LOG.isDebugEnabled())
                LOG.debug("no entries found for feed constrain -- feedID: "
                        + feed.getId() + "; start-index: "
                        + feed.getStartIndex() + "; items per page: "
                        + feed.getItemsPerPage());
            return persistentFeed;
        }

        int start = feed.getStartIndex() - 1;
        int items = start + feed.getItemsPerPage();
        if (items > size)
            items = size;
        
        List<DB4oEntry> sublist = set.subList(start, items);
        
        for (DB4oEntry entry : sublist) {
            persistentFeed.getEntries().add(clearDynamicElements(entry.getEntry()));
        }
        this.container.activate(persistentFeed,RENDER_ACTIVATION_DEPTH);
        return persistentFeed;

    }

    @SuppressWarnings("unchecked")
    private BaseFeed<BaseFeed, BaseEntry> getFeedOnly(final String feedId, final String serviceId)
            throws StorageException {
        if(!checkService(feedId,serviceId))
            throw new StorageException();
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
      
        query.constrain(BaseFeed.class);

        query.descend("id").constrain(feedId).equal();

        ObjectSet set = query.execute();
        if (set.size() > 1)
            throw new StorageException("Query for feed id " + feedId
                    + " returns more than one result");
        if (set.hasNext())
        return (BaseFeed<BaseFeed, BaseEntry>) set.next();
        throw new ResourceNotFoundException("can not find feed for given feed id -- "
                + feedId);

    }
    private boolean checkService(String feedId,String serviceId){
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feedId).equal();
        query.descend("serviceType").constrain(serviceId).equal();
        return query.execute().size() == 1;
    }
    private ObjectSet getEnriesForFeedID(String feedId) {
        Query query = this.container.query();
        query.constrain(DB4oEntry.class);
        query.descend("feedId").constrain(feedId).equal();

        return query.execute();
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */

    public BaseEntry getEntry(ServerBaseEntry entry) throws StorageException {
        if (entry == null)
            throw new StorageException("can not retrieve entry -- is null");
        if (entry.getId() == null)
            throw new StorageException("can not retrieve entry -- id is null");
        if (LOG.isInfoEnabled())
            LOG.info("Retrieving entry for entryID: " + entry.getId());
        DB4oEntry retval = getInternalEntry(entry.getId());
        this.container.activate(retval.getEntry(),RENDER_ACTIVATION_DEPTH);
        return clearDynamicElements(retval.getEntry());
    }

    @SuppressWarnings("unchecked")
    private DB4oEntry getInternalEntry(String id) throws StorageException {
        Query query = this.container.query();
        query.constrain(DB4oEntry.class);
        query.descend("entry").descend("id").constrain(id).equal();
        ObjectSet<DB4oEntry> resultSet = query.execute();
        if (resultSet.size() > 1)
            throw new StorageException(
                    "Entry query returned not a unique result");
        if (resultSet.hasNext())
            return resultSet.next();
        throw new ResourceNotFoundException("no entry with entryID: " + id
                + " stored -- query returned no result");
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void storeAccount(GDataAccount account) throws StorageException {
        if (account == null)
            throw new StorageException("can not store account -- is null");
        if (account.getName() == null)
            throw new StorageException("can not store account -- name is null");
        if (account.getPassword() == null)
            throw new StorageException(
                    "can not store account -- password is null");
        try {
            getAccount(account.getName());
            throw new IllegalStateException("account with accountname: "
                    + account.getName() + " already exists");
        } catch (IllegalStateException e) {
            throw new StorageException("Account already exists");
        } catch (StorageException e) {
            if (LOG.isDebugEnabled())
                LOG
                        .debug("checked account for existence -- does not exist -- store account");
        }
        try {
            this.container.set(account);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
        if (LOG.isInfoEnabled())
            LOG.info("Stored account: " + account);
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void updateAccount(GDataAccount account) throws StorageException {
        if (account == null)
            throw new StorageException("can not update account -- is null");
        if (account.getName() == null)
            throw new StorageException("can not update account -- name is null");
        if (account.getPassword() == null)
            throw new StorageException(
                    "can not update account -- password is null");
        GDataAccount persitentAccount = getAccount(account.getName());
        refreshPersistentObject(persitentAccount);
        try {
            this.container.set(account);
            this.container.delete(persitentAccount);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteAccount(java.lang.String)
     */
    public void deleteAccount(String accountname) throws StorageException {
        if (accountname == null)
            throw new StorageException(
                    "can not delete account -- account name is null");
        GDataAccount account = this.getAccount(accountname);
        refreshPersistentObject(account);
        if (LOG.isInfoEnabled())
            LOG.info("delete account -- account name: " + accountname);
        try {
            this.container.delete(account);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeFeed(org.apache.lucene.gdata.data.ServerBaseFeed,
     *      java.lang.String)
     */
    public void storeFeed(ServerBaseFeed feed, String accountname)
            throws StorageException {
        if (feed == null)
            throw new StorageException("Can not store feed -- is null");
        if (feed.getId() == null)
            throw new StorageException("Can not store feed -- id is null");
        if(feed.getServiceType() == null)
            throw new StorageException("Can not store feed -- service type is null");
        if(accountname == null)
            throw new StorageException("Account name is null");
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feed.getId()).equal();
        ObjectSet set = query.execute();
        if (set.hasNext())
            throw new StorageException("feed with feedID " + feed.getId()
                    + " is already stored");
        GDataAccount account = getAccount(accountname);
        refreshPersistentObject(account);
        feed.setAccount(account);
        /*
         * service config not required in db4o storage.
         * Entries/Feeds don't have to be build from xml
         */
        feed.setServiceConfig(null);
        try {
            this.container.set(feed);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteFeed(java.lang.String)
     */
    public void deleteFeed(String feedId) throws StorageException {
        if (feedId == null)
            throw new StorageException("can not delete feed -- feed id is null");
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feedId).equal();
        ObjectSet set = query.execute();
        if (set.size() > 1)
            throw new StorageException(
                    "Feed query returned not a unique result");
        if (set.size() == 0)
            throw new StorageException("no feed with feedID: " + feedId
                    + " stored -- query returned no result");

        ServerBaseFeed feed = (ServerBaseFeed) set.next();
        refreshPersistentObject(feed);
        ObjectSet entrySet = getEnriesForFeedID(feed.getId());
        try {
            this.container.delete(feed);
            this.container.delete(feed.getFeed());
            for (Object object : entrySet) {
                refreshPersistentObject(object);
                this.container.delete(object);
            }
            
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occured on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }

    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateFeed(org.apache.lucene.gdata.data.ServerBaseFeed,
     *      java.lang.String)
     */
    @SuppressWarnings("unchecked")
    public void updateFeed(ServerBaseFeed feed, String accountname)
            throws StorageException {
        if (feed == null)
            throw new StorageException("Can not update feed -- is null");
        if (feed.getId() == null)
            throw new StorageException("Can not update feed -- id is null");
        if(feed.getServiceType() == null)
            throw new StorageException("Can not update feed -- service type is null");
        if(accountname == null)
            throw new StorageException("Account name is null");
        GDataAccount account = getAccount(accountname);
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feed.getId());
        ObjectSet<ServerBaseFeed> set=  query.execute();
        if (set.size() > 1)
            throw new StorageException("Query for feed id " + feed.getId()
                    + " returns more than one result");
        if (set.size() < 1)
            throw new StorageException("can not find feed for given feed id -- "
                + feed.getId());
        ServerBaseFeed result = set.next();
        refreshPersistentObject(result);
        BaseFeed oldFeed = result.getFeed();
        result.setAccount(account);
        result.setFeed(feed.getFeed());
        try {
            this.container.delete(oldFeed);
            this.container.set(result);
            this.container.commit();
        } catch (Exception e) {
            LOG
                    .error("Error occurred on persisting changes -- rollback changes");
            this.container.rollback();
            throw new StorageException("Can not persist changes -- "
                    + e.getMessage(), e);
        }
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getServiceForFeed(java.lang.String)
     */
    @SuppressWarnings("unchecked")
    public String getServiceForFeed(final String feedId) throws StorageException {
        if(feedId == null)
            throw new StorageException("can not get Service for feed -- feed id is null");
        if(LOG.isInfoEnabled())
            LOG.info("Retrieving Service for feed -- feed id: "+feedId);
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feedId);
        ObjectSet<ServerBaseFeed> feed =  query.execute();
        if (feed.size() > 1)
            throw new StorageException("Query for feed id " + feedId
                    + " returns more than one result");
        if (feed.size() < 1)
            throw new StorageException("can not find feed for given feed id -- "
                + feedId);
        
        ServerBaseFeed result = feed.next();
        if(LOG.isInfoEnabled())
            LOG.info("Retrieved Service for feed -- serviceType: "+result.getServiceType());
        return result.getServiceType();
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getAccount(java.lang.String)
     */
    public GDataAccount getAccount(String accountName) throws StorageException {
        if (accountName == null)
            throw new StorageException(
                    "Can not get account -- account name is null");
        if (LOG.isInfoEnabled())
            LOG.info("Retrieving account for account name: " + accountName);
        Query query = this.container.query();
        query.constrain(GDataAccount.class);
        query.descend("name").constrain(accountName).equal();
        ObjectSet set = query.execute();
        if (set.size() > 1)
            throw new StorageException(
                    "Account query returned not a unique result -- account name: "
                            + accountName);
        if (!set.hasNext())
            throw new ResourceNotFoundException(
                    "No such account stored -- query returned not result for account name: "
                            + accountName);

        return (GDataAccount) set.next();
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#close()
     */
    public void close() {
        releaseAllSemaphore();
        
    }
    
   

    
    /**
     * @see org.apache.lucene.gdata.storage.Storage#getAccountNameForFeedId(java.lang.String)
     */
    
    public String getAccountNameForFeedId(String feedId)
            throws StorageException {
        if(feedId == null)
            throw new StorageException("feed id is null");
        GDataAccount account = getServerBaseFeed(feedId).getAccount();
        if(account == null)
            throw new IllegalStateException("No account stored with feed -- feedID: "+feedId);
        
        return account.getName();
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getEntryLastModified(java.lang.String, java.lang.String)
     */
    public Long getEntryLastModified(String entryId, final String feedId)
            throws StorageException {
        if(entryId == null)
            throw new StorageException("Entry ID is null");
        return new Long(getInternalEntry(entryId).getUpdateTime());
    }
    
    @SuppressWarnings("unchecked")
    private ServerBaseFeed getServerBaseFeed(String feedId)throws StorageException{
        Query query = this.container.query();
        query.constrain(ServerBaseFeed.class);
        query.descend("feed").descend("id").constrain(feedId);
        ObjectSet<ServerBaseFeed> feed =  query.execute();
        if (feed.size() > 1)
            throw new StorageException("Query for feed id " + feedId
                    + " returns more than one result");
        if (feed.size() < 1)
            throw new StorageException("can not find feed for given feed id -- "
                + feedId);
        return feed.next();
    }
    
    /*
     * !Caution! -- could instantiate a lot of objects if used with certain classes!!
     * Refresh a persisted object with a depth of 100
     * 
     */
    private void refreshPersistentObject(Object o){
        this.container.ext().refresh(o,100);
    }
    
    /**
     * @see org.apache.lucene.gdata.storage.Storage#getFeedLastModified(java.lang.String)
     */
    public Long getFeedLastModified(String feedId) throws StorageException {
        if(feedId == null)
            throw new StorageException("can not get last modified -- id is null");
        ServerBaseFeed feed = getServerBaseFeed(feedId);
        return new Long(feed.getUpdated().getValue());
     
    }
    
    private BaseEntry clearDynamicElements(BaseEntry entry){
        this.container.ext().refresh(entry.getLinks(), 2);
        return entry;
    }
    private BaseFeed clearDynamicElements(BaseFeed feed){
        this.container.ext().refresh(feed.getLinks(), 2);
        feed.getEntries().clear();
        return feed;
    }
    ObjectContainer getContainer(){
        return this.container;
    }

    static class DB4oEntry {
        private BaseEntry entry;
        
        private int version;

        private String feedId;

        private long updateTime;

        /**
         * @return Returns the entry.
         */
        protected BaseEntry getEntry() {
            return this.entry;
        }

        /**
         * @param entry
         *            The entry to set.
         */
        protected void setEntry(BaseEntry entry) {
            this.entry = entry;
        }

        /**
         * @return Returns the feed.
         */
        protected String getFeedId() {
            return this.feedId;
        }

        /**
         * @param feed
         *            The feed to set.
         */
        protected void setFeedId(String feed) {
            this.feedId = feed;
        }

        /**
         * @return Returns the updateTime.
         */
        protected long getUpdateTime() {
            return this.updateTime;
        }

        /**
         * @param updateTime
         *            The updateTime to set.
         */
        protected void setUpdateTime(long updateTime) {
            this.updateTime = updateTime;
        }

        /**
         * @return Returns the version.
         */
        public int getVersion() {
            return this.version;
        }

        /**
         * @param version The version to set.
         */
        public void setVersion(int version) {
            this.version = version;
            if(this.entry != null)
                this.entry.setVersionId(""+this.version);
        }

    }
    
    
    
    
    

}
