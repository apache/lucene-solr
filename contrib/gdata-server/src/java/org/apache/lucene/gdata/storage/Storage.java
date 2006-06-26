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
package org.apache.lucene.gdata.storage;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;

/**
 * A interface every storage implementation must provide to access the
 * <tt>Storage</tt>. It describes all access methodes needed to store,
 * retrieve and look up data stored in the <tt>Storage</tt> component. This
 * interface acts as a <tt>Facade</tt> to hide the storage implementation from
 * the user.
 * <p>
 * This could also act as a proxy for a remote storage. It also removes any
 * restrictions from custom storage implementations.
 * </p>
 * 
 * 
 * @author Simon Willnauer
 * 
 */
/*
 * not final yet
 */
public interface Storage {

    /**
     * 
     * Stores the given entry. The ServerBaseEntry must provide a feed id and
     * the service type. configuration for the entry.
     * 
     * @param entry -
     *            the entry to store
     * 
     * @return - the stored Entry for the server response
     * @throws StorageException -
     *             if the entry can not be stored or required field are not set.
     */
    public abstract BaseEntry storeEntry(ServerBaseEntry entry)
            throws StorageException;

    /**
     * Deletes the given entry. The ServerBaseEntry just hase to provide the
     * entry id to be deleted.
     * 
     * @param entry -
     *            the entry to delete from the storage
     * @throws StorageException -
     *             if the entry can not be deleted or the entry does not exist
     *             or required field are not set.
     */
    public abstract void deleteEntry(ServerBaseEntry entry)
            throws StorageException;

    /**
     * Updates the given entry. The ServerBaseEntry must provide a feed id,
     * service id and the
     * {@link org.apache.lucene.gdata.server.registry.ProvidedService}
     * 
     * @param entry -
     *            the entry to update
     * 
     * @return - the updated entry for server response.
     * @throws StorageException -
     *             if the entry can not be updated or does not exist or required
     *             field are not set.
     */
    public abstract BaseEntry updateEntry(ServerBaseEntry entry)
            throws StorageException;

    /**
     * Retrieves the requested feed from the storage. The given ServerBaseFeed
     * must provide information about the feed id, max-result count and the
     * start index. To create feeds and entries also the service type must be
     * provided.
     * 
     * @param feed -
     *            the to retieve from the storage
     * @return the requested feed
     * @throws StorageException -
     *             the feed does not exist or can not be retrieved or required
     *             field are not set.
     */
    public abstract BaseFeed getFeed(ServerBaseFeed feed)
            throws StorageException;

    /**
     * Retrieves the requested entry from the storage. The given entry must
     * provide information about the entry id and service type.
     * 
     * @param entry -
     *            the entry to retrieve
     * @return - the requested entry
     * @throws StorageException -
     *             if the entry does not exist or can not be created or required
     *             field are not set.
     */
    public abstract BaseEntry getEntry(ServerBaseEntry entry)
            throws StorageException;

    /**
     * Saves a new account. Required attributes to set are <tt>password</tt>
     * and <tt>accountname</tt>
     * 
     * @param account -
     *            the account to save
     * @throws StorageException -
     *             if the account can not be stored or the account already
     *             exists or required field are not set.
     */
    public abstract void storeAccount(final GDataAccount account)
            throws StorageException;

    /**
     * Updates an existing account. Required attributes to set are
     * <tt>password</tt> and <tt>accountname</tt>
     * 
     * @param account -
     *            the account to update
     * @throws StorageException -
     *             if the account does not exist or required field are not set.
     */
    public abstract void updateAccount(final GDataAccount account)
            throws StorageException;

    /**
     * Deletes the account for the given account name. All feeds and entries
     * referencing this account will be deleted as well!
     * 
     * @param accountname -
     *            the name of the account to delete
     * @throws StorageException -
     *             if the account does not exist
     */
    public abstract void deleteAccount(final String accountname)
            throws StorageException;

    /**
     * Stores a new feed for a existing account. The Feed must provide
     * information about the service type to store the feed for and the feed id
     * used for accessing and retrieving the feed from the storage. Each feed is
     * associated with a provided service. This method does check wheather a
     * feed with the same feed id as the given feed does already exists.
     * 
     * @see org.apache.lucene.gdata.server.registry.ProvidedService
     * @param feed -
     *            the feed to create
     * @param accountname -
     *            the account name belongs to the feed
     * @throws StorageException -
     *             if the feed already exists or the feed can not be stored
     */
    public abstract void storeFeed(final ServerBaseFeed feed, String accountname)
            throws StorageException;

    /**
     * Deletes the feed for the given feed id. All Entries referencing the given
     * feed id will be deleted as well.
     * 
     * @param feedId -
     *            the feed id for the feed to delete.
     * @throws StorageException -
     *             if the feed for the feed id does not exist or the feed can
     *             not be deleted
     */
    public abstract void deleteFeed(final String feedId)
            throws StorageException;

    /**
     * Updates a stored feed. The Feed must provide information about the
     * service type to store the feed for and the feed id used for accessing and
     * retrieving the feed from the storage.
     * 
     * @param feed -
     *            the feed to update
     * @param accountname -
     *            the account name belongs to the feed
     * @throws StorageException -
     *             if the feed does not exist or the feed can not be updated
     */
    public abstract void updateFeed(final ServerBaseFeed feed,
            String accountname) throws StorageException;

    /**
     * Retrieves the service name for a stored feed
     * 
     * @param feedId -
     *            the feed id
     * @return - the name of the service
     * @throws StorageException -
     *             if no feed for the provided id is stored
     */
    public abstract String getServiceForFeed(String feedId)
            throws StorageException;

    /**
     * @param accountName -
     *            the name of the requested account
     * @return - a {@link GDataAccount} instance for the requested account name
     * @throws StorageException -
     *             if no account for the account name is stored
     * 
     */
    public abstract GDataAccount getAccount(String accountName)
            throws StorageException;

    /**
     * close this storage instance. This method will be called by clients after
     * use.
     */
    public abstract void close();

    /**
     * Each feed belongs to one specific account. This method retrieves the
     * account name for
     * 
     * @param feedId -
     *            the id of the feed to retrieve the accountname
     * @return - the name / id of the account associated with the feed for the
     *         given feed id
     * @throws StorageException -
     *             if the feed is not stored or the storage can not be accessed
     */
    public String getAccountNameForFeedId(String feedId)
            throws StorageException;

    /**
     * Retrieves the date of the last modification for the given id
     * 
     * @param entryId -
     *            the entry Id
     * @param feedId -
     *            the feed which contains the entry
     * @return - The date of the last modifiaction in milliseconds or
     *         <code>new Long(0)</code> if the resource can not be found eg.
     *         the time can not be accessed
     * @throws StorageException -
     *             if the storage can not be accessed
     */
    public Long getEntryLastModified(String entryId, String feedId)
            throws StorageException;

    /**
     * Retrieves the date of the last modification for the given id
     * 
     * @param feedId -
     *            the feed Id
     * @return - The date of the last modifiaction in milliseconds or
     *         <code>new Long(0)</code> if the resource can not be found eg.
     *         the time can not be accessed
     * @throws StorageException -
     *             if the storage can not be accessed
     */
    public Long getFeedLastModified(String feedId) throws StorageException;

}
