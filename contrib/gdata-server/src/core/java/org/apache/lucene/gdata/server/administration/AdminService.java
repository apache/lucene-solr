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
package org.apache.lucene.gdata.server.administration;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.Service;
import org.apache.lucene.gdata.server.ServiceException;

/**
 * The AdminService interface extends the Service interface to serve common
 * administrator requests. Common users can not create feed or user instances.
 * This interface provides all actions for create, delete or update Users and
 * Feeds. Each Feed has an associated Feed - Name which acts as an ID. Feed will
 * be identified by the feed name e.g. {@link com.google.gdata.data.Source#getId()}
 * <p>User accounts are supposed to have a unique username attribute as the username acts as a primary key for the storage</p>
 *  
 * 
 * @author Simon Willnauer
 * 
 */
public interface AdminService extends Service {

    /**
     * Creates a new feed instance.
     * 
     * @param feed -
     *            the feed to create
     * @param account - the account who own this feed
     * @throws ServiceException -
     *             if the feed can not be created
     */
    public abstract void createFeed(final ServerBaseFeed feed,
            final GDataAccount account) throws ServiceException;

    /**
     * Updates the given feed
     * 
     * @param feed -
     *            the feed to update
     * @param account - the account who own this feed
     * 
     * @throws ServiceException -
     *             if the feed can not be updated or does not exist.
     */
    public abstract void updateFeed(final ServerBaseFeed feed,
            final GDataAccount account) throws ServiceException;

    /**
     * Deletes the given feed and all containing entries from the storage. The feed will not be accessable
     * anymore.
     * 
     * @param feed -
     *            the feed to deltete
     * 
     * @throws ServiceException -
     *             if the feed can not be deleted or does not exist
     */
    public abstract void deleteFeed(final ServerBaseFeed feed) throws ServiceException;

    /**
     * Creates a new account accout.
     * 
     * @param account -
     *            the account to create
     * @throws ServiceException -
     *             if the account can not be created or the account does already
     *             exist.
     */
    public abstract void createAccount(final GDataAccount account)
            throws ServiceException;

    /**
     * Deletes the given account from the storage. it will also delete all
     * accociated feeds.
     * 
     * @param account
     *            the account to delete
     * @throws ServiceException -
     *             if the account does not exist or the account can not be deleted
     */
    public abstract void deleteAccount(final GDataAccount account)
            throws ServiceException;

    /**
     * Updates the given account if the account already exists.
     * 
     * @param account - the account to update 
     * @throws ServiceException - if the account can not be updated or the account does not exist
     */
    public abstract void updateAccount(final GDataAccount account)
            throws ServiceException;

    /**
     * Returns the account for the given account name or <code>null</code> if the account does not exist 
     * 
     * @param account - account name
     * @return - the account for the given account name or <code>null</code> if the account does not exist
     * @throws ServiceException - if the account can not be accessed
     */
    public abstract GDataAccount getAccount(String account) throws ServiceException;
    
    /**
     * Returns the account associated with the feed for the given feed id
     * @param feedId - the feed id
     * @return - the GdataAccount assoziated with the feed for the given feed Id or <code>null</code> if there is no feed for the given feed Id
     * @throws ServiceException - if the storage can not be accessed
     */
    public abstract GDataAccount getFeedOwningAccount(String feedId) throws ServiceException;
}
