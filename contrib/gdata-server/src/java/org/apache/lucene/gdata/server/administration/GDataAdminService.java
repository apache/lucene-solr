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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.GDataService;
import org.apache.lucene.gdata.server.ServiceException;
import org.apache.lucene.gdata.storage.StorageException;



/**
 * default implementation of the {@link org.apache.lucene.gdata.server.administration.AdminService} interface.
 * @author Simon Willnauer
 *
 */
public class GDataAdminService extends GDataService implements AdminService {
    private static final Log LOG = LogFactory.getLog(GDataAdminService.class);
    /**
     * @throws ServiceException
     */
    public GDataAdminService() throws ServiceException {
        super();
        
    }

    

    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#createFeed(org.apache.lucene.gdata.data.ServerBaseFeed, org.apache.lucene.gdata.data.GDataAccount)
     */
    public void createFeed(final ServerBaseFeed feed,final GDataAccount account) throws ServiceException {
        if(feed == null)
            throw new ServiceException("Can not create feed -- feed is null");
        if(account == null)
            throw new ServiceException("Can not create feed -- account is null");
        if(feed.getId() == null)
            throw new ServiceException("Feed ID is null can not create feed");
        if(account.getName() == null)
            throw new ServiceException("Account name is null -- can't create feed");
    try {
        feed.setAccount(account);
        this.storage.storeFeed(feed,account.getName());
    } catch (StorageException e) {
        if(LOG.isInfoEnabled())
            LOG.info("Can not save feed -- "+e.getMessage(),e);
        throw new ServiceException("Can not save feed",e);
    }
 
    }


   
    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#updateFeed(org.apache.lucene.gdata.data.ServerBaseFeed, org.apache.lucene.gdata.data.GDataAccount)
     */
    public void updateFeed(ServerBaseFeed feed, GDataAccount account) throws ServiceException {
        if(feed == null)
            throw new ServiceException("Can not update null feed");
        if(account == null)
            throw new ServiceException("Can not update feed -- account is null");
        if(feed.getId() == null)
            throw new ServiceException("Feed ID is null can not update feed");
        if(account.getName() == null)
            throw new ServiceException("Account name is null -- can't update feed");
    try {
        feed.setAccount(account);
        this.storage.updateFeed(feed,account.getName());
    } catch (StorageException e) {
        if(LOG.isInfoEnabled())
            LOG.info("Can not update feed -- "+e.getMessage(),e);
        throw new ServiceException("Can not update feed",e);
    }

    }

   
    
    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#deleteFeed(org.apache.lucene.gdata.data.ServerBaseFeed)
     */
    public void deleteFeed(ServerBaseFeed feed) throws ServiceException {
        if(feed == null)
            throw new ServiceException("Can not delete null feed");
        if(feed.getId() == null)
            throw new ServiceException("Feed ID is null can not delete feed");
    try {
        this.storage.deleteFeed(feed.getId());
    } catch (StorageException e) {
        if(LOG.isInfoEnabled())
            LOG.info("Can not delete feed -- "+e.getMessage(),e);
        throw new ServiceException("Can not delete feed",e);
    }

    }

    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#createAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void createAccount(GDataAccount account) throws ServiceException {
        if(account == null)
            throw new ServiceException("Can not save null account");
        try {
            this.storage.storeAccount(account);
        } catch (StorageException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not save account -- "+e.getMessage(),e);
            throw new ServiceException("Can not save account",e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#deleteAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void deleteAccount(GDataAccount account) throws ServiceException {
        if(account == null)
            throw new ServiceException("Can not delete null account");
        try {
            this.storage.deleteAccount(account.getName());
        } catch (StorageException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not save account -- "+e.getMessage(),e);
            throw new ServiceException("Can not save account",e);
        }
    }

    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#updateAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void updateAccount(GDataAccount account) throws ServiceException {
        if(account == null)
            throw new ServiceException("Can not update null account");
        try {
            this.storage.updateAccount(account);
        } catch (StorageException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not save account -- "+e.getMessage(),e);
            throw new ServiceException("Can not save account",e);
        }
    }
    
    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#getAccount(java.lang.String)
     */
    public GDataAccount getAccount(String accountName)throws ServiceException{
        if(accountName == null)
            throw new ServiceException("Can not get null account");
        try {
            return this.storage.getAccount(accountName);
        } catch (StorageException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not get account -- "+e.getMessage(),e);
            throw new ServiceException("Can not get account",e);
        }
       
    }



    /**
     * @see org.apache.lucene.gdata.server.administration.AdminService#getFeedOwningAccount(java.lang.String)
     */
    public GDataAccount getFeedOwningAccount(String feedId) throws ServiceException {
        if(feedId == null)
            throw new ServiceException("Can not get account - feed id must not be null");
        try {
            String accountName =  this.storage.getAccountNameForFeedId(feedId);
           return this.storage.getAccount(accountName);
            
        } catch (StorageException e) {
            if(LOG.isInfoEnabled())
                LOG.info("Can not get account for feed Id -- "+e.getMessage(),e);
            throw new ServiceException("Can not get account for the given feed id",e);
        }
    }

   

}
