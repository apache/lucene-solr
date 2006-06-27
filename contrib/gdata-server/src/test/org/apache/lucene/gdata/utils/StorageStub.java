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

package org.apache.lucene.gdata.utils;

import java.util.Date;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.data.ServerBaseFeed;
import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageController;
import org.apache.lucene.gdata.storage.StorageException;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;

/**
 * @author Simon Willnauer
 *
 */
@Component(componentType=ComponentType.STORAGECONTROLLER)
public class StorageStub implements Storage, StorageController {
public static String SERVICE_TYPE_RETURN = "service";
    /**
     * 
     */
    public StorageStub() {
        super();
        // TODO Auto-generated constructor stub
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry storeEntry(ServerBaseEntry entry)
            throws StorageException {

        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public void deleteEntry(ServerBaseEntry entry) throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry updateEntry(ServerBaseEntry entry)
            throws StorageException {

        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getFeed(org.apache.lucene.gdata.data.ServerBaseFeed)
     */
    public BaseFeed getFeed(ServerBaseFeed feed) throws StorageException {

        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getEntry(org.apache.lucene.gdata.data.ServerBaseEntry)
     */
    public BaseEntry getEntry(ServerBaseEntry entry)
            throws StorageException {

        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void storeAccount(GDataAccount Account) throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateAccount(org.apache.lucene.gdata.data.GDataAccount)
     */
    public void updateAccount(GDataAccount Account) throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteAccount(java.lang.String)
     */
    public void deleteAccount(String Accountname) throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#storeFeed(org.apache.lucene.gdata.data.ServerBaseFeed, java.lang.String)
     */
    public void storeFeed(ServerBaseFeed feed, String accountname)
            throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#deleteFeed(java.lang.String)
     */
    public void deleteFeed(String feedId) throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#updateFeed(org.apache.lucene.gdata.data.ServerBaseFeed, java.lang.String)
     */
    public void updateFeed(ServerBaseFeed feed, String accountname)
            throws StorageException {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#close()
     */
    public void close() {
    }

    /**
     * @see org.apache.lucene.gdata.storage.Storage#getServiceForFeed(java.lang.String)
     */
    public String getServiceForFeed(String feedId) throws StorageException {

        return SERVICE_TYPE_RETURN;
    }

    public void destroy() {
    }

    public Storage getStorage() throws StorageException {
        
        return new StorageStub();
    }

    public GDataAccount getAccount(String accountName) throws StorageException {
        
        return null;
    }

    public String getAccountNameForFeedId(String feedId) throws StorageException {
        
        return null;
    }

    public void initialize() {
    }


    public Long getFeedLastModified(String feedId) throws StorageException {
        
        return null;
    }

    public Long getEntryLastModified(String entryId, String feedId) throws StorageException {
        
        return null;
    }

}
