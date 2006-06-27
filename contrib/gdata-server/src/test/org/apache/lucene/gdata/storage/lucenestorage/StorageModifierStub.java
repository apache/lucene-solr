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

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer;
import org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController;
import org.apache.lucene.gdata.storage.lucenestorage.StorageModifier;
import org.apache.lucene.gdata.utils.StorageControllerStub;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.store.RAMDirectory;

/**
 * @author Simon Willnauer
 *
 */
public class StorageModifierStub extends StorageModifier {

    /**
     * @param controller
     * @param modifier
     * @param buffer
     * @param persitsFactor
     * @param optimizeInterval
     * @throws IOException 
     * @throws StorageException 
     */
    public StorageModifierStub(StorageCoreController controller,
            IndexModifier modifier, StorageBuffer buffer, int persitsFactor,
            int optimizeInterval) throws IOException, StorageException {
        
        super(new StorageCoreController(), new IndexModifier(new RAMDirectory(),new StandardAnalyzer(),true), new StorageBuffer(1),1, 1);
        
        // TODO Auto-generated constructor stub
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#close()
     */
    @Override
    protected void close() throws IOException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#createAccount(org.apache.lucene.gdata.storage.lucenestorage.StorageAccountWrapper)
     */
    @Override
    public void createAccount(StorageAccountWrapper account) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#createFeed(org.apache.lucene.gdata.storage.lucenestorage.StorageFeedWrapper)
     */
    @Override
    public void createFeed(StorageFeedWrapper wrapper) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#deleteAccount(java.lang.String)
     */
    @Override
    public void deleteAccount(String accountName) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#deleteEntry(org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper)
     */
    @Override
    public void deleteEntry(StorageEntryWrapper wrapper) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#deleteFeed(java.lang.String)
     */
    @Override
    public void deleteFeed(String feedId) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#forceWrite()
     */
    @Override
    protected void forceWrite() throws IOException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#insertEntry(org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper)
     */
    @Override
    public void insertEntry(StorageEntryWrapper wrapper) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#updateAccount(org.apache.lucene.gdata.storage.lucenestorage.StorageAccountWrapper)
     */
    @Override
    public void updateAccount(StorageAccountWrapper user) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#updateEntry(org.apache.lucene.gdata.storage.lucenestorage.StorageEntryWrapper)
     */
    @Override
    public void updateEntry(StorageEntryWrapper wrapper) throws StorageException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageModifier#updateFeed(org.apache.lucene.gdata.storage.lucenestorage.StorageFeedWrapper)
     */
    @Override
    public void updateFeed(StorageFeedWrapper wrapper) throws StorageException {
        
        
    }

}
