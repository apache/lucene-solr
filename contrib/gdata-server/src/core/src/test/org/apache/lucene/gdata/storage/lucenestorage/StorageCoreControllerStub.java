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

package org.apache.lucene.gdata.storage.lucenestorage;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;

import org.apache.lucene.gdata.server.registry.Component;
import org.apache.lucene.gdata.server.registry.ComponentType;
import org.apache.lucene.gdata.storage.IDGenerator;
import org.apache.lucene.gdata.storage.Storage;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.IndexModifier;
import org.apache.lucene.store.Directory;

/**
 * @author Simon Willnauer
 *
 */
@Component(componentType = ComponentType.STORAGECONTROLLER)
public class StorageCoreControllerStub extends StorageCoreController {
    private final IDGenerator idGenerator;


    public StorageCoreControllerStub() throws IOException, StorageException {
        try{
            this.idGenerator = new IDGenerator(5);
        }catch (NoSuchAlgorithmException e) {
            throw new StorageException(e);
        }
       
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#createIndexModifier()
     */
    @Override
    protected IndexModifier createIndexModifier() throws IOException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#forceWrite()
     */
    @Override
    public void forceWrite() throws IOException {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getDirectory()
     */
    @Override
    protected Directory getDirectory() {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getBufferSize()
     */
    @Override
    public int getBufferSize() {
        
        return 1;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getStorageModifier()
     */
    @Override
    protected StorageModifier getStorageModifier() {
        
        try {
            return new StorageModifierStub();
        } catch (IOException e) {
            
            e.printStackTrace();
        } catch (StorageException e) {
            
            e.printStackTrace();
        }
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getPersistFactor()
     */
    @Override
    public int getPersistFactor() {
        
        return 1;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getStorageQuery()
     */
    @Override
    protected ReferenceCounter<StorageQuery> getStorageQuery() {
        
        ReferenceCounter<StorageQuery> retVal =  new ReferenceCounter<StorageQuery>(new StorageQueryStub(null,null)){

            @Override
            protected void close() {
                //
            }
            
        };
        retVal.increamentReference();
        retVal.increamentReference();
        return retVal;
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#registerNewStorageQuery()
     */
    @Override
    protected void registerNewStorageQuery() throws IOException {
        
       
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#releaseId()
     */
    @Override
    public synchronized String releaseId() throws StorageException {
        
        try {
            return this.idGenerator.getUID();
        } catch (InterruptedException e) {
            
          throw new StorageException(e);
        } 
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#releaseNewStorageBuffer()
     */
    @Override
    protected StorageBuffer releaseNewStorageBuffer() {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#setBufferSize(int)
     */
    @Override
    public void setBufferSize(int storageBufferSize) {
        
        
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#setPersistFactor(int)
     */
    @Override
    public void setPersistFactor(int storagePersistFactor) {
        
       
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#destroy()
     */
    @Override
    public void destroy() {
        
        this.idGenerator.stopIDGenerator();
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getStorage()
     */
    @Override
    public Storage getStorage() throws StorageException {
        
        return new StorageImplementation();
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#initialize()
     */
    @Override
    public void initialize() {
//        this.setStorageDir(new RAMDirectory());
//        super.initialize();
    }

   
}
