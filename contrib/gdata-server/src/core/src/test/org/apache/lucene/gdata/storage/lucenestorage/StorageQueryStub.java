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
import java.util.List;

import org.apache.lucene.gdata.data.GDataAccount;
import org.apache.lucene.gdata.server.registry.ProvidedService;
import org.apache.lucene.gdata.storage.StorageException;
import org.apache.lucene.search.Searcher;

import com.google.gdata.data.BaseEntry;
import com.google.gdata.data.BaseFeed;
import com.google.gdata.util.ParseException;

/**
 * @author Simon Willnauer
 *
 */
public class StorageQueryStub extends StorageQuery {

    public boolean booleanReturn = true;

    /**
     * @param buffer
     * @param searcher
     */
    public StorageQueryStub(StorageBuffer buffer, Searcher searcher) {
        super(buffer, searcher);
        // TODO Auto-generated constructor stub
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#checkEntryVersion(java.lang.String, java.lang.String, int)
     */
    @Override
    protected boolean checkEntryVersion(String id, String feedId, int version) throws IOException {
        
        return booleanReturn;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#close()
     */
    @Override
    public void close() throws IOException {
        
        super.close();
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#entryQuery(java.util.List, java.lang.String, org.apache.lucene.gdata.server.registry.ProvidedService)
     */
    @Override
    public List<BaseEntry> entryQuery(List<String> entryIds, String feedId, ProvidedService config) throws IOException, ParseException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getAccountNameForFeedId(java.lang.String)
     */
    @Override
    public String getAccountNameForFeedId(String feedId) throws IOException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getEntryLastModified(java.lang.String, java.lang.String)
     */
    @Override
    protected long getEntryLastModified(String entryId, String feedId) throws IOException, StorageException {
        
        return System.currentTimeMillis();
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getFeedLastModified(java.lang.String)
     */
    @Override
    protected long getFeedLastModified(String feedId) throws IOException {
        
        return System.currentTimeMillis();
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getLatestFeedQuery(java.lang.String, int, int, org.apache.lucene.gdata.server.registry.ProvidedService)
     */
    @Override
    public BaseFeed getLatestFeedQuery(String feedId, int resultCount, int startIndex, ProvidedService config) throws IOException, ParseException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getService(java.lang.String)
     */
    @Override
    public String getService(String feedID) throws IOException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#getUser(java.lang.String)
     */
    @Override
    public GDataAccount getUser(String username) throws IOException {
        
        return null;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#isEntryStored(java.lang.String, java.lang.String)
     */
    @Override
    protected boolean isEntryStored(String entryId, String feedId) throws IOException {
        
        return booleanReturn;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#isFeedStored(java.lang.String)
     */
    @Override
    public boolean isFeedStored(String feedId) throws IOException {
        
        return booleanReturn;
    }

    /**
     * @see org.apache.lucene.gdata.storage.lucenestorage.StorageQuery#singleEntryQuery(java.lang.String, java.lang.String, org.apache.lucene.gdata.server.registry.ProvidedService)
     */
    @Override
    public BaseEntry singleEntryQuery(String entryId, String feedId, ProvidedService config) throws IOException, ParseException {
        
        return null;
    }

}
