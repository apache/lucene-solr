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
 
import java.util.List; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.BaseFeed; 
import com.google.gdata.data.ExtensionProfile; 
 
/** 
 * This is the main storage interface. The Storage represents the internal 
 * server storage. It acts as a Database to persist the feed data. 
 * This inferface is not public yet!! 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public interface Storage { 
 
    /** 
     * This stores an incoming entry for a later retrival. 
     * The Entry will be associated with the feedid. 
     * @param entry - the entry 
     * @param feedId - the feedID 
     * @return - the stored Entry 
     * @throws StorageException 
     */ 
    public abstract BaseEntry storeEntry(BaseEntry entry, String feedId) 
            throws StorageException; 
 
    /** 
     * @param entryId 
     * @param feedId 
     * @throws StorageException 
     */ 
    public abstract void deleteEntry(String entryId, String feedId) 
            throws StorageException; 
 
    /** 
     * @param entry 
     * @param feedId 
     * @return  
     * @throws StorageException 
     */ 
    public abstract BaseEntry updateEntry(BaseEntry entry, String feedId) 
            throws StorageException; 
 
    /** 
     * @param feedId 
     * @param startIndex 
     * @param resultCount 
     * @return 
     * @throws StorageException 
     */ 
    public abstract BaseFeed getFeed(String feedId, int startIndex, 
            int resultCount) throws StorageException; 
 
    /** 
     * @param entryId 
     * @param feedId 
     * @return 
     * @throws StorageException 
     */ 
    public abstract BaseEntry getEntry(String entryId, String feedId) 
            throws StorageException; 
 
    /** 
     * @param entryIdList 
     * @param feedId 
     * @return 
     * @throws StorageException 
     */ 
    public abstract List<BaseEntry> getEntries(List<String> entryIdList, 
            String feedId) throws StorageException; 
 
    /** 
     * @param profile 
     */ 
    public abstract void setExtensionProfile(final ExtensionProfile profile); 
 
    /** 
     * close this storage instance 
     */ 
    public abstract void close(); 
 
} 
