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
import java.io.StringReader; 
import java.util.ArrayList; 
import java.util.Iterator; 
import java.util.List; 
 
import org.apache.lucene.document.Document; 
import org.apache.lucene.gdata.server.FeedNotFoundException; 
import org.apache.lucene.gdata.server.GDataEntityBuilder; 
import org.apache.lucene.index.Term; 
import org.apache.lucene.search.BooleanClause; 
import org.apache.lucene.search.BooleanQuery; 
import org.apache.lucene.search.Hit; 
import org.apache.lucene.search.Hits; 
import org.apache.lucene.search.Searcher; 
import org.apache.lucene.search.Sort; 
import org.apache.lucene.search.SortField; 
import org.apache.lucene.search.TermQuery; 
import org.apache.lucene.search.BooleanClause.Occur; 
 
import com.google.gdata.data.BaseEntry; 
import com.google.gdata.data.ExtensionProfile; 
import com.google.gdata.util.ParseException; 
 
/** 
 * StorageQuery wrapps a Lucene {@link org.apache.lucene.search.IndexSearcher} 
 * and a {@link org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer} to 
 * perform all request on the lucene storage. 
 * The wrapped components are thread - safe. 
 * <p> 
 * An instance of this class will serve all client requests. To obtain the 
 * current instance of the {@link StorageQuery} the method 
 * {@link org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController#getStorageQuery()} 
 * has to be invoked. This method will release the current StorageQuery. 
 * </p> 
 * @see org.apache.lucene.search.IndexSearcher 
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageCoreController 
 * @see org.apache.lucene.gdata.storage.lucenestorage.StorageBuffer 
 *  
 * @author Simon Willnauer 
 *  
 */ 
public class StorageQuery { 
    private final StorageBuffer buffer; 
 
    private final Searcher searcher; 
 
    /* 
     * Sort the result by timestamp desc 
     */ 
    private final Sort timeStampSort = new Sort(new SortField(StorageEntryWrapper.FIELD_TIMESTAMP, 
            SortField.STRING, true)); 
 
    /** 
     * Creates a new StorageQuery 
     *  
     * @param buffer - 
     *            the buffer instance to get the buffered inserts, updates from. 
     * @param searcher - 
     *            the searcher instance to use to query the storage index. 
     *  
     *  
     */ 
    protected StorageQuery(final StorageBuffer buffer, final Searcher searcher) { 
 
        this.buffer = buffer; 
        this.searcher = searcher; 
 
    } 
 
    private Hits storageQuery(List<String> entryId) throws IOException { 
        BooleanQuery query = new BooleanQuery(); 
        /* 
         * query the index using a BooleanQuery 
         */ 
        for (String id : entryId) { 
            TermQuery termQuery = new TermQuery(new Term( 
                    StorageEntryWrapper.FIELD_ENTRY_ID, id)); 
            // use an OR query 
            query.add(new BooleanClause(termQuery, Occur.SHOULD)); 
        } 
 
        return this.searcher.search(query, new ModifiedEntryFilter(this.buffer 
                .getExculdList())); 
    } 
 
    /* 
     * query the storage index for a entire feed. 
     */ 
    private Hits storageFeedQuery(final String feedId, final Sort sort) 
            throws IOException { 
        TermQuery query = new TermQuery(new Term(StorageEntryWrapper.FIELD_FEED_ID, feedId)); 
        return this.searcher.search(query, new ModifiedEntryFilter(this.buffer 
                .getExculdList()), sort); 
 
    } 
 
    /* 
     * get a single entry 
     */ 
    private Hits storageQuery(String entryId) throws IOException { 
        TermQuery termQuery = new TermQuery(new Term( 
                StorageEntryWrapper.FIELD_ENTRY_ID, entryId)); 
        /* 
         * Filter entries inside the buffer, buffered entries might contain 
         * deleted entries. These entries must be found!! 
         */ 
        return this.searcher.search(termQuery, new ModifiedEntryFilter( 
                this.buffer.getExculdList())); 
 
    } 
 
    /** 
     * This method fetches the latest feed entries from the storage. Feed 
     * ususaly requested via a search query or as a simple query to the REST 
     * interface. 
     * <p> 
     * The REST interface requestes all the entries from a Storage. The Storage 
     * retrieves the entries corresponding to the parameters specified. This 
     * method first requests the latest entries or updated entries from the 
     * {@link StorageBuffer}. If the buffer already contains enought entries 
     * for the the specified result count the entires will be returned. If not, 
     * the underlaying lucene index will be searcher for all documents of the 
     * specified feed sorted by storing timestamp desc. 
     * </p> 
     * <p> 
     * The entries will be searched in a feed context specified by the given 
     * feed ID 
     * </p> 
     *  
     *  
     * @param feedId - 
     *            the requested feed, this id will be used to retrieve the 
     *            entries. 
     * @param resultCount - 
     *            how many entries are requested 
     * @param startIndex - 
     *            the offset of the entriy to start from. 
     * @param profil - 
     *            the extension profile used to create the entriy instances 
     * @return - an ordered list of {@link BaseEntry} objects, or an empty list 
     *         if no entries could be found 
     * @throws IOException - 
     *             if the index could not be queries or the entries could not be 
     *             build 
     * @throws FeedNotFoundException - 
     *             if the requested feed is not registered 
     * @throws ParseException - 
     *             if an entry could not be parsed while building it from the 
     *             Lucene Document. 
     */ 
    // TODO check input parameter 
    public List<BaseEntry> getLatestFeedQuery(final String feedId, 
            final int resultCount, final int startIndex, 
            final ExtensionProfile profil) throws IOException, 
            FeedNotFoundException, ParseException { 
        List<BaseEntry> returnList = new ArrayList<BaseEntry>(resultCount); 
        List<StorageEntryWrapper> bufferedWrapperList = this.buffer 
                .getSortedEntries(feedId); 
        int alreadyAdded = 0; 
        int offset = startIndex - 1; 
        if (bufferedWrapperList != null 
                && bufferedWrapperList.size() >= startIndex) { 
 
            for (; alreadyAdded < resultCount; alreadyAdded++) { 
                if ((bufferedWrapperList.size() - offset) > 0) { 
                    StorageEntryWrapper wrappedEntry = bufferedWrapperList 
                            .get(offset++); 
                    returnList.add(wrappedEntry.getEntry()); 
                } else 
                    break; 
            } 
            // reset offset 
            offset = startIndex - 1; 
            if (alreadyAdded == resultCount) 
                return returnList; 
        } else { 
            /* 
             * if the buffersize is less than the startindex the buffersize must 
             * be considered. Sublists would not be a repeatable read part of 
             * the whole list 
             */ 
            if (bufferedWrapperList != null) 
                offset = startIndex - 1 - bufferedWrapperList.size(); 
        } 
 
        Hits hits = storageFeedQuery(feedId, this.timeStampSort); 
        if (hits.length() > 0) { 
 
            for (; (offset < hits.length()) && (alreadyAdded < resultCount); offset++, alreadyAdded++) { 
                Document doc = hits.doc(offset); 
                BaseEntry entry = buildEntryFromLuceneDocument(doc, profil); 
                returnList.add(entry); 
            } 
 
        } 
        return returnList; 
    } 
 
    /** 
     * This method retrieves a single entry from the storage. If the 
     * {@link StorageBuffer} does not contain the requested entry the 
     * underlaying storage index will be searched. 
     * <p> 
     * The Entry will be searched in a feed context specified by the given feed 
     * ID 
     * </p> 
     *  
     * @param entryId - 
     *            the entry to fetch 
     * @param feedId - 
     *            the feedid eg. feed context 
     * @param profil - 
     *            the extension profile used to create the entriy instances 
     * @return - the requested {@link BaseEntry} or <code>null</code> if the 
     *         entry can not be found 
     * @throws IOException - 
     *             if the index could not be queries or the entries could not be 
     *             build 
     * @throws FeedNotFoundException - 
     *             if the requested feed is not registered 
     * @throws ParseException - 
     *             if an entry could not be parsed while building it from the 
     *             Lucene Document. 
     */ 
    public BaseEntry singleEntryQuery(final String entryId, 
            final String feedId, final ExtensionProfile profil) 
            throws IOException, FeedNotFoundException, ParseException { 
        StorageEntryWrapper wrapper = this.buffer.getEntry(entryId, feedId); 
 
        if (wrapper == null) { 
            Hits hits = storageQuery(entryId); 
            if (hits.length() <= 0) 
                return null; 
            Document doc = hits.doc(0); 
 
            return buildEntryFromLuceneDocument(doc, profil); 
        } 
        return wrapper.getEntry(); 
 
    } 
 
    /** 
     * Fetches the requested entries from the storage. The given list contains 
     * entry ids to be looked up in the storage. First the {@link StorageBuffer} 
     * will be queried for the entry ids. If not all of the entries remain in 
     * the buffer the underlaying lucene index will be searched. The entries are 
     * not guaranteed to be in the same order as they are in the given id list. 
     * Entry ID's not found in the index or the buffer will be omitted. 
     * <p> 
     * The entries will be searched in a feed context specified by the given 
     * feed ID 
     * </p> 
     *  
     * @param entryIds - 
     *            the entriy ids to fetch. 
     * @param feedId - 
     *            the feed id eg. feed context. 
     * @param profil - 
     *            the extension profile used to create the entry instances. 
     * @return - the list of entries corresponding to the given entry id list. 
     * @throws IOException - 
     *             if the index could not be queries or the entries could not be 
     *             build 
     * @throws FeedNotFoundException - 
     *             if the requested feed is not registered 
     * @throws ParseException - 
     *             if an entry could not be parsed while building it from the 
     *             Lucene Document. 
     */ 
    public List<BaseEntry> entryQuery(List<String> entryIds, 
            final String feedId, final ExtensionProfile profil) 
            throws IOException, FeedNotFoundException, ParseException { 
        List<BaseEntry> resultList = new ArrayList<BaseEntry>(entryIds.size()); 
        List<String> searchList = new ArrayList<String>(entryIds.size()); 
        for (String entry : entryIds) { 
 
            StorageEntryWrapper bufferedEntry = this.buffer.getEntry(entry, 
                    feedId); 
            if (bufferedEntry != null) { 
                resultList.add(bufferedEntry.getEntry()); 
            } else 
                searchList.add(entry); 
        } 
        if (searchList.isEmpty()) 
            return resultList; 
 
        Hits hits = storageQuery(searchList); 
        Iterator hitIterator = hits.iterator(); 
        while (hitIterator.hasNext()) { 
            Hit hit = (Hit) hitIterator.next(); 
            Document doc = hit.getDocument(); 
            BaseEntry entry = buildEntryFromLuceneDocument(doc, profil); 
            resultList.add(entry); 
 
        } 
 
        return resultList; 
 
    } 
 
    private BaseEntry buildEntryFromLuceneDocument(final Document doc, 
            final ExtensionProfile profil) throws FeedNotFoundException, 
            ParseException, IOException { 
        StringReader reader = new StringReader(doc.getField(StorageEntryWrapper.FIELD_CONTENT) 
                .stringValue()); 
        return GDataEntityBuilder.buildEntry(doc.getField(StorageEntryWrapper.FIELD_FEED_ID) 
                .stringValue(), reader, profil); 
 
    } 
 
    /** 
     * Closes all resources used in the {@link StorageQuery}. The instance can 
     * not be reused after invoking this method. 
     *  
     * @throws IOException - 
     *             if the resouces can not be closed 
     */ 
    public void close() throws IOException { 
        this.searcher.close(); 
        this.buffer.close(); 
    } 
 
} 
