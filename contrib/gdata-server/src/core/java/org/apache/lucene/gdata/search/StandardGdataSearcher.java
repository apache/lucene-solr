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

package org.apache.lucene.gdata.search;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.search.index.IndexDocument;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryFilter;
import org.apache.lucene.search.TermQuery;

/**
 * Standard implementation of
 * {@link org.apache.lucene.gdata.search.GDataSearcher}
 * 
 * @author Simon Willnauer
 * 
 */
public class StandardGdataSearcher implements GDataSearcher<String> {
    private final AtomicBoolean isClosed = new AtomicBoolean(false);

    private final ReferenceCounter<IndexSearcher> searcher;

    private static final Map<String, QueryFilter> queryFilterCache = new HashMap<String, QueryFilter>();

    /** constructs a new GdataSearcher
     * @param searcher - the current lucene searcher instance
     */
    public StandardGdataSearcher(ReferenceCounter<IndexSearcher> searcher) {
        if (searcher == null)
            throw new IllegalArgumentException("searcher must not be null");

        this.searcher = searcher;

    }

    /**
     * @see org.apache.lucene.gdata.search.GDataSearcher#search(org.apache.lucene.search.Query,
     *      int, int, java.lang.String)
     */
    public List<String> search(Query query, int hitcount, int offset,
            String feedId) throws IOException {
        if (hitcount < 0 || offset < 0)
            throw new IllegalArgumentException(
                    "hit count and offset must not be less than 0");
        if (this.isClosed.get())
            throw new IllegalStateException("Searcher is closed");
        if (query == null)
            throw new RuntimeException("query is null can not apply search");
        if (feedId == null)
            throw new IllegalArgumentException("feed id must not be null");
        QueryFilter filter = null;
        synchronized (queryFilterCache) {
            filter = queryFilterCache.get(feedId);
        
        if (filter == null)
            filter = new QueryFilter(new TermQuery(new Term(
                    IndexDocument.FIELD_FEED_ID, feedId)));
            queryFilterCache.put(feedId, filter);
        }
        IndexSearcher indexSearcher = this.searcher.get();
        Hits hits = indexSearcher.search(query, filter);
        
        return collectHits(hits, hitcount, offset);

    }

    protected List<String> collectHits(Hits hits, int hitcount, int offset)
            throws IOException {
        int hitLength = hits.length();
        if (hitLength < offset || hitLength == 0)
            return new ArrayList<String>(0);
        if (offset > 0)
            --offset;
        /*
         * include the offset
         */
        int remainingHits = hitLength - offset;
        int returnSize = remainingHits > hitcount ? hitcount : remainingHits;
        List<String> retVal = new ArrayList<String>(returnSize);
        for (int i = 0; i < returnSize; i++) {
            Document doc = hits.doc(offset++);
            /*
             * the entry id is sufficient to retrieve the entry from the
             * storage. the result will be ordered by score (default)
             */
            Field field = doc.getField(IndexDocument.FIELD_ENTRY_ID);
            retVal.add(i, field.stringValue());
        }
        return retVal;

    }

    /**
     * @see org.apache.lucene.gdata.search.GDataSearcher#close()
     */
    public void close() {
        this.isClosed.set(true);
        this.searcher.decrementRef();

    }

    static void flushFilterCache() {
        synchronized (queryFilterCache) {
            queryFilterCache.clear();
        }
        
    }

}
