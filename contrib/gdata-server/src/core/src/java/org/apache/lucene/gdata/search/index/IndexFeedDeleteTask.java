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

package org.apache.lucene.gdata.search.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * This IndexDocumentBuilder deletes a entire feed form the index the builder is
 * passed to if the feed has any entries in the search index. Each created and
 * passed IndexFeedDeleteTask forces a commit.
 * 
 * @author Simon Willnauer
 * 
 */
public class IndexFeedDeleteTask implements IndexDocumentBuilder<IndexDocument> {
    private final String feedId;

    IndexFeedDeleteTask(String feedId) {
        if (feedId == null)
            throw new IllegalArgumentException("feedId must not be null");
        this.feedId = feedId;

    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocumentBuilder#call()
     */
    public IndexDocument call() throws GdataIndexerException {
        return new FeedDeleteDocument(this.feedId);
        
    }

    private static class FeedDeleteDocument implements IndexDocument {
        private final Term deleteTerm;

        FeedDeleteDocument(String feedId) {
            this.deleteTerm = new Term(FIELD_FEED_ID, feedId);
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#isUpdate()
         */
        public boolean isUpdate() {

            return false;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#isDelete()
         */
        public boolean isDelete() {

            return true;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#isInsert()
         */
        public boolean isInsert() {

            return false;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#getWriteable()
         */
        public Document getWriteable() {

            return null;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#getDeletealbe()
         */
        public Term getDeletealbe() {

            return this.deleteTerm;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#commitAfter()
         */
        public boolean commitAfter() {
            /*
             * force commit after delete a entire feed and its entries
             */
            return true;
        }

        /**
         * @see org.apache.lucene.gdata.search.index.IndexDocument#optimizeAfter()
         */
        public boolean optimizeAfter() {

            return false;
        }

    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object obj) {
        if(obj == null)
            return false;
        if (obj instanceof IndexFeedDeleteTask) {
            IndexFeedDeleteTask other = (IndexFeedDeleteTask) obj;
            return this.feedId.equals(other.feedId);
            
        }
        return false;
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode() {
        
        return this.feedId.hashCode();
    }

}
