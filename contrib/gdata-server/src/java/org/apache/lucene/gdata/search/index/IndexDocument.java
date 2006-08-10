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
package org.apache.lucene.gdata.search.index;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * IndexDocument encapsulates the acual entity to store, update or delete. All
 * infomation to process the action on this document are provided via this
 * interface.
 * <p>
 * This enables the GDataIndexer to index every kind of document. All the
 * processing of the original document happens somewhere behind this facade.
 * {@link org.apache.lucene.gdata.search.index.IndexDocumentBuilderTask} passed
 * to the {@link org.apache.lucene.gdata.search.index.GDataIndexer} task queue
 * produce instances of this interface concurrently.
 * </p>
 * 
 * @author Simon Willnauer
 * 
 * 
 */
public interface IndexDocument {
    /**
     * the index field to identify a document in the index. This acts as a
     * primary key to fetch the entire entry from the storage
     */
    public static final String FIELD_ENTRY_ID = "enryId";
    /**
     * the index field to associate a document with a specific feed 
     */
    public static final String FIELD_FEED_ID = "feedId";
    public static final String GDATA_MANDATORY_FIELD_UPDATED = "updated";
    public static final String GDATA_MANDATORY_FIELD_CATEGORY = "category";

    /**
     * @return <code>true</code> if and only if this document is an update,
     *         otherwise <code>false</code>
     */
    public abstract boolean isUpdate();

    /**
     * @return <code>true</code> if and only if this document is a delete,
     *         otherwise <code>false</code>
     */
    public abstract boolean isDelete();

    /**
     * @return <code>true</code> if and only if this document is an insert,
     *         otherwise <code>false</code>
     */
    public abstract boolean isInsert();

    /**
     * 
     * @return - the lucene document to write to the index if the action is
     *         insert or updated, otherwise it will return <code>null</code>;
     */
    public abstract Document getWriteable();

    /**
     * @return - a term that identifies this document in the index to delete
     *         this document on a update or delete
     */
    public abstract Term getDeletealbe();

    /**
     * Indicates that the index should be commited after this document has been
     * processed
     * 
     * @return <code>true</code> if the index should be commited after this
     *         document, otherwise <code>false</code>
     */
    public abstract boolean commitAfter();

    /**
     * Indicates that the index should be optimized after this document has been
     * processed
     * 
     * 
     * @return <code>true</code> if the index should be optimized after this
     *         document, otherwise <code>false</code>
     */
    public abstract boolean optimizeAfter();
    
}
