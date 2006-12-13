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

import java.util.ArrayList;
import java.util.Collection;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.search.analysis.ContentStrategy;
import org.apache.lucene.index.Term;

/**
 * Simple implementation
 * 
 * @author Simon Willnauer
 * @see org.apache.lucene.gdata.search.index.IndexDocument
 */
class GDataIndexDocument implements IndexDocument {
    private final IndexAction action;

    private final boolean commitAfter;

    private final boolean optimizeAfter;

    private String id;

    protected Collection<ContentStrategy> fields;

    private final String feedId;

    GDataIndexDocument(final IndexAction action, final String entryId,final String feedId,final boolean commitAfter,final boolean optimizeAfter) {
        this.action = action;
        this.id = entryId;
        this.feedId = feedId;
        this.fields = new ArrayList<ContentStrategy>(10);
        this.commitAfter = commitAfter;
        this.optimizeAfter = optimizeAfter;
    }

    /**
     * Adds a new field e.g. <tt>ContentStrategy</tt> to the IndexDocument
     * 
     * @param field -
     *            the strategy to add
     */
    public void addField(ContentStrategy field) {
        if (field == null)
            return;
        this.fields.add(field);
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#getWriteable()
     */
    public Document getWriteable() {
        Document retVal = new Document();
        retVal.add(new Field(FIELD_ENTRY_ID, this.id, Field.Store.YES,
                Field.Index.UN_TOKENIZED));
        retVal.add(new Field(FIELD_FEED_ID, this.feedId, Field.Store.YES,
                Field.Index.UN_TOKENIZED));
        for (ContentStrategy strategy : this.fields) {
            Field[] fieldArray = strategy.createLuceneField();
            for (int i = 0; i < fieldArray.length; i++) {
                retVal.add(fieldArray[i]);
            }
            
        }
        return retVal;
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#getDeletealbe()
     */
    public Term getDeletealbe() {

        return new Term(IndexDocument.FIELD_ENTRY_ID, this.id);
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#isUpdate()
     */
    public boolean isUpdate() {

        return isAction(IndexAction.UPDATE);
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#isDelete()
     */
    public boolean isDelete() {

        return isAction(IndexAction.DELETE);
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#isInsert()
     */
    public boolean isInsert() {

        return isAction(IndexAction.INSERT);
    }

    private boolean isAction(IndexAction indexAction) {
        return this.action == indexAction;
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#commitAfter()
     */
    public boolean commitAfter() {

        return this.commitAfter;
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#optimizeAfter()
     */
    public boolean optimizeAfter() {

        return this.optimizeAfter;
    }

    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(Object obj) {
        if(obj == null)
            return false;
        if(this == obj)
            return true;
        if(obj instanceof GDataIndexDocument){
            GDataIndexDocument other = (GDataIndexDocument)obj; 
            if(this.id == null)
                return false;
            return this.id.equals(other.id);
        }
        return false; 
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        if(this.id == null)
            return super.hashCode();
        return this.id.hashCode();
    }
    
    

}
