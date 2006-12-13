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

import java.util.concurrent.CountDownLatch;

import org.apache.lucene.document.Document;
import org.apache.lucene.index.Term;

/**
 * @author Simon Willnauer
 *
 */
public class IndexDocumentStub implements IndexDocument {
    Document document;
    Term deleteTerm;
    IndexAction action;
    CountDownLatch latch;
    boolean commitAfter;
    boolean optimizeAfter;
    /**
     * 
     */
    public IndexDocumentStub(Document doc, Term deleteTerm, IndexAction action, CountDownLatch latch) {
        this.document = doc;
        this.deleteTerm = deleteTerm;
        this.action = action;
        this.latch = latch;
    }
    public IndexDocumentStub(Document doc, Term deleteTerm, IndexAction action) {
        this(doc,deleteTerm,action,null);
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
    private boolean isAction(IndexAction currentAction){
        return this.action == currentAction;
    }
    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#getWriteable()
     */
    public Document getWriteable() {
        if(latch != null)
        latch.countDown();        
        return this.document;
    }

    /**
     * @see org.apache.lucene.gdata.search.index.IndexDocument#getDeletealbe()
     */
    public Term getDeletealbe() {
        if(latch != null)
            latch.countDown();
        return this.deleteTerm;
    }
    public boolean commitAfter() {
        
        return this.commitAfter;
    }
    public boolean optimizeAfter() {
        
        return this.optimizeAfter;
    }
    /**
     * @param commitAfter The commitAfter to set.
     */
    public void setCommitAfter(boolean commitAfter) {
        this.commitAfter = commitAfter;
    }
    /**
     * @param optimizeAfter The optimizeAfter to set.
     */
    public void setOptimizeAfter(boolean optimizeAfter) {
        this.optimizeAfter = optimizeAfter;
    }
    
    /**
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public final boolean equals(Object obj) {
        if(obj == null)
            return false;
        if(obj instanceof IndexDocumentStub){
            IndexDocumentStub other = (IndexDocumentStub)obj;
            return this.document.getField(IndexDocument.FIELD_ENTRY_ID).stringValue().equals(other.document.getField(IndexDocument.FIELD_ENTRY_ID).stringValue());
              
        }
        return false; 
    }

    /**
     * @see java.lang.Object#hashCode()
     */
    @Override
    public final int hashCode() {
        return this.document.getField(IndexDocument.FIELD_ENTRY_ID).stringValue().hashCode();
    }

}
