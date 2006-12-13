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

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Hits;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

/**
 * @author Simon Willnauer
 *
 */
public class TestGdataIndexer extends TestCase {
    private GDataIndexer indexer;

    private Directory dir;

    private static IndexSchema config;

    private static String FIELD_ID = IndexDocument.FIELD_ENTRY_ID;

    static {
        config = new IndexSchema();
        config.setName("testService");
        config.setCommitLockTimeout(-1);
        config.setServiceAnalyzer(new StandardAnalyzer());
        config.setMaxBufferedDocs(-1);
        config.setMaxFieldLength(-1);
        config.setMaxMergeDocs(-1);
        config.setWriteLockTimeout(-1);
        config.setMergeFactor(-1);
    }

    /**
     * @see junit.framework.TestCase#setUp()
     */
    @Override
    protected void setUp() throws Exception {
        this.dir = new RAMDirectory();
        this.indexer = GDataIndexer.createGdataIndexer(config, this.dir, true);
        super.setUp();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    @Override
    protected void tearDown() throws Exception {
        this.indexer.destroy();
    }
    public void testStaticFactoryMethodes() throws IOException{
        GDataIndexer i =GDataIndexer.createGdataIndexer(config, new RAMDirectory(), true);
        assertNotNull(i);
        assertEquals(IndexTask.class,i.indexTask.getClass());
        
        i =GDataIndexer.createTimedGdataIndexer(config, new RAMDirectory(), true,60);
        assertNotNull(i);
        assertEquals(TimedIndexTask.class,i.indexTask.getClass());
        assertEquals(60,((TimedIndexTask)i.indexTask).getIdleTime());
        
        i.destroy();
    }
    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.GDataIndexer(IndexServiceConfiguration,
     * Directory, boolean)'
     */
    public void testGDataIndexer() throws InterruptedException, IOException {
        try {
            new GDataIndexer(null, dir, true);
            fail("config is null");
        } catch (IllegalArgumentException e) {
            //
        }

        try {
            new GDataIndexer(config, null, true);
            fail("dir is null");
        } catch (IllegalArgumentException e) {
            //
        }
        
        GDataIndexer in = new GDataIndexer(config,new RAMDirectory(),true);
        in.setIndexTask(null);
        in.init();
        assertNotNull(in.indexTask);
        assertEquals(IndexTask.class,in.indexTask.getClass());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.addIndexableDocumentTask(Future<IndexDocument>)'
     */
    public void testAddIndexableDocumentTask() throws InterruptedException,
            IOException {
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm = new Term(FIELD_ID, id);
        /*
         * Latch will be decremented in FutureStub#get() and
         * IndexDocumentStub#getIndexable
         */
        CountDownLatch l = new CountDownLatch(2);
        IndexDocument iDoc = new IndexDocumentStub(doc, delTerm,
                IndexAction.INSERT, l);
        Future<IndexDocument> future = new FutureStub<IndexDocument>(iDoc, l);

        this.indexer.addIndexableDocumentTask(future);
        // wait for the latch do decrement
        l.await(5000, TimeUnit.MILLISECONDS);

        this.indexer.commit(false);
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        // test for update
        /*
         * Latch will be decremented in FutureStub#get() and
         * IndexDocumentStub#getIndexable
         */
        l = new CountDownLatch(2);
        iDoc = new IndexDocumentStub(doc, delTerm, IndexAction.UPDATE, l);
        future = new FutureStub<IndexDocument>(iDoc, l);
        this.indexer.addIndexableDocumentTask(future);
        l.await(5000, TimeUnit.MILLISECONDS);
        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();

        // test for delete
        /*
         * Latch will be decremented in FutureStub#get()
         */
        l = new CountDownLatch(1);
        iDoc = new IndexDocumentStub(doc, delTerm, IndexAction.DELETE, l);
        future = new FutureStub<IndexDocument>(iDoc, l);

        this.indexer.addIndexableDocumentTask(future);
        /*
         * wait for the indexer task to add the deleted
         */
        while (this.indexer.docsDeleted.get() == 0)
            l.await(5000, TimeUnit.MILLISECONDS);

        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(0, h.length());
        s.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.addDocument(IndexDocument)'
     */
public void testAddDocument() throws IOException {
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm =  new Term(FIELD_ID, id);
        IndexDocument iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
        
        this.indexer.addDocument(iDoc);
        assertEquals(1,this.indexer.docsAdded.get());
        assertEquals(0,this.indexer.docsDeleted.get());
        assertEquals(0,this.indexer.docsUpdated.get());
        this.indexer.addDocument(iDoc);
        this.indexer.commit(false);
        
        
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.UPDATE);
        try{
            this.indexer.addDocument(iDoc);
            fail("document has not insert action ");
        }catch (GdataIndexerException e) {
            
        }
        
    }
    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.updateDocument(IndexDocument)'
     */
    public void testUpdateDocument() throws IOException {
        
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm =  new Term(FIELD_ID, id);
        IndexDocument iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
        /*
         * write doc to index
         */
        this.indexer.writer.addDocument(doc);
        this.indexer.closeWriter();
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        String testFieldName = "someTestFieldupdate";
        doc.add(new Field(testFieldName,"someText",Field.Store.YES,Field.Index.TOKENIZED));
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.UPDATE);
        /*
         * updateDoc via indexer 
         */
        this.indexer.updateDocument(iDoc);
        assertEquals(0,this.indexer.docsAdded.get());
        assertEquals(0,this.indexer.docsDeleted.get());
        assertEquals(1,this.indexer.docsUpdated.get());
        
        this.indexer.commit(false);
        
        
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        assertNotNull(h.doc(0).getField(testFieldName));
        s.close();
        
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        try{
            this.indexer.updateDocument(iDoc);
            fail("document has not update action ");
        }catch (GdataIndexerException e) {
            
        }
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.deleteDocument(IndexDocument)'
     */
    public void testDeleteDocument() throws IOException {
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm =  new Term(FIELD_ID, id);
        IndexDocument iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
        /*
         * write doc to index
         */
        this.indexer.writer.addDocument(doc);
       
        this.indexer.closeWriter();
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        
        /*
         * del doc via indexer
         */
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        this.indexer.deleteDocument(iDoc);
        assertEquals(0,this.indexer.docsAdded.get());
        assertEquals(1,this.indexer.docsDeleted.get());
        assertEquals(0,this.indexer.docsUpdated.get());
        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(0, h.length());
        s.close();
        
        /*
         * test insert / del without optimize
         */ 
        iDoc = new IndexDocumentStub(doc,delTerm,
                 IndexAction.INSERT);
        this.indexer.addDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        this.indexer.deleteDocument(iDoc);
        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(0, h.length());
        s.close();
        
        
        
        /*
         * test insert / del / update without optimize
         */ 
        iDoc = new IndexDocumentStub(doc,delTerm,
                 IndexAction.INSERT);
        this.indexer.addDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        this.indexer.deleteDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
       this.indexer.addDocument(iDoc);
        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        
        
        
        
        /*
         * test insert / update / del without optimize
         */
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
        this.indexer.addDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.UPDATE);
        this.indexer.updateDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        this.indexer.deleteDocument(iDoc);
        this.indexer.commit(false);
        s = new IndexSearcher(this.dir);
        h = s.search(new TermQuery(delTerm));
        assertEquals(0, h.length());
        s.close();
        
        
        
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.UPDATE);
        try{
            this.indexer.deleteDocument(iDoc);
            fail("document has not delete action ");
        }catch (GdataIndexerException e) {
            
        }
        
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.commit(boolean)'
     */
    public void testCommit() throws IOException {
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm =  new Term(FIELD_ID, id);
        IndexDocument iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.INSERT);
        this.indexer.addDocument(iDoc);
         iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.UPDATE);
        this.indexer.updateDocument(iDoc);
        this.indexer.updateDocument(iDoc);
        iDoc = new IndexDocumentStub(doc,delTerm,
                IndexAction.DELETE);
        this.indexer.deleteDocument(iDoc);
        IndexEventListenerStub evListener = new IndexEventListenerStub();
        this.indexer.registerIndexEventListener(evListener);
        assertEquals(1,this.indexer.docsAdded.get());
        assertEquals(1,this.indexer.docsDeleted.get());
        assertEquals(2,this.indexer.docsUpdated.get());
        assertEquals(0,evListener.getCalledCount());
        this.indexer.commit(true);
        this.indexer.commit(false);
        assertEquals(1,evListener.getCalledCount());
        assertEquals(0,this.indexer.docsAdded.get());
        assertEquals(0,this.indexer.docsDeleted.get());
        assertEquals(0,this.indexer.docsUpdated.get());
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(0, h.length());
        s.close();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.registerIndexEventListener(IndexEventListener)'
     */
    public void testRegisterIndexEventListener() {
        IndexEventListenerStub evListener = new IndexEventListenerStub();
        this.indexer.registerIndexEventListener(evListener);
        this.indexer.registerIndexEventListener(evListener);
        assertEquals(0,evListener.getCalledCount());
        this.indexer.notifyCommitListeners("someId");
        this.indexer.notifyCommitListeners("someId");
        assertEquals(2,evListener.getCalledCount());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.removeIndexEventListener(IndexEventListener)'
     */
    public void testRemoveIndexEventListener() {
        IndexEventListenerStub evListener = new IndexEventListenerStub();
        this.indexer.registerIndexEventListener(evListener);
        assertEquals(0,evListener.getCalledCount());
        this.indexer.notifyCommitListeners("someId");
        assertEquals(1,evListener.getCalledCount());
        this.indexer.removeIndexEventListener(evListener);
        this.indexer.removeIndexEventListener(evListener);
        this.indexer.notifyCommitListeners("someId");
        assertEquals(1,evListener.getCalledCount());
        
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.notifyCommitListeners(String)'
     */
    public void testNotifyCommitListeners() {
        IndexEventListenerStub evListener = new IndexEventListenerStub();
        IndexEventListenerStub evListener1 = new IndexEventListenerStub();
        IndexEventListenerStub evListener2 = new IndexEventListenerStub();
        this.indexer.registerIndexEventListener(evListener);
        this.indexer.registerIndexEventListener(evListener1);
        this.indexer.registerIndexEventListener(evListener2);
        assertEquals(0,evListener.getCalledCount());
        this.indexer.notifyCommitListeners("someId");
        assertEquals(1,evListener.getCalledCount());
        assertEquals(1,evListener1.getCalledCount());
        assertEquals(1,evListener2.getCalledCount());
        this.indexer.removeIndexEventListener(evListener);
        this.indexer.notifyCommitListeners("someId");
        assertEquals(1,evListener.getCalledCount());
        assertEquals(2,evListener1.getCalledCount());
        assertEquals(2,evListener2.getCalledCount());
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.closeWriter()'
     */
    public void testCloseWriter() throws IOException{
        assertNotNull(this.indexer.writer);
        this.indexer.closeWriter();
        assertNull(this.indexer.writer);

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.closeSearcher()'
     */
    public void testCloseSearcher() throws IOException {
        this.indexer.openSearcher();
        assertNotNull(this.indexer.searcher);
        this.indexer.closeSearcher();
        assertNull(this.indexer.searcher);
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.openSearcher()'
     */
    public void testOpenSearcher() throws IOException {
        this.indexer.searcher = null;
        this.indexer.openSearcher();
         assertNotNull(this.indexer.searcher);
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.openWriter()'
     */
    public void testOpenWriter() throws IOException {
        this.indexer.closeWriter();
        assertNull(this.indexer.writer);
       this.indexer.openWriter();
        assertNotNull(this.indexer.writer);
    }


    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.GDataIndexer.destroy()'
     */
    public void testDestroy() throws InterruptedException, IOException {
        this.indexer.destroy();
        String id = "myID";
        Field f = new Field(FIELD_ID, id, Field.Store.YES,
                Field.Index.UN_TOKENIZED);
        Document doc = new Document();
        doc.add(f);
        Term delTerm = new Term(FIELD_ID, id);
        IndexDocument iDoc = new IndexDocumentStub(doc, delTerm,
                IndexAction.INSERT);
        Future<IndexDocument> future = new FutureStub<IndexDocument>(iDoc);
        try{
        this.indexer.addIndexableDocumentTask(future);
        fail("indexer already closed exc. expected");
        }catch (IllegalStateException e) {}
        this.indexer = GDataIndexer.createGdataIndexer(config, dir, true);
        CountDownLatch documentLatch = new CountDownLatch(1);
        iDoc = new IndexDocumentStub(doc, delTerm,
                IndexAction.INSERT,documentLatch);
        
        CountDownLatch latch = new CountDownLatch(1);
        future = new FutureStub<IndexDocument>(iDoc,latch,true);
        this.indexer.addIndexableDocumentTask(future);
        this.indexer.destroy();
        latch.countDown();
        documentLatch.await(5000,TimeUnit.MILLISECONDS);
        // wait active for the commit
        while(this.indexer.writer != null){}
        
        IndexSearcher s = new IndexSearcher(this.dir);
        Hits h = s.search(new TermQuery(delTerm));
        assertEquals(1, h.length());
        s.close();
        
        
    }
    
    public void testInnerClassFuture() throws InterruptedException, ExecutionException, TimeoutException{
        Future f = new GDataIndexer.FinishingFuture();
        assertNull(f.get());
        assertNull(f.get(0,TimeUnit.MICROSECONDS));
        assertTrue(f.isDone());
        assertFalse(f.isCancelled());
        assertFalse(f.cancel(true));
        
    }

   

}
