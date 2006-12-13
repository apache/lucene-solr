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

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import junit.framework.TestCase;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.GDataSearcher;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.search.index.IndexController.ServiceIndex;
import org.apache.lucene.gdata.server.registry.GDataServerRegistry;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;
import org.apache.lucene.gdata.utils.ReferenceCounter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermQuery;


/**
 * @author Simon Willnauer
 * 
 */
public class TestIndexController extends TestCase {
    IndexController controller;

    IndexSchema schema;

    File indexLocation;

    protected void setUp() throws Exception {
        this.controller = new IndexController();
        GDataServerRegistry reg = GDataServerRegistry.getRegistry();

        this.indexLocation = new File(System.getProperty("java.io.tmpdir"));
        
        
        ProvidedServiceStub stub = new ProvidedServiceStub();
        this.schema = new IndexSchema();
        // must be set
        this.schema.setDefaultSearchField("content");
        this.schema.setName(ProvidedServiceStub.SERVICE_NAME);
        this.schema.setIndexLocation(this.indexLocation.getAbsolutePath());
        IndexSchemaField field = new IndexSchemaField();
        field.setName("content");
        field.setPath("/somePath");
        field.setContentType(ContentType.TEXT);
        this.schema.addSchemaField(field);
        stub.setIndexSchema(this.schema);

        reg.registerService(stub);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        GDataServerRegistry.getRegistry().destroy();
        this.controller.destroy();
        /*
         * this file will be created by the controller
         */
        File toDel = new File(this.indexLocation,
                ProvidedServiceStub.SERVICE_NAME);
        delAllFiles(toDel);
    }

    /*
     * del all created files
     */
    private void delAllFiles(File dir) {
        if (dir == null || !dir.exists())
            return;
        File[] files = dir.listFiles();
        for (int i = 0; i < files.length; i++) {
            while (!files[i].canWrite()) {
            }
            ;
            files[i].delete();
        }
        dir.delete();
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.initialize()'
     */
    public void testInitialize() {
        this.controller.initialize();
        assertTrue(this.controller.indexerMap
                .containsKey(ProvidedServiceStub.SERVICE_NAME));
        ServiceIndex bean = this.controller.indexerMap
                .get(ProvidedServiceStub.SERVICE_NAME);
        assertNotNull(bean);
        assertNotNull(bean.getIndexer());
        assertSame(this.schema, bean.getSchema());
        assertTrue(GDataServerRegistry.getRegistry().getEntryEventMediator()
                .isListenerRegistered(this.controller));

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.initialize()'
     */
    public void testInitializeValueMissing() {
        this.schema.setIndexLocation(null);
        try {

            this.controller.initialize();
            fail("missing index location");
        } catch (RuntimeException e) {
        }
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.addIndexSchema(IndexSchema)'
     */
    public void testAddIndexSchema() {
        this.controller.initialize();
        assertEquals(1, this.controller.indexerMap.size());
        try {
            this.controller.addIndexSchema(this.schema);
            fail("schema already added");
        } catch (IllegalStateException e) {

        }
        this.schema.setName(null);
        try {
            this.controller.addIndexSchema(this.schema);
            fail("schema name is null");
        } catch (IllegalStateException e) {

        }

        this.schema.setName("someOthername");
        this.controller.addIndexSchema(this.schema);
        assertEquals(2, this.controller.indexerMap.size());

    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.createIndexer(IndexSchema)'
     */
    public void testCreateIndexDirectory() throws IOException {
        File f = new File(System.getProperty("java.io.tmpdir"), "gdataindexdir"
                + System.currentTimeMillis());
        f.mkdir();
        f.deleteOnExit();
        IndexWriter w = new IndexWriter(f, new StandardAnalyzer(), true);
        Document d = new Document();
        d.add(new Field("test", "test", Field.Store.NO, Field.Index.TOKENIZED));
        w.addDocument(d);
        w.close();
        assertFalse(this.controller.createIndexDirectory(f));
        // throw away files in the directory
        delAllFiles(f);
        File f1 = new File(System.getProperty("java.io.tmpdir"), "newIndexDir"
                + System.currentTimeMillis());
        f1.mkdir();
        f1.deleteOnExit();
        assertTrue(this.controller.createIndexDirectory(f1));
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.createIndexLocation(String,
     * String)'
     */
    public void testCreateIndexLocation() throws IOException {
        File f = new File(System.getProperty("java.io.tmpdir"), "gdatadir"
                + System.currentTimeMillis());
        f.mkdir();
        f.deleteOnExit();

        assertEquals(f.getAbsolutePath(), this.controller.createIndexLocation(
                f.getParent(), f.getName()).getAbsolutePath());
        ;
        File pFile = new File(System.getProperty("java.io.tmpdir"), "gdatafile"
                + System.currentTimeMillis());
        pFile.deleteOnExit();
        try{
        this.controller.createIndexLocation(pFile.getParent(),pFile.getAbsolutePath());
        fail("can not create dir");
        }catch (RuntimeException e) {
            
        }
        assertTrue(pFile.createNewFile());
        try {
            this.controller.createIndexLocation(pFile.getParent(), pFile
                    .getName());
            fail("file is not a directory");
        } catch (IllegalArgumentException e) {

        }
        try {
            this.controller.createIndexLocation(pFile.getName(), pFile
                    .getName());
            fail("parent is not a directory");
        } catch (IllegalArgumentException e) {

        }
        try{
            this.controller.createIndexLocation(null,null);
            fail("null");
        }catch (GdataIndexerException e) {

        }
    }

    /*
     * Test method for
     * 'org.apache.lucene.gdata.search.index.IndexController.getServiceSearcher(ProvidedService)'
     */
    public void testGetServiceSearcher() {

        this.controller.initialize();
        ReferenceCounter<IndexSearcher> refCounter = this.controller.indexerMap
                .get(ProvidedServiceStub.SERVICE_NAME).getSearcher();
        GDataSearcher searcher = this.controller
                .getServiceSearcher(new ProvidedServiceStub());
        assertNotNull(searcher);
        GDataSearcher sameSearcher = this.controller
                .getServiceSearcher(new ProvidedServiceStub());
        assertSame(refCounter, this.controller.indexerMap.get(
                ProvidedServiceStub.SERVICE_NAME).getSearcher());

        this.controller.commitCallBack(ProvidedServiceStub.SERVICE_NAME);
        GDataSearcher newSearcher = this.controller
                .getServiceSearcher(new ProvidedServiceStub());
        assertNotSame(refCounter, this.controller.indexerMap.get(
                ProvidedServiceStub.SERVICE_NAME).getSearcher());

        sameSearcher.close();
        searcher.close();
        newSearcher.close();

    }

    public void testDestroy() {
        this.controller.initialize();
        try {
            this.controller.initialize();
            fail("controller is initialized");
        } catch (IllegalStateException e) {
        }
        this.controller.destroy();
        try {
            this.controller.getServiceSearcher(null);
            fail("controller is closed");
        } catch (IllegalStateException e) {
        }
        try {
            this.controller.commitCallBack("null");
            fail("controller is closed");
        } catch (IllegalStateException e) {
        }
        
        try {
            this.controller.destroy();
            fail("controller is closed");
        } catch (IllegalStateException e) {
        }   
        try {
            this.controller.addIndexSchema(null);
            fail("controller is closed");
        } catch (IllegalStateException e) {
        }
        /*
         * init again to destroy in teardown
         */
        this.controller.initialize();
    }
    
    
    public void testfireInsertEvent(){
        try{
        this.controller.fireInsertEvent(null);
        fail("not initialized");
        }catch (IllegalStateException e) {
            // TODO: handle exception
        }
        this.controller.initialize();
        ServerBaseEntry e = new ServerBaseEntry();
        e.setId("someId");
        e.setFeedId("someId");
        e.setServiceConfig(new ProvidedServiceStub());
        this.controller.fireInsertEvent(e);
    }
    
    public void testCreateNewIndexTask() throws InterruptedException, IOException{
        this.schema.setCommitAfterDocuments(1);
        this.schema.setOptimizeAfterCommit(1);
        IndexSchemaField f = new IndexSchemaField();
        f.setName("myField");
        f.setContentType(ContentType.KEYWORD);
        f.setPath("entry/id");
        this.schema.addSchemaField(f);
        this.controller.initialize();
        ServerBaseEntry e = new ServerBaseEntry();
        e.setId("someId");
        e.setFeedId("someId");
        e.setServiceConfig(new ProvidedServiceStub());
        CommitListener l = new CommitListener();
        l.createLatch(1);
        ServiceIndex sIndex = this.controller.indexerMap.get(this.schema.getName());
        sIndex.getIndexer().registerIndexEventListener(l);
        this.controller.fireInsertEvent(e);
        l.waitOnLatch();      
       
       assertEquals(1,sIndex.getIndexer().optimized.get());
       assertEquals(1,sIndex.getIndexer().committed.get());
       
       sIndex.getIndexer().removeIndexEventListener(l);
       
       
       e = new ServerBaseEntry();
       e.setId("someId");
       e.setFeedId("someId");
       e.setServiceConfig(new ProvidedServiceStub());
       l = new CommitListener();
       l.createLatch(1);
        sIndex = this.controller.indexerMap.get(this.schema.getName());
       sIndex.getIndexer().registerIndexEventListener(l);
       this.controller.fireUpdateEvent(e);
       l.waitOnLatch();      
      
      assertEquals(2,sIndex.getIndexer().optimized.get());
      assertEquals(2,sIndex.getIndexer().committed.get());
      
      GDataSearcher<String> searcher = this.controller.getServiceSearcher(e.getServiceConfig());
      List<String> results = searcher.search(new TermQuery(new Term(IndexDocument.FIELD_ENTRY_ID,"someId")),10,0,"someId");
      assertEquals(1,results.size());
      searcher.close();
      
       
        
    }
    
    static class CommitListener implements IndexEventListener{
        public CountDownLatch latch;
        
        public void createLatch(int count){
            this.latch = new CountDownLatch(count);
        }
        public void waitOnLatch() throws InterruptedException{
            if(latch != null)
            latch.await();
        }

        public void commitCallBack(String service) {
            if(this.latch != null)
            this.latch.countDown();
        }
        
    }

}
