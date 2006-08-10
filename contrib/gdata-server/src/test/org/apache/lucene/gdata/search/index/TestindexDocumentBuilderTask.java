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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import junit.framework.TestCase;

import org.apache.lucene.gdata.data.ServerBaseEntry;
import org.apache.lucene.gdata.search.config.IndexSchema;
import org.apache.lucene.gdata.search.config.IndexSchemaField;
import org.apache.lucene.gdata.search.config.IndexSchemaField.ContentType;
import org.apache.lucene.gdata.utils.ProvidedServiceStub;

import com.google.gdata.data.PlainTextConstruct;

/**
 * @author Simon Willnauer
 *
 */
public class TestindexDocumentBuilderTask extends TestCase {
    IndexDocumentBuilder fineBuilder;
    IndexDocumentBuilder failInStrategyBuilder;
    IndexDocumentBuilder builder;
    IndexDocumentBuilderTask zeroFields;
    static String ID = "someId";
    static String CONTENT_FIELD = "someId";
    static String CONTENT = "foo bar";
    protected void setUp() throws Exception {
        ServerBaseEntry entry = new ServerBaseEntry();
        entry.setVersionId("1");
        entry.setFeedId("myFeed");
        entry.setId(ID);
        entry.setContent(new PlainTextConstruct(CONTENT));
        entry.setServiceConfig(new ProvidedServiceStub());
        IndexSchema schema = new IndexSchema();
        schema.setName("mySchema");
        IndexSchemaField field = new IndexSchemaField();
        field.setName(CONTENT_FIELD);
        field.setPath("/entry/content");
        field.setContentType(ContentType.TEXT);
        schema.addSchemaField(field);
        this.fineBuilder = new IndexDocumentBuilderTask(entry,schema,IndexAction.INSERT,true,true);
        
        /*
         * two fields, one will fail due to broken xpath.
         * One will remain.
         */
        schema = new IndexSchema();
        schema.setName("mySchema");
        field = new IndexSchemaField();
        field.setName("someContent");
        //broken xpath
        field.setPath("/entry///wrongXPath");
        field.setContentType(ContentType.TEXT);
        schema.addSchemaField(field);
        field = new IndexSchemaField();
        field.setName(CONTENT_FIELD);
        field.setPath("/entry/content");
        field.setContentType(ContentType.TEXT);
        schema.addSchemaField(field);
        this.failInStrategyBuilder = new IndexDocumentBuilderTask(entry,schema,IndexAction.INSERT,false,false);
        //fail with no fields
        schema = new IndexSchema();
        schema.setName("mySchema");
        this.zeroFields = new IndexDocumentBuilderTask(entry,schema,IndexAction.INSERT,false,false);
        
        
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.IndexDocumentBuilderTask.IndexDocumentBuilderTask(ServerBaseEntry, IndexSchema, IndexAction, boolean)'
     */
    public void testIndexDocumentBuilderTask() {
        IndexDocument doc = this.fineBuilder.call();
        assertNotNull(doc.getDeletealbe());
        assertNotNull(doc.getWriteable());
        assertEquals(IndexDocument.FIELD_ENTRY_ID,doc.getDeletealbe().field());
        assertEquals(ID,doc.getDeletealbe().text());
        assertEquals(ID,doc.getWriteable().getField(IndexDocument.FIELD_ENTRY_ID).stringValue());
        assertNotNull(doc.getWriteable().getField(CONTENT_FIELD).stringValue());
      
        /*
         * the broken xpath fails but the other fields will be indexed
         */
        doc = this.failInStrategyBuilder.call();
        assertNotNull(doc.getDeletealbe());
        assertNotNull(doc.getWriteable());
        assertEquals(IndexDocument.FIELD_ENTRY_ID,doc.getDeletealbe().field());
        assertEquals(ID,doc.getDeletealbe().text());
        assertEquals(ID,doc.getWriteable().getField(IndexDocument.FIELD_ENTRY_ID).stringValue());
        assertNotNull(doc.getWriteable().getField(CONTENT_FIELD).stringValue());
        
        try{
        this.zeroFields.call();
        fail("zero fields in document");
        }catch (GdataIndexerException e) {}
    }

    /*
     * Test method for 'org.apache.lucene.gdata.search.index.IndexDocumentBuilderTask.call()'
     */
    public void testCall() throws InterruptedException, ExecutionException {
        ExecutorService service = Executors.newSingleThreadExecutor();
        Future<IndexDocument> future = service.submit(this.fineBuilder);
        IndexDocument doc = future.get();
        assertNotNull(doc.getDeletealbe());
        assertNotNull(doc.getWriteable());
        assertEquals(IndexDocument.FIELD_ENTRY_ID,doc.getDeletealbe().field());
        assertEquals(ID,doc.getDeletealbe().text());
        assertEquals(ID,doc.getWriteable().getField(IndexDocument.FIELD_ENTRY_ID).stringValue());
        assertNotNull(doc.getWriteable().getField(CONTENT_FIELD).stringValue());
        assertTrue(doc.commitAfter());
        assertTrue(doc.optimizeAfter());
      
        /*
         * the broken xpath fails but the other fields will be indexed
         */
        future = service.submit(this.failInStrategyBuilder);
         doc = future.get();
        
        assertNotNull(doc.getDeletealbe());
        assertNotNull(doc.getWriteable());
        assertEquals(IndexDocument.FIELD_ENTRY_ID,doc.getDeletealbe().field());
        assertEquals(ID,doc.getDeletealbe().text());
        assertEquals(ID,doc.getWriteable().getField(IndexDocument.FIELD_ENTRY_ID).stringValue());
        assertNotNull(doc.getWriteable().getField(CONTENT_FIELD).stringValue());
        future = service.submit(this.zeroFields);
        
        try{
         future.get();
        fail("zero fields in document");
        }catch (ExecutionException e) {
            assertTrue(e.getCause().getClass() == GdataIndexerException.class);
            
        }
        service.shutdownNow();
    }
    
    
    

}
